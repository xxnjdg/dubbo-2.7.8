/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LeastActiveLoadBalance
 * <p>
 * Filter the number of invokers with the least number of active calls and count the weights and quantities of these invokers.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 *
 * 基于最少活跃调用数
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers
        int length = invokers.size();// 总个数
        // The least active value of all invokers
        int leastActive = -1;// 最小的活跃数
        // The number of invokers having the same least active value (leastActive)
        int leastCount = 0;// 相同最小活跃数的个数
        // The index of invokers having the same least active value (leastActive)
        int[] leastIndexes = new int[length];// 相同最小活跃数的下标
        // the weight of every invokers
        int[] weights = new int[length];
        // The sum of the warmup weights of all the least active invokers
        int totalWeight = 0;// 总权重
        // The weight of the first least active invoker
        int firstWeight = 0;// 第一个权重，用于于计算是否相同
        // Every least active invoker has the same weight value?
        boolean sameWeight = true; // 是否所有权重相同


        // Filter out all the least active invokers
        // 计算获得相同最小活跃数的数组和个数
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            // Get the active number of the invoker
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();
            // Get the weight of the invoker's configuration. The default value is 100.
            int afterWarmup = getWeight(invoker, invocation);
            // save for later use
            weights[i] = afterWarmup;
            // If it is the first invoker or the active number of the invoker is less than the current least active number
            if (leastActive == -1 || active < leastActive) {// 发现更小的活跃数，重新开始
                // Reset the active number of the current invoker to the least active number
                leastActive = active;// 记录最小活跃数
                // Reset the number of least active invokers
                leastCount = 1;// 重新统计相同最小活跃数的个数
                // Put the first least active invoker first in leastIndexes
                leastIndexes[0] = i;// 重新记录最小活跃数下标
                // Reset totalWeight
                totalWeight = afterWarmup;// 重新累计总权重
                // Record the weight the first least active invoker
                firstWeight = afterWarmup;// 记录第一个权重
                // Each invoke has the same weight (only one invoker here)
                sameWeight = true;// 还原权重相同标识
                // If current invoker's active value equals with leaseActive, then accumulating.
            } else if (active == leastActive) {// 累计相同最小的活跃数
                // Record the index of the least active invoker in leastIndexes order
                leastIndexes[leastCount++] = i;// 累计相同最小活跃数下标
                // Accumulate the total weight of the least active invoker
                totalWeight += afterWarmup;// 累计总权重
                // If every invoker has the same weight?
                if (sameWeight && afterWarmup != firstWeight) {
                    sameWeight = false;// 判断所有权重是否一样
                }
            }
        }
        // Choose an invoker from all the least active invokers
        if (leastCount == 1) {
            // If we got exactly one invoker having the least active value, return this invoker directly.
            return invokers.get(leastIndexes[0]);// 如果只有一个最小则直接返回
        }
        if (!sameWeight && totalWeight > 0) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on 
            // totalWeight.
            // 如果权重不相同且权重大于0则按总权重数随机
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            // Return a invoker based on the random value.
            // 并确定随机值落在哪个片断上
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        // 如果权重相同或权重为0则均等随机
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
