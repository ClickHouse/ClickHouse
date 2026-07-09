---
description: '控制服务器 CPU 过载时的行为。'
sidebar_label: '服务器过载'
slug: /operations/settings/server-overload
title: '服务器过载'
doc_type: 'reference'
---

<div id="overview">
  ## 概述
</div>

有时，服务器可能会因各种原因而过载。为了确定当前的 CPU 过载情况，
ClickHouse 服务器会计算 CPU 等待时间 (`OSCPUWaitMicroseconds` 指标) 与忙碌时间
 (`OSCPUVirtualTimeMicroseconds` 指标) 的比率。当服务器的过载比率超过某个阈值时，
丢弃部分查询，甚至丢弃连接请求，以避免负载进一步增加，是有意义的。

有一个服务器设置 `os_cpu_busy_time_threshold`，用于控制将 CPU
视为正在执行有效工作的最小忙碌时间。如果当前 `OSCPUVirtualTimeMicroseconds` 指标的值低于该值，
则认为 CPU 过载为 0。

<div id="rejecting-queries">
  ## 拒绝查询
</div>

是否拒绝查询由查询级设置 `min_os_cpu_wait_time_ratio_to_throw` 和
`max_os_cpu_wait_time_ratio_to_throw` 控制。如果设置了这两个参数，且 `min_os_cpu_wait_time_ratio_to_throw` 小于
`max_os_cpu_wait_time_ratio_to_throw`，那么当过载比率至少达到 `min_os_cpu_wait_time_ratio_to_throw` 时，查询会以一定概率被拒绝，并抛出
`SERVER_OVERLOADED` 错误。该概率
通过最小和最大比率之间的线性插值来确定。例如，如果 `min_os_cpu_wait_time_ratio_to_throw = 2`，
`max_os_cpu_wait_time_ratio_to_throw = 6`，且 `cpu_overload = 4`，那么该查询会以 `0.5` 的概率被拒绝。

<div id="dropping-connections">
  ## 拒绝连接
</div>

是否拒绝连接由服务器级设置 `min_os_cpu_wait_time_ratio_to_drop_connection` 和
`max_os_cpu_wait_time_ratio_to_drop_connection` 控制。这些设置无需重启服务器即可修改。其背后的思路
与拒绝查询类似。唯一的区别是，在这种情况下，如果服务器过载，
连接尝试会在服务器端被拒绝。

<div id="resource-overload-warnings">
  ## 资源过载警告
</div>

当服务器过载时，ClickHouse 还会将 CPU 和内存过载警告记录到 `system.warnings` 表。你可以
通过服务器配置自定义这些阈值。

**示例**

```xml

<resource_overload_warnings>
    <cpu_overload_warn_ratio>0.9</cpu_overload_warn_ratio>
    <cpu_overload_clear_ratio>0.8</cpu_overload_clear_ratio>
    <cpu_overload_duration_seconds>600</cpu_overload_duration_seconds>
    <memory_overload_warn_ratio>0.9</memory_overload_warn_ratio>
    <memory_overload_clear_ratio>0.8</memory_overload_clear_ratio>
    <memory_overload_duration_seconds>600</memory_overload_duration_seconds>
</resource_overload_warnings>
```