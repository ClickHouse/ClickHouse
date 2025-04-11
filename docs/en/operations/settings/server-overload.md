---
description: 'Controlling behavior on server CPU overload.'
slug: /operations/settings/server-overload
title: 'Server overload'
---

# Server overload

## Overview {#overview}

Sometimes server can become overloaded due to different reasons. In order to determine the current CPU overload,
ClickHouse server calculates the ratio of CPU wait time (`OSCPUWaitMicroseconds` metric) to busy time
(`OSCPUVirtualTimeMicroseconds` metric). When the server is overloaded above certain ratio,
it makes sense to discard some queries or even drop connection requests to not increase the load even more.

There's a server setting `os_cpu_busy_time_threshold` which controls the minimum busy time to consider CPU
doing some useful work. If the current value of `OSCPUVirtualTimeMicroseconds` metric is below this value,
CPU overload is assumed to be 0.

## Rejecting queries

The behavior of rejecting queries is controlled by query-level settings `min_os_cpu_wait_time_ratio_to_throw` and
`max_os_cpu_wait_time_ratio_to_throw`. If those settings are set and `min_os_cpu_wait_time_ratio_to_throw` is less
than `max_os_cpu_wait_time_ratio_to_throw`, then the query is rejected and `SERVER_OVERLOADED` error is thrown
with some probability is the overload ratio is at least `min_os_cpu_wait_time_ratio_to_throw`. The probability
is determined as a linear interpolation between min and max ratios. For example, if `min_os_cpu_wait_time_ratio_to_throw = 2`,
`max_os_cpu_wait_time_ratio_to_throw = 6`, and `cpu_overload = 4`, then the query will be rejected with a probability of `0.5`.

## Dropping connections

Dropping connections is controlled by server-level settings `min_os_cpu_wait_time_ratio_to_drop_connection` and
`max_os_cpu_wait_time_ratio_to_drop_connection`. Those settings can be changed without server restart. The idea behind
those settings is similar to the one with rejecting queries. The only difference in this case is if the server is overloaded,
the connection attempt will be rejected from the server side.
