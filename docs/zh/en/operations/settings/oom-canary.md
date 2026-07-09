---
description: '一个充当“替死鬼”的子进程，会在 ClickHouse server 之前被 Linux OOM killer 选中，从而让服务器有机会降低负载并继续存活。'
sidebar_label: 'OOM canary'
sidebar_position: 60
slug: /operations/settings/oom-canary
title: 'OOM canary'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge />

:::note
OOM canary 目前处于 Experimental 阶段，且默认处于禁用状态。在完成生产环境验证之前，
其行为在不同 ClickHouse 版本之间可能会有所变化。
:::

<div id="overview">
  ## 概览
</div>

当主机或 memory cgroup 耗尽内存时，Linux OOM (内存不足) killer
会用 `SIGKILL` 终止某个进程——通常是占用内存最多的那个，而在专用主机上，
往往就是 `clickhouse-server` 本身。结果是整个服务器直接退出，
而没有机会恢复。

OOM canary 改变了谁会先被杀死。它会运行一个小型的 *牺牲型* 子
进程，并让自己成为最容易被 OOM 选中的目标，这样内核杀死的就是它，
而不是服务器。随后，服务器会检测到该进程已终止，确认这是一次 OOM
事件，并缓解内存压力，从而存活下来。

canary 不会提高任何内存限制，也不能替代正确的限制配置
(参见 [内存 overcommit](/zh/operations/settings/memory-overcommit) 和
`max_server_memory_usage`) 。它是最后一道防线，以少量固定的
内存为代价，换取在内存突增时存活下来的机会。

<div id="how-it-works">
  ## 工作原理
</div>

canary 是一个独立的 `clickhouse oom-canary` 进程。它会将自己的
`oom_score_adj` 设为最大值 (`1000`) ，以便让内核优先选中它，然后
分配、触碰并对 `oom_canary_size` 字节执行 `mlock` (默认 100 MB) ，从而确保
它的常驻内存集是真实占用的。如果 server 退出，它也会被自动终止。

在 server 中，一个监控线程会通过 `pidfd` 监视 canary，并在
它死亡时作出响应：

* 因 `SIGKILL` 被杀死，**且** 有 cgroup OOM 证据 → 运行 OOM 响应，然后
  重新启动一个新的 canary。
* 被杀死但**没有** OOM 证据 (例如手动执行 `kill -9`) ，或者因瞬时故障而退出
  → 仅重新启动，不执行响应。
* 永久性 setup 失败，或 server 关闭 → canary 将自行禁用。

OOM 证据仅来自 cgroup v2 `memory.events.local` 中的 `oom_kill`
计数器。这是有意限制为 cgroup 本地的：分层计数器或全主机范围的计数器可能
会被无关进程推进，从而触发误响应。

在确认发生 OOM 后，响应会执行以下彼此独立的步骤：记录一条 `FATAL`
消息，清理分配器 (jemalloc) arenas，尽力取消所有正在运行的
查询，取消所有合并和变更，并在
[`system.crash_log`](/zh/operations/system-tables/crash_log) 中将一个事件加入队列。系统日志不会同步
flush，因为在内存压力下强制执行 I/O 可能会让情况更糟。

<div id="requirements">
  ## 要求
</div>

* **Linux ≥ 5.3。** monitor 通过 `pidfd_open` 持有 canary；在较旧的内核上，
  canary 会在启动时自行禁用。在非 Linux 平台上，它不起任何作用。
* **用于 OOM 响应的、带有 `memory.events.local` 的 cgroup v2。** 如果没有它，
  canary 在收到 `SIGKILL` 后仍会重新启动，但无法确认是否发生了 OOM，因此
  响应永远不会执行 (启动时会记录一条警告日志) 。
* **`mlock` 能力 (可选) 。** 锁定 canary 的内存需要
  `CAP_IPC_LOCK` 或足够的 `RLIMIT_MEMLOCK`；如果失败，canary 会记录一条
  警告日志，其内存也可能被换出，从而削弱其作为 OOM 目标的作用。

:::warning memory.oom.group
如果为 server 的 cgroup 启用了 cgroup v2 `memory.oom.group`，内核会在 OOM 时
将整个 cgroup 作为一个整体杀死——server 会与
canary 一同终止，因此响应永远不会执行。canary 在这种
模式下无法保护 server；启动时会记录一条警告日志。
:::

<div id="configuration">
  ## 配置
</div>

canary 由[服务器设置](/zh/operations/server-configuration-parameters/settings)控制，
这些设置作为服务器配置的顶层元素进行配置，并在重启后生效。

| 设置                                   | 默认值                  | 描述                                                                                              |
| ------------------------------------ | -------------------- | ----------------------------------------------------------------------------------------------- |
| `oom_canary_enable`                  | `false`              | 启用 OOM canary。                                                                                  |
| `oom_canary_size`                    | `104857600` (100 MB) | canary 分配并访问的字节数。值越大，它就越容易成为 OOM 的目标。                                                           |
| `oom_canary_relaunch`                | `true`               | canary 终止后重新启动它 (除非是永久性初始化失败或正常关闭) ，并受以下限制约束。                                                   |
| `oom_canary_max_rapid_relaunches`    | `10`                 | 为避免反复抖动，在禁用自动重新启动之前，允许连续*快速*重新启动的最大次数。canary 一旦存活时间超过 `oom_canary_max_backoff_seconds`，该计数就会重置。 |
| `oom_canary_initial_backoff_seconds` | `1`                  | 两次重新启动之间的初始延迟；每次翻倍，直到达到最大值。                                                                     |
| `oom_canary_max_backoff_seconds`     | `60`                 | 两次重新启动之间的最大延迟。                                                                                  |

```xml
<clickhouse>
    <oom_canary_enable>1</oom_canary_enable>
    <oom_canary_size>104857600</oom_canary_size>
</clickhouse>
```

<div id="observability">
  ## 可观测性
</div>

确认发生 OOM 后，会在
[`system.crash_log`](/zh/operations/system-tables/crash_log) 中生成一行记录，其中 `signal = 9`，且
`signal_description` 中会提及 `OOM Canary`：

```sql
SELECT event_time, signal, signal_description
FROM system.crash_log
WHERE signal = 9 AND signal_description LIKE '%OOM Canary%'
ORDER BY event_time DESC;
```

canary 的生命周期以及 OOM 响应的各个步骤也会被记录到服务器日志中。