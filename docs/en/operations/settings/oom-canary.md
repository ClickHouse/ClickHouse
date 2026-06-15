---
description: 'A sacrificial child process that attracts the Linux OOM killer before
  the ClickHouse server, giving the server a chance to shed load and survive.'
sidebar_label: 'OOM canary'
sidebar_position: 60
slug: /operations/settings/oom-canary
title: 'OOM canary'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge/>

:::note
The OOM canary is experimental. Its behavior may change between ClickHouse
versions until production validation is complete. It is disabled by default.
:::

## Overview {#overview}

When a Linux machine or memory cgroup runs out of memory, the kernel OOM
(out-of-memory) killer chooses a process and terminates it with `SIGKILL`. On a
dedicated host the largest memory consumer is usually `clickhouse-server`
itself, so the server is killed outright — losing all in-flight queries, caches,
and connections — instead of being given a chance to recover.

The OOM canary changes who the kernel kills first. It launches a small
*sacrificial* child process that deliberately makes itself the most attractive
OOM target. When memory runs out, the kernel kills the canary instead of the
server. The server detects the canary's death, confirms it was an OOM event, and
runs a response sequence that sheds memory pressure (cancels queries and merges,
purges allocator arenas) so the server can survive.

The canary does not increase the total memory limit and is not a replacement for
correct memory limits (see [Memory overcommit](/operations/settings/memory-overcommit)
and `max_server_memory_usage`). It is a last line of defense that trades a small,
fixed amount of memory for a chance to keep the server alive through a
memory spike.

## How it works {#how-it-works}

The canary is a separate `clickhouse oom-canary` sub-process spawned via
`posix_spawn`. Because it is a fresh process image, it inherits nothing from the
server's address space — no copy-on-write residue under memory pressure, no
allocator state, and no signal handlers. The child:

1. Sets its own `oom_score_adj` to `1000` (the maximum), so the kernel OOM
   killer targets it before any other process in the cgroup.
2. Allocates, touches, and `mlock`-s a configurable amount of memory
   (`oom_canary_size`, 100 MB by default) so its resident set is real and
   predictable for the kernel's heuristic.
3. Asks the kernel to send it `SIGKILL` if the server (its parent) ever exits,
   so the canary can never outlive the server.

In the server process, a dedicated monitor thread owns the canary through a
`pidfd` and waits — using `epoll` — for either the canary's death or a shutdown
request. When the canary dies, the monitor classifies the death:

- Killed by `SIGKILL` **with** cgroup OOM-kill evidence → run the OOM response,
  then relaunch a fresh canary.
- Killed by `SIGKILL` **without** OOM evidence (for example, a manual
  `kill -9`) → no OOM response; relaunch only.
- Exited with a transient resource failure → relaunch only.
- Exited with a permanent setup failure, or the server is shutting down → the
  canary disables itself.

OOM evidence comes exclusively from the cgroup v2 `memory.events.local` file's
`oom_kill` counter. This counter is deliberately *cgroup-local*: hierarchical or
host-wide counters can be advanced by unrelated processes, which would make a
manual canary kill look like a real OOM and trigger a spurious response.

## The OOM response {#the-oom-response}

When the monitor confirms an OOM kill, it runs the following steps in order.
Each step is independent: a failure in one does not skip the rest.

1. Log a `FATAL` message reporting tracked memory and RSS.
2. Purge allocator (jemalloc) arenas to return free memory to the OS.
3. Best-effort cancel of all running queries.
4. Cancel all running merges and mutations.
5. Queue an event in [`system.crash_log`](/operations/system-tables/crash_log)
   so the OOM is visible after the fact.

System log flushing is intentionally *not* forced synchronously here, because
forcing I/O while under severe memory pressure can make the situation worse.

## Requirements {#requirements}

- **Linux only.** The canary is a no-op on other platforms; enabling it
  elsewhere logs a warning and is ignored.
- **Linux ≥ 5.3.** The monitor owns the canary via `pidfd_open` and waits on the
  `pidfd`; there is no `/proc`-polling fallback. On older kernels `pidfd_open`
  fails and the canary disables itself at startup.
- **cgroup v2 with `memory.events.local`** for the OOM response. Without this
  evidence source, the canary still relaunches after a `SIGKILL`, but it cannot
  confirm an OOM and so will not run the response sequence. A warning is logged
  at startup in this case.
- **`mlock` capability (optional).** Locking the canary's memory requires
  `CAP_IPC_LOCK` or a sufficient `RLIMIT_MEMLOCK`. If locking fails the canary
  logs a warning; the memory stays allocated and touched but may be swapped out,
  which weakens the canary as an OOM target.

:::warning memory.oom.group
If cgroup v2 `memory.oom.group` is enabled for the server's cgroup, the kernel
kills the *entire cgroup as a single unit* on a memcg OOM — the server dies
together with the canary, so the OOM response can never run. In this mode the
canary cannot protect the server. A warning is logged at startup, but because
the flag can change at runtime this is a diagnostic, not a guarantee.
:::

## Configuration {#configuration}

The OOM canary is controlled by [server settings](/operations/server-configuration-parameters/settings).
Set them as top-level elements of the server configuration; they take effect on
server restart.

| Setting | Default | Description |
|---------|---------|-------------|
| `oom_canary_enable` | `false` | Enable the OOM canary. |
| `oom_canary_size` | `104857600` (100 MB) | Size in bytes of the memory region the canary allocates and touches. Larger values make the canary a more attractive OOM target. |
| `oom_canary_relaunch` | `true` | Relaunch the canary after it dies for any reason other than a permanent setup failure or server shutdown, subject to the limits below. |
| `oom_canary_max_rapid_relaunches` | `10` | Maximum number of consecutive *rapid* relaunches before automatic relaunch is disabled, to avoid thrashing under sustained memory pressure. Resets once a canary survives longer than `oom_canary_max_backoff_seconds`. Applies only when `oom_canary_relaunch` is `true`. |
| `oom_canary_initial_backoff_seconds` | `1` | Initial delay between relaunches. The delay doubles on each relaunch up to the maximum. |
| `oom_canary_max_backoff_seconds` | `60` | Maximum delay between relaunches. |

### Example {#example}

```xml
<clickhouse>
    <oom_canary_enable>1</oom_canary_enable>
    <oom_canary_size>104857600</oom_canary_size>
    <oom_canary_relaunch>1</oom_canary_relaunch>
    <oom_canary_max_rapid_relaunches>10</oom_canary_max_rapid_relaunches>
    <oom_canary_initial_backoff_seconds>1</oom_canary_initial_backoff_seconds>
    <oom_canary_max_backoff_seconds>60</oom_canary_max_backoff_seconds>
</clickhouse>
```

## Observability {#observability}

The canary logs its lifecycle to the server log: when the child is spawned, when
the monitor thread starts and exits, when relaunch limits are reached, and the
`FATAL` message and each step of the OOM response.

A confirmed OOM also produces a row in [`system.crash_log`](/operations/system-tables/crash_log)
with `signal = 9` and a `signal_description` that mentions `OOM Canary`:

```sql
SELECT event_time, signal, signal_description
FROM system.crash_log
WHERE signal = 9 AND signal_description LIKE '%OOM Canary%'
ORDER BY event_time DESC;
```
