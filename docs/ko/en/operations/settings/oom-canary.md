---
description: 'ClickHouse 서버보다 먼저 Linux OOM killer의 표적이 되도록 설계된 희생용 자식 프로세스로, 서버가 부하를 줄이고 계속 실행될 수 있는 시간을 벌어줍니다.'
sidebar_label: 'OOM 카나리'
sidebar_position: 60
slug: /operations/settings/oom-canary
title: 'OOM 카나리'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge />

:::note
OOM 카나리는 Experimental 기능이며 기본적으로 비활성화되어 있습니다. 프로덕션 환경 검증이 완료될 때까지 ClickHouse 버전별로 동작이 변경될 수 있습니다.
:::

<div id="overview">
  ## 개요
</div>

호스트 또는 메모리 cgroup의 메모리가 고갈되면 Linux OOM(out-of-memory)
킬러가 `SIGKILL`로 프로세스를 종료합니다. 보통은 메모리를 가장 많이 사용하는
프로세스가 대상이 되는데, 전용 호스트에서는 대개 `clickhouse-server` 자체입니다.
그 결과 서버는 복구할 기회도 없이 전체가 중단됩니다.

OOM 카나리는 누가 먼저 종료될지를 바꿉니다. 작은 *희생용* 하위 프로세스를
실행해 스스로를 OOM의 가장 유력한 대상으로 만들므로, 커널은 서버 대신 이
프로세스를 종료합니다. 그러면 서버는 해당 종료를 감지하고, 그것이 OOM
이벤트였음을 확인한 뒤 메모리 압박을 줄여 살아남을 수 있습니다.

카나리는 메모리 한도를 높여 주지 않으며, 올바른 한도 설정을 대체하는 것도
아닙니다([메모리 오버커밋](/ko/operations/settings/memory-overcommit) 및
`max_server_memory_usage` 참조). 이는 소량의 고정 메모리를 대가로,
메모리 사용량이 급증하는 상황에서 살아남을 가능성을 확보하기 위한 최후의
방어선입니다.

<div id="how-it-works">
  ## 작동 방식
</div>

카나리는 별도의 `clickhouse oom-canary` 프로세스입니다. 자체
`oom_score_adj`를 최대값(`1000`)으로 설정해 커널이 이를 가장 먼저 대상으로 삼게 한 다음,
`oom_canary_size` 바이트(기본값 100 MB)를 할당하고, 실제로 접근한 뒤 `mlock`하여
상주 집합이 실제 메모리를 사용하도록 합니다. 서버가 종료되면 자동으로 함께 종료됩니다.

서버에서는 모니터 스레드가 `pidfd`를 통해 카나리를 감시하다가,
카나리가 종료되면 다음과 같이 대응합니다:

* cgroup OOM 근거가 **있는 상태에서** `SIGKILL`로 종료됨 → OOM 대응을 실행한 다음
  새 카나리를 다시 시작합니다.
* OOM 근거 **없이** 종료됨(예: 수동 `kill -9`) 또는 일시적인 실패와 함께 종료됨 →
  대응은 하지 않고 다시 시작만 합니다.
* 영구적인 설정 실패 또는 서버 종료 → 카나리가 자체적으로 비활성화됩니다.

OOM 근거는 cgroup v2 `memory.events.local`의 `oom_kill`
counter에서만 가져옵니다. 이는 의도적으로 cgroup 로컬 기준만 사용합니다. 계층형 카운터나 호스트 전체 카운터는
관련 없는 프로세스로 인해 증가할 수 있으므로, 잘못된 대응이 트리거될 수 있습니다.

OOM이 확인되면 대응은 다음의 독립적인 단계를 수행합니다: `FATAL`
메시지를 기록하고, allocator(jemalloc) arena를 purge하고, 실행 중인 모든
쿼리를 가능한 범위에서 취소하고, 모든 머지와 뮤테이션을 취소하며, 이벤트를
[`system.crash_log`](/ko/operations/system-tables/crash_log)의 큐에 넣습니다. 시스템 로그는
동기적으로 플러시되지 않습니다. 메모리 압박 상태에서 I/O를 강제로 수행하면 상황이 더 악화될 수 있기 때문입니다.

<div id="requirements">
  ## 요구 사항
</div>

* **Linux ≥ 5.3.** 모니터는 `pidfd_open`을 통해 카나리를 관리합니다. 더 오래된 커널에서는
  카나리가 시작 시 자동으로 비활성화됩니다. Linux 이외의 플랫폼에서는 아무 동작도 하지 않습니다.
* **OOM 대응을 위해 `memory.events.local`이 포함된 cgroup v2.** 이것이 없으면
  카나리는 `SIGKILL` 이후에도 다시 시작되지만 OOM을 확인할 수 없으므로
  대응 로직은 실행되지 않습니다(시작 시 경고가 기록됩니다).
* **`mlock` capability(선택 사항).** 카나리의 메모리를 잠그려면
  `CAP_IPC_LOCK` 또는 충분한 `RLIMIT_MEMLOCK`이 필요합니다. 실패하면 카나리가
  경고를 기록하고 메모리가 스왑될 수 있어 OOM 대상로서의 효과가 약해질 수 있습니다.

:::warning memory.oom.group
서버의 cgroup에서 cgroup v2 `memory.oom.group`이 활성화되어 있으면 커널은
OOM 발생 시 전체 cgroup을 하나의 단위로 종료합니다. 즉, 서버가
카나리와 함께 종료되며 대응 로직도 실행되지 않습니다. 이 모드에서는 카나리가 서버를 보호할 수
없고, 시작 시 경고가 기록됩니다.
:::

<div id="configuration">
  ## 구성
</div>

카나리는 [서버 설정](/ko/operations/server-configuration-parameters/settings)으로 제어되며,
서버 구성의 최상위 요소로 설정되고 서버를 재시작하면 적용됩니다.

| Setting                              | Default              | Description                                                                                                                   |
| ------------------------------------ | -------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `oom_canary_enable`                  | `false`              | OOM 카나리를 활성화합니다.                                                                                                              |
| `oom_canary_size`                    | `104857600` (100 MB) | 카나리가 할당하고 실제로 접근하는 바이트 수입니다. 값이 클수록 OOM의 우선 대상이 되기 쉬워집니다.                                                                     |
| `oom_canary_relaunch`                | `true`               | 카나리가 종료된 후 다시 시작합니다(영구적인 Setup 실패 또는 정상 종료는 제외). 단, 아래 제한이 적용됩니다.                                                             |
| `oom_canary_max_rapid_relaunches`    | `10`                 | 과도한 반복을 방지하기 위해 자동 재시작을 비활성화하기 전까지 허용되는 연속 *빠른* 재시작의 최대 횟수입니다. 카나리가 `oom_canary_max_backoff_seconds`보다 오래 살아남으면 이 횟수가 재설정됩니다. |
| `oom_canary_initial_backoff_seconds` | `1`                  | 재시작 사이의 초기 지연 시간이며, 최대값에 도달할 때까지 매번 2배로 늘어납니다.                                                                                |
| `oom_canary_max_backoff_seconds`     | `60`                 | 재시작 사이의 최대 지연 시간입니다.                                                                                                          |

```xml
<clickhouse>
    <oom_canary_enable>1</oom_canary_enable>
    <oom_canary_size>104857600</oom_canary_size>
</clickhouse>
```

<div id="observability">
  ## 관측성
</div>

확인된 OOM이 발생하면
[`system.crash_log`](/ko/operations/system-tables/crash_log)에 `signal = 9`이고
`signal_description`에 `OOM Canary`가 언급된 행이 생성됩니다:

```sql
SELECT event_time, signal, signal_description
FROM system.crash_log
WHERE signal = 9 AND signal_description LIKE '%OOM Canary%'
ORDER BY event_time DESC;
```

카나리의 생명 주기와 각 OOM 대응 단계도 서버 로그에 기록됩니다.