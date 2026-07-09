---
description: 'ClickHouse의 할당 프로파일링을 설명하는 페이지'
sidebar_label: '할당 프로파일링'
slug: /operations/allocation-profiling
title: '할당 프로파일링'
doc_type: 'guide'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling">
  # 할당 프로파일링
</div>

ClickHouse는 전역 할당자로 [jemalloc](https://github.com/jemalloc/jemalloc)을 사용합니다. jemalloc에는 할당 샘플링 및 프로파일링을 위한 도구가 포함되어 있습니다.

ClickHouse와 Keeper에서는 구성, 쿼리 설정, `SYSTEM` 명령, 그리고 Keeper의 4문자 단어(4LW) 명령을 사용해 샘플링을 제어할 수 있습니다. 결과를 확인하는 방법은 여러 가지가 있습니다.

* 쿼리별 분석을 위해 샘플을 `system.trace_log`에 `JemallocSample` 유형으로 수집합니다.
* 내장된 [jemalloc 웹 UI](#jemalloc-web-ui)에서 실시간 메모리 통계를 확인하고 힙 프로파일을 가져옵니다(26.2+).
* [`system.jemalloc_profile_text`](#fetching-heap-profiles-from-sql)를 사용해 SQL에서 현재 힙 프로파일을 직접 쿼리합니다(26.2+).
* 힙 프로파일을 디스크에 플러시하고 [`jeprof`](#analyzing-heap-profile-files-with-jeprof)로 분석합니다.

:::note

이 가이드는 25.9+ 버전에 적용됩니다.
이전 버전은 [25.9 이전 버전용 할당 프로파일링](/ko/operations/allocation-profiling-old.md)을 확인하십시오.

:::

<div id="sampling-allocations">
  ## 메모리 할당 샘플링
</div>

메모리 할당을 샘플링하고 프로파일링하려면 `jemalloc_enable_global_profiler` 구성을 활성화한 상태로 ClickHouse/Keeper를 시작하십시오:

```xml
<clickhouse>
    <jemalloc_enable_global_profiler>1</jemalloc_enable_global_profiler>
</clickhouse>
```

`jemalloc`은 메모리 할당을 샘플링하고 해당 정보를 내부적으로 저장합니다.

`jemalloc_enable_profiler` 설정을 사용하면 쿼리별로 샘플링을 활성화할 수도 있습니다.

:::warning 경고
ClickHouse는 메모리 할당이 빈번한 애플리케이션이므로 jemalloc 샘플링으로 인해 성능 저하가 발생할 수 있습니다.
:::

<div id="storing-jemalloc-samples-in-system-trace-log">
  ## `system.trace_log`에 jemalloc 샘플 저장
</div>

jemalloc 샘플은 `JemallocSample` 유형으로 `system.trace_log`에 저장할 수 있습니다.
전역으로 활성화하려면 `jemalloc_collect_global_profile_samples_in_trace_log` 구성(config)을 사용하십시오:

```xml
<clickhouse>
    <jemalloc_collect_global_profile_samples_in_trace_log>1</jemalloc_collect_global_profile_samples_in_trace_log>
</clickhouse>
```

:::warning 경고
ClickHouse는 메모리 할당이 많은 애플리케이션이므로 system.trace&#95;log에 모든 샘플을 수집하면 부하가 크게 증가할 수 있습니다.
:::

`jemalloc_collect_profile_samples_in_trace_log` 설정을 사용하면 쿼리별로도 활성화할 수 있습니다.

<div id="example-analyzing-memory-usage-trace-log">
  ### 예시: 쿼리의 메모리 사용량 분석
</div>

먼저, jemalloc 프로파일러를 활성화한 상태에서 쿼리를 실행하고 샘플을 `system.trace_log`에 수집합니다:

```sql
SELECT *
FROM numbers(1000000)
ORDER BY number DESC
SETTINGS max_bytes_ratio_before_external_sort = 0
FORMAT `Null`
SETTINGS jemalloc_enable_profiler = 1, jemalloc_collect_profile_samples_in_trace_log = 1

Query id: 8678d8fe-62c5-48b8-b0cd-26851c62dd75

Ok.

0 rows in set. Elapsed: 0.009 sec. Processed 1.00 million rows, 8.00 MB (108.58 million rows/s., 868.61 MB/s.)
Peak memory usage: 12.65 MiB.
```

:::note
ClickHouse를 `jemalloc_enable_global_profiler`와 함께 시작한 경우 `jemalloc_enable_profiler`를 활성화할 필요가 없습니다.
`jemalloc_collect_global_profile_samples_in_trace_log`와 `jemalloc_collect_profile_samples_in_trace_log`도 마찬가지입니다.
:::

`system.trace_log`를 플러시하세요:

```sql
SYSTEM FLUSH LOGS trace_log
```

그런 다음 쿼리를 실행해 시간 경과에 따른 누적 메모리 사용량을 확인합니다:

```sql
WITH per_bucket AS
(
    SELECT
        event_time_microseconds AS bucket_time,
        sum(size) AS bucket_sum
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
    GROUP BY bucket_time
)
SELECT
    bucket_time,
    sum(bucket_sum) OVER (
        ORDER BY bucket_time ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_size,
    formatReadableSize(cumulative_size) AS cumulative_size_readable
FROM per_bucket
ORDER BY bucket_time
```

메모리 사용량이 가장 높았던 시점을 찾으십시오:

```sql
SELECT
    argMax(bucket_time, cumulative_size),
    max(cumulative_size)
FROM
(
    WITH per_bucket AS
    (
        SELECT
            event_time_microseconds AS bucket_time,
            sum(size) AS bucket_sum
        FROM system.trace_log
        WHERE trace_type = 'JemallocSample'
          AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
        GROUP BY bucket_time
    )
    SELECT
        bucket_time,
        sum(bucket_sum) OVER (
            ORDER BY bucket_time ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_size,
        formatReadableSize(cumulative_size) AS cumulative_size_readable
    FROM per_bucket
    ORDER BY bucket_time
)
```

그 결과를 바탕으로, 정점 시점에 가장 활발했던 할당 스택을 확인하십시오.

```sql
SELECT
    concat(
        '\n',
        arrayStringConcat(
            arrayMap(
                (x, y) -> concat(x, ': ', y),
                arrayMap(x -> addressToLine(x), allocation_trace),
                arrayMap(x -> demangle(addressToSymbol(x)), allocation_trace)
            ),
            '\n'
        )
    ) AS symbolized_trace,
    sum(s) AS per_trace_sum
FROM
(
    SELECT
        ptr,
        sum(size) AS s,
        argMax(trace, event_time_microseconds) AS allocation_trace
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
      AND event_time_microseconds <= '2025-09-04 11:56:21.737139'
    GROUP BY ptr
    HAVING s > 0
)
GROUP BY ALL
ORDER BY per_trace_sum ASC
```

<div id="jemalloc-web-ui">
  ## Jemalloc 웹 UI
</div>

:::note
이 섹션은 버전 26.2+에 적용됩니다.
:::

ClickHouse는 `/jemalloc` HTTP 엔드포인트에서 jemalloc 메모리 통계를 확인할 수 있는 내장 웹 UI를 제공합니다.
이 UI는 allocated, active, resident, mapped 메모리를 포함한 실시간 메모리 메트릭을 차트와 함께 표시하며, arena별 및 bin별 통계도 제공합니다.
또한 UI에서 전역 및 쿼리별 힙 프로파일을 직접 가져올 수 있습니다.

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```text
    http://localhost:8123/jemalloc
    ```

    서버 UI에는 Summary, Allocations, Arenas, Operations, Global Profiler, Query Profiler, Raw Output 탭이 모두 포함되어 있습니다.
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```text
    http://localhost:9182/jemalloc
    ```

    Keeper UI는 HTTP 제어 포트에서 사용할 수 있습니다. 이 포트는 **기본적으로 비활성화**되어 있으므로, Keeper 구성에서 `keeper_server.http_control.port`를 설정해 명시적으로 활성화해야 합니다.

    ```xml
    <clickhouse>
        <keeper_server>
            <http_control>
                <port>9182</port>
            </http_control>
        </keeper_server>
    </clickhouse>
    ```

    활성화되면 이 UI는 SQL 및 `system.trace_log`가 필요한 Query Profiler 탭을 제외하고, Summary, Allocations, Arenas, Operations, Global Profiler, Raw Output 등 서버와 동일한 시각화를 제공합니다.

    :::warning 보안
    Keeper HTTP 제어 포트에는 애플리케이션 수준의 인증이 없습니다. 모든 데이터 쿼리가 SQL HTTP handler를 통해 전달되고 사용자 이름/패스워드 자격 증명이 필요한 ClickHouse 서버 jemalloc UI와 달리, Keeper REST API 엔드포인트에는 인증이 적용되지 않습니다. 이는 다른 Keeper HTTP 제어 엔드포인트(commands, storage, dashboard)와도 일관됩니다.

    네트워크 수준 제어를 사용해 이 포트에 대한 접근을 제한하십시오. Keeper를 localhost에 바인딩하거나, firewall 규칙을 사용하거나, 인증이 적용된 리버스 프록시 뒤에 배치하십시오. `listen_host`가 구성되지 않은 경우 Keeper는 기본적으로 localhost에서만 수신합니다.
    :::

    Keeper는 프로그래밍 방식으로 접근할 수 있도록 REST API 엔드포인트도 제공합니다.

    * `GET /jemalloc/stats` — 원시 `malloc_stats_print` 출력
    * `GET /jemalloc/status` — JSON 형식의 프로파일링 상태 (`prof_enabled`, `prof_active`, `thread_active_init`, `lg_sample`)
    * `GET /jemalloc/profile?format={collapsed|raw}` — 서버 측 심벌화를 사용해 힙 프로파일을 플러시하고, flame graph 렌더링에 적합한 collapsed stacks(기본값) 또는 원시 jemalloc dump를 반환합니다.
  </TabItem>
</Tabs>

<div id="fetching-heap-profiles-from-sql">
  ## SQL에서 힙 프로필 가져오기
</div>

:::note
이 섹션은 버전 26.2+에 적용됩니다.
:::

`system.jemalloc_profile_text` 시스템 테이블(system table)을 사용하면 외부 도구 없이, 그리고 먼저 디스크에 플러시하지 않아도 현재 jemalloc 힙 프로필을 SQL에서 직접 가져와 확인할 수 있습니다.

이 테이블에는 단일 컬럼이 있습니다:

| 컬럼     | 유형     | 설명                             |
| ------ | ------ | ------------------------------ |
| `line` | String | 심볼화된 jemalloc 힙 프로필의 텍스트 줄입니다. |

테이블에 직접 쿼리할 수 있으므로, 사전에 힙 프로필을 플러시할 필요가 없습니다:

```sql
SELECT * FROM system.jemalloc_profile_text
```

<div id="output-format">
  ### 출력 형식
</div>

출력 형식은 `jemalloc_profile_text_output_format` 설정으로 제어되며, 다음 3가지 값을 지원합니다:

* `raw` — jemalloc이 생성한 원시 힙 프로파일입니다.
* `symbolized` — 함수 심볼이 내장된 jeprof 호환 형식입니다. 심볼이 이미 내장되어 있으므로 `jeprof`는 ClickHouse 실행 파일 없이도 출력을 분석할 수 있습니다.
* `collapsed` (기본값) — 플레임 그래프와 호환되는 collapsed stacks 형식으로, 각 줄에 하나의 스택과 바이트 수가 포함됩니다.

예를 들어, 원시 프로파일을 가져오려면 다음과 같이 합니다:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'raw'
```

심볼 정보가 해석된 출력을 얻으려면:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'symbolized'
```

<div id="fetching-heap-profiles-settings">
  ### 추가 설정
</div>

* `jemalloc_profile_text_symbolize_with_inline` (Bool, 기본값: `true`) — 심볼화할 때 인라인 프레임을 포함할지 여부입니다. 이 옵션을 비활성화하면 심볼화 속도는 크게 빨라지지만, 인라인된 함수 호출이 스택에 표시되지 않아 정밀도가 떨어집니다. `symbolized` 및 `collapsed` 포맷에만 영향을 줍니다.
* `jemalloc_profile_text_collapsed_use_count` (Bool, 기본값: `false`) — `collapsed` 포맷을 사용할 때 바이트 대신 할당 횟수를 기준으로 집계합니다.

<div id="example-flamegraph-from-sql">
  ### 예시: SQL로 플레임 그래프 생성하기
</div>

기본 출력 형식이 `collapsed`이므로 출력을 바로 FlameGraph로 파이프할 수 있습니다.

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text" | flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

바이트 수가 아니라 할당 횟수를 기준으로 flame graph를 생성하려면:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text SETTINGS jemalloc_profile_text_collapsed_use_count = 1" | flamegraph.pl --color=mem --title="Allocation Count Flame Graph" --width 2400 > result.svg
```

<div id="flushing-heap-profiles">
  ## 힙 프로파일을 디스크에 플러시하기
</div>

`jeprof`로 오프라인 분석을 하기 위해 힙 프로파일을 파일로 저장해야 한다면, 디스크로 플러시할 수 있습니다.

기본적으로 힙 프로파일 파일은 `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`에 생성됩니다. 여기서 `_pid_`는 ClickHouse의 PID이고, `_seqnum_`은 현재 힙 프로파일의 전역 시퀀스 번호입니다.
Keeper의 기본 파일은 `/tmp/jemalloc_keeper._pid_._seqnum_.heap`이며, 동일한 규칙이 적용됩니다.

현재 프로파일을 플러시하려면 다음을 수행하십시오.

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```

    플러시된 프로파일의 위치가 반환됩니다.
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

`MALLOC_CONF` 환경 변수에 `prof_prefix` 옵션을 추가해 다른 위치를 지정할 수 있습니다.
예를 들어 파일 이름 접두사를 `my_current_profile`로 하여 `/data` 폴더에 프로파일을 생성하려면, 다음 환경 변수로 ClickHouse/Keeper를 실행하면 됩니다.

```sh
MALLOC_CONF=prof_prefix:/data/my_current_profile
```

생성된 파일 이름에는 접두사, PID 및 시퀀스 번호가 추가됩니다.

<div id="analyzing-heap-profile-files-with-jeprof">
  ## `jeprof`를 사용한 힙 프로파일 파일 분석
</div>

힙 프로파일을 디스크에 플러시한 후에는 `jemalloc`의 [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in) 도구를 사용해 분석할 수 있습니다. 설치 방법은 여러 가지가 있습니다.

* 시스템 패키지 관리자 사용
* [jemalloc repo](https://github.com/jemalloc/jemalloc)를 클론한 뒤 루트 폴더에서 `autogen.sh`를 실행합니다. 그러면 `bin` 폴더 안에 `jeprof` script가 생성됩니다

사용 가능한 출력 형식은 다양합니다. 전체 옵션 목록은 `jeprof --help`를 실행해 확인하십시오.

<div id="symbolized-heap-profiles">
  ### 심볼화된 힙 프로파일
</div>

버전 26.1+부터 ClickHouse는 `SYSTEM JEMALLOC FLUSH PROFILE`로 프로파일을 플러시하면 심볼화된 힙 프로파일을 자동으로 생성합니다.
심볼화된 프로파일(`.symbolized` 확장 기능)은 함수 심볼이 내장되어 있으므로 ClickHouse 바이너리 없이도 `jeprof`로 분석할 수 있습니다.

예시로, 다음을 실행하면:

```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

ClickHouse는 심볼 정보가 해석된 프로필의 경로(예: `/tmp/jemalloc_clickhouse.12345.0.heap.symbolized`)를 반환합니다.

그런 다음 `jeprof`를 사용해 이를 직접 분석할 수 있습니다:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --output_format [ > output_file]
```

:::note

**바이너리 불필요**: 심볼이 해석된 프로필(`.symbolized` 파일)을 사용하는 경우 `jeprof`에 ClickHouse 바이너리 경로를 지정할 필요가 없습니다. 따라서 다른 머신에서 분석하거나 바이너리가 업데이트된 후에도 프로필을 훨씬 더 쉽게 분석할 수 있습니다.

:::

이전 방식의, 심볼이 해석되지 않은 힙 프로파일이 있고 ClickHouse 바이너리에 계속 접근할 수 있다면 기존 방식을 사용할 수 있습니다:

```sh
jeprof path/to/clickhouse path/to/heap/profile --output_format [ > output_file]
```

:::note

심볼화되지 않은 프로파일의 경우 `jeprof`는 스택트레이스를 생성할 때 `addr2line`을 사용하는데, 이 과정이 상당히 느릴 수 있습니다.
이 경우 이 도구의 [대체 구현](https://github.com/gimli-rs/addr2line)을 설치하는 것이 좋습니다.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

또는 `llvm-addr2line`도 동일하게 사용할 수 있습니다(단, `llvm-objdump`는 `jeprof`와 호환되지 않으므로 유의하십시오)

이후에는 다음과 같이 사용할 수 있습니다 `jeprof --tools addr2line:/usr/bin/llvm-addr2line,nm:/usr/bin/llvm-nm,objdump:/usr/bin/objdump,c++filt:/usr/bin/llvm-cxxfilt`

:::

두 profiles를 비교할 때는 `--base` 인수를 사용할 수 있습니다:

```sh
jeprof --base /path/to/first.heap.symbolized /path/to/second.heap.symbolized --output_format [ > output_file]
```

<div id="examples">
  ### 예시
</div>

심볼 정보가 포함된 프로파일 사용(권장):

* 각 프로시저를 한 줄씩 적은 텍스트 파일을 생성합니다:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --text > result.txt
```

* 호출 그래프가 포함된 PDF 파일을 생성합니다:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --pdf > result.pdf
```

심볼화되지 않은 프로파일 사용 시(바이너리 필요):

* 각 프로시저를 한 줄에 하나씩 적은 텍스트 파일을 생성합니다:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --text > result.txt
```

* call-graph가 포함된 PDF 파일을 생성합니다:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### 플레임 그래프 생성
</div>

`jeprof`를 사용하면 플레임 그래프를 만들기 위한 collapsed stacks를 생성할 수 있습니다.

`--collapsed` 인수를 사용해야 합니다:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --collapsed > result.collapsed
```

또는 심볼이 해석되지 않은 프로파일:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --collapsed > result.collapsed
```

그 후에는 collapsed stacks를 시각화하는 데 사용할 수 있는 다양한 도구가 있습니다.

가장 널리 사용되는 도구는 `flamegraph.pl`이라는 script를 포함한 [FlameGraph](https://github.com/brendangregg/FlameGraph)입니다:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

또 다른 유용한 도구로는 [speedscope](https://www.speedscope.app/)가 있으며, 이를 사용하면 수집된 스택을 더 대화형 방식으로 분석할 수 있습니다.

<div id="additional-options-for-profiler">
  ## 프로파일러의 추가 옵션
</div>

`jemalloc`에는 프로파일러와 관련된 다양한 옵션이 있습니다. 이러한 옵션은 `MALLOC_CONF` 환경 변수를 수정하여 제어할 수 있습니다.
예를 들어, 할당 샘플 간 인터벌은 `lg_prof_sample`로 제어할 수 있습니다.
N바이트마다 힙 프로파일을 덤프하려면 `lg_prof_interval`을 활성화하면 됩니다.

전체 옵션 목록은 `jemalloc` [참고 페이지](https://jemalloc.net/jemalloc.3.html)에서 확인하는 것이 좋습니다.

<div id="other-resources">
  ## 기타 리소스
</div>

ClickHouse/Keeper는 `jemalloc` 관련 메트릭을 여러 방식으로 노출합니다.

:::warning 경고
이들 메트릭은 서로 동기화되지 않으므로 값에 차이가 생길 수 있다는 점에 유의해야 합니다.
:::

<div id="system-table-asynchronous_metrics">
  ### 시스템 테이블 `asynchronous_metrics`
</div>

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric LIKE '%jemalloc%'
FORMAT Vertical
```

[참고](/ko/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### 시스템 테이블 `jemalloc_bins`
</div>

모든 arena에서 집계한 서로 다른 크기 클래스(bin)의 jemalloc 메모리 할당 정보를 포함합니다.

[참고](/ko/operations/system-tables/jemalloc_bins)

<div id="system-table-jemalloc_stats">
  ### 시스템 테이블 `jemalloc_stats` (26.2+)
</div>

`malloc_stats_print()`의 전체 Output을 하나의 문자열로 반환합니다. `SYSTEM JEMALLOC STATS` 명령과 동일합니다.

```sql
SELECT * FROM system.jemalloc_stats
```

<div id="prometheus">
  ### Prometheus
</div>

`asynchronous_metrics`의 모든 `jemalloc` 관련 메트릭은 ClickHouse와 Keeper 모두에서 Prometheus 엔드포인트를 통해서도 제공됩니다.

[참고](/ko/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Keeper의 `jmst` 4LW 명령
</div>

Keeper는 [기본 메모리 할당자 통계](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics)를 반환하는 `jmst` 4LW 명령을 지원합니다:

```sh
echo jmst | nc localhost 9181
```