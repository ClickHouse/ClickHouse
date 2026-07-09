---
description: 'ClickHouse의 메모리 할당 프로파일링을 설명하는 페이지'
sidebar_label: '25.9 이전 버전의 메모리 할당 프로파일링'
slug: /operations/allocation-profiling-old
title: '25.9 이전 버전의 메모리 할당 프로파일링'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling-for-versions-before-259">
  # 25.9 이전 버전의 메모리 할당 프로파일링
</div>

ClickHouse는 전역 메모리 할당자로 [jemalloc](https://github.com/jemalloc/jemalloc)을 사용합니다. jemalloc에는 메모리 할당 샘플링 및 프로파일링을 위한 몇 가지 도구가 포함되어 있습니다.
메모리 할당 프로파일링을 더 편리하게 수행할 수 있도록 Keeper의 four letter word(4LW) 명령과 함께 `SYSTEM` 명령이 제공됩니다.

<div id="sampling-allocations-and-flushing-heap-profiles">
  ## 메모리 할당 샘플링 및 힙 프로필 플러시
</div>

`jemalloc`에서 메모리 할당을 샘플링하고 프로파일링하려면 환경 변수 `MALLOC_CONF`를 사용해 프로파일링을 활성화한 상태로 ClickHouse/Keeper를 시작해야 합니다:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:true
```

`jemalloc`은 메모리 할당을 샘플링하고 해당 정보를 내부에 저장합니다.

다음을 실행하면 `jemalloc`에서 현재 프로파일을 플러시할 수 있습니다.

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

기본적으로 힙 프로파일 파일은 `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`에 생성됩니다. 여기서 `_pid_`는 ClickHouse의 PID이고, `_seqnum_`은 현재 힙 프로파일의 전역 시퀀스 번호입니다.
Keeper의 기본 파일은 `/tmp/jemalloc_keeper._pid_._seqnum_.heap`이며, 동일한 규칙을 따릅니다.

`MALLOC_CONF` 환경 변수에 `prof_prefix` 옵션을 추가하면 다른 위치를 지정할 수 있습니다.
예를 들어, 파일 이름 접두사를 `my_current_profile`로 하여 `/data` 폴더에 프로파일을 생성하려면 다음 환경 변수와 함께 ClickHouse/Keeper를 실행할 수 있습니다.

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
```

생성된 파일명에는 prefix, PID, 시퀀스 번호가 추가됩니다.

<div id="analyzing-heap-profiles">
  ## 힙 프로필 분석
</div>

힙 프로필이 생성된 후에는 이를 분석해야 합니다.
이를 위해 `jemalloc`의 [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in) 도구를 사용할 수 있습니다. 설치 방법은 여러 가지가 있습니다.

* 시스템 패키지 관리자를 사용합니다
* [jemalloc 저장소](https://github.com/jemalloc/jemalloc)를 복제한 뒤 루트 폴더에서 `autogen.sh`를 실행합니다. 그러면 `bin` 폴더에 `jeprof` 스크립트가 생성됩니다

:::note
`jeprof`는 스택트레이스를 생성하기 위해 `addr2line`을 사용하므로 매우 느릴 수 있습니다.
그런 경우 이 도구의 [대체 구현체](https://github.com/gimli-rs/addr2line)를 설치하는 것이 좋습니다.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

:::

`jeprof`를 사용하면 힙 프로파일에서 다양한 포맷을 생성할 수 있습니다.
사용법과 도구에서 제공하는 다양한 옵션은 `jeprof --help`를 실행해 확인하는 것이 좋습니다.

일반적으로 `jeprof` 명령은 다음과 같이 사용합니다:

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

두 프로파일 간에 어떤 메모리 할당(allocations)이 발생했는지 비교하려면 `base` 인수를 설정할 수 있습니다:

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

<div id="examples">
  ### 예시
</div>

* 각 프로시저를 한 줄에 하나씩 적은 텍스트 파일을 생성하려는 경우:

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

* 호출 그래프가 포함된 PDF 파일을 생성하려면:

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### 플레임 그래프 생성
</div>

`jeprof`를 사용하면 플레임 그래프를 생성하기 위한 축약된 스택을 만들 수 있습니다.

`--collapsed` 인수를 사용해야 합니다:

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
```

그 후에는 축약된 스택을 시각화하는 데 다양한 도구를 사용할 수 있습니다.

가장 널리 쓰이는 도구는 [FlameGraph](https://github.com/brendangregg/FlameGraph)이며, 여기에는 `flamegraph.pl`이라는 스크립트가 포함되어 있습니다:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

또 다른 유용한 도구로는 수집된 스택을 보다 대화형으로 분석할 수 있는 [speedscope](https://www.speedscope.app/)가 있습니다.

<div id="controlling-allocation-profiler-during-runtime">
  ## 런타임 중 allocation 프로파일러 제어
</div>

ClickHouse/Keeper를 프로파일러가 활성화된 상태로 시작하면, 런타임 중 메모리 할당 프로파일링을 비활성화하거나 활성화하는 추가 명령을 사용할 수 있습니다.
이 명령을 사용하면 특정 인터벌만 더 쉽게 프로파일링할 수 있습니다.

프로파일러를 비활성화하려면:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC DISABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmdp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

프로파일러를 활성화하려면:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC ENABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmep | nc localhost 9181
    ```
  </TabItem>
</Tabs>

기본적으로 활성화되어 있는 `prof_active` 옵션을 설정하여 프로파일러의 초기 상태를 제어할 수도 있습니다.
예를 들어 시작 중에는 메모리 할당을 샘플링하지 않고 시작 후에만 하려면, 프로파일러를 활성화할 수 있습니다. 다음 환경 변수를 사용하여 ClickHouse/Keeper를 시작할 수 있습니다:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

프로파일러는 나중에 활성화할 수 있습니다.

<div id="additional-options-for-profiler">
  ## 프로파일러 관련 추가 옵션
</div>

`jemalloc`에는 프로파일러와 관련된 다양한 옵션이 있습니다. 이러한 옵션은 `MALLOC_CONF` 환경 변수를 수정하여 제어할 수 있습니다.
예를 들어, 할당 샘플 간 인터벌은 `lg_prof_sample`로 제어할 수 있습니다.
N바이트마다 힙 프로파일을 덤프하려면 `lg_prof_interval`을 활성화하면 됩니다.

전체 옵션 목록은 `jemalloc`의 [참고 페이지](https://jemalloc.net/jemalloc.3.html)에서 확인하는 것이 좋습니다.

<div id="other-resources">
  ## 기타 리소스
</div>

ClickHouse/Keeper는 `jemalloc` 관련 메트릭을 다양한 방식으로 노출합니다.

:::warning 경고
중요: 이러한 메트릭은 서로 동기화되지 않으므로 값에 차이가 생길 수 있습니다.
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

모든 arena에서 집계한, 서로 다른 크기 클래스(bin)별 jemalloc 메모리 할당자를 통한 메모리 할당 정보를 포함합니다.

[참고](/ko/operations/system-tables/jemalloc_bins)

<div id="prometheus">
  ### Prometheus
</div>

`asynchronous_metrics`의 모든 `jemalloc` 관련 메트릭은 ClickHouse와 Keeper 모두에서 Prometheus 엔드포인트를 통해서도 제공됩니다.

[참고](/ko/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Keeper의 `jmst` 4LW 명령
</div>

Keeper는 [기본 메모리 할당자 통계 정보](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics)를 반환하는 `jmst` 4LW 명령을 지원합니다:

```sh
echo jmst | nc localhost 9181
```