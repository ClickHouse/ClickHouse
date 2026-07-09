---
description: 'clickhouse-benchmark 문서 '
sidebar_label: 'clickhouse-benchmark'
sidebar_position: 61
slug: /operations/utilities/clickhouse-benchmark
title: 'clickhouse-benchmark'
doc_type: 'reference'
---

ClickHouse 서버에 연결하여 지정된 쿼리를 반복적으로 전송합니다.

**구문**

```bash
$ clickhouse-benchmark --query ["single query"] [keys]
```

또는

```bash
$ echo "single query" | clickhouse-benchmark [keys]
```

또는

```bash
$ clickhouse-benchmark [keys] <<< "single query"
```

여러 개의 쿼리를 전송하려면 텍스트 파일을 만들고 이 파일에 각 쿼리를 한 줄씩 입력하십시오. 예시:

```sql
SELECT * FROM system.numbers LIMIT 10000000;
SELECT 1;
```

그런 다음 이 파일을 `clickhouse-benchmark`의 표준 입력으로 전달하십시오:

```bash
clickhouse-benchmark [keys] < queries_file;
```

<div id="clickhouse-benchmark-command-line-options">
  ## 명령줄 옵션
</div>

* `--query=QUERY` — 실행할 쿼리입니다. 이 매개변수를 지정하지 않으면 `clickhouse-benchmark`가 표준 입력에서 쿼리를 읽습니다.
* `--query_id=ID` — 쿼리 ID입니다.
* `--query_id_prefix=ID_PREFIX` — 쿼리 ID 접두사입니다.
* `--queries-format=FORMAT` — 표준 입력에서 읽는 쿼리의 포맷입니다. 가능한 값: `tsv`(기본값, 각 줄에 탭 이스케이프된 쿼리 1개) 및 `script`(세미콜론으로 구분된 여러 쿼리의 스크립트로 입력을 구문 분석)입니다. `script`의 제한 사항: `INSERT ... FORMAT` 쿼리는 반드시 한 줄에 있어야 합니다.
* `-c N`, `--concurrency=N` — `clickhouse-benchmark`가 동시에 전송하는 쿼리 수입니다. 기본값: 1입니다.
* `-C N`, `--max_concurrency=N` — 병렬 쿼리 수를 지정한 값까지 점진적으로 늘리며, 각 Concurrency 수준마다 보고서 1개를 생성합니다.
* `--precise` — 가중 메트릭을 사용한 정밀한 인터벌별 보고를 활성화합니다.
* `-d N`, `--delay=N` — 중간 보고 간 인터벌(초)입니다(보고를 비활성화하려면 0으로 설정). 기본값: 1입니다.
* `-h HOST`, `--host=HOST` — 서버 호스트입니다. 기본값: `localhost`입니다. [비교 모드](#clickhouse-benchmark-comparison-mode)에서는 여러 개의 `-h` 옵션을 사용할 수 있습니다.
* `-i N`, `--iterations=N` — 전체 쿼리 수입니다. 기본값: 0(무한 반복)입니다.
* `-r`, `--randomize` — 입력 쿼리가 2개 이상인 경우 쿼리 실행 순서를 무작위로 설정합니다.
* `-s`, `--secure` — `TLS` 연결을 사용합니다.
* `-t N`, `--timelimit=N` — 제한 시간(초)입니다. 지정한 제한 시간에 도달하면 `clickhouse-benchmark`가 쿼리 전송을 중지합니다. 기본값: 0(제한 시간 비활성화)입니다.
* `--port=N` — 서버 포트입니다. 기본값: 9000입니다. [비교 모드](#clickhouse-benchmark-comparison-mode)에서는 여러 개의 `--port` 옵션을 사용할 수 있습니다.
* `--confidence=N` — T-test의 신뢰 수준입니다. 가능한 값: 0(80%), 1(90%), 2(95%), 3(98%), 4(99%), 5(99.5%)입니다. 기본값: 5입니다. [비교 모드](#clickhouse-benchmark-comparison-mode)에서 `clickhouse-benchmark`는 선택한 신뢰 수준으로 두 분포가 서로 다르지 않은지 판단하기 위해 [Independent two-sample Student&#39;s t-test](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test)를 수행합니다.
* `--cumulative` — 인터벌별 데이터 대신 누적 데이터를 출력합니다.
* `--database=DATABASE_NAME` — ClickHouse 데이터베이스 이름입니다. 기본값: `default`입니다.
* `--user=USERNAME` — ClickHouse 사용자 이름입니다. 기본값: `default`입니다.
* `--password=PSWD` — ClickHouse 사용자 비밀번호입니다. 기본값: 빈 문자열입니다.
* `--stacktrace` — 스택 트레이스를 출력합니다. 이 옵션을 설정하면 `clickhouse-benchmark`가 예외의 스택 트레이스를 출력합니다.
* `--stage=WORD` — 서버에서의 쿼리 처리 단계입니다. ClickHouse는 지정한 단계에서 쿼리 처리를 중단하고 `clickhouse-benchmark`에 응답을 반환합니다. 가능한 값: `complete`, `fetch_columns`, `with_mergeable_state`입니다. 기본값: `complete`입니다.
* `--roundrobin` — 서로 다른 `--host`/`--port`의 쿼리를 비교하는 대신, 각 쿼리마다 무작위 `--host`/`--port` 하나를 선택해 해당 대상으로 쿼리를 전송합니다.
* `--reconnect=N` — 재연결 동작을 제어합니다. 가능한 값: 0(재연결 안 함), 1(모든 쿼리마다 재연결), 또는 N(매 N개 쿼리 후 재연결)입니다. 기본값: 0입니다.
* `--max-consecutive-errors=N` — 허용되는 연속 오류 수입니다. 기본값: 0입니다.
* `--ignore-error`,`--continue_on_errors` — 쿼리가 실패해도 테스트를 계속합니다.
* `--client-side-time` — 서버 측 시간 대신 네트워크 통신을 포함한 시간을 표시합니다. 단, 22.8 이전 서버 버전에서는 항상 클라이언트 측 시간을 표시합니다.
* `--proto-caps` — 데이터 전송에서 청크 분할(chunking) 활성화 여부를 설정합니다. 선택 가능한 값(쉼표로 구분 가능): `chunked_optional`, `notchunked`, `notchunked_optional`, `send_chunked`, `send_chunked_optional`, `send_notchunked`, `send_notchunked_optional`, `recv_chunked`, `recv_chunked_optional`, `recv_notchunked`, `recv_notchunked_optional`. 기본값: `notchunked`입니다.
* `--help` — 도움말 메시지를 표시합니다.
* `--verbose` — 도움말 메시지의 상세 수준을 높입니다.

쿼리에 일부 [설정](/ko/operations/settings/overview)을 적용하려면 `--<session setting name>=SETTING_VALUE` 키 형식으로 전달하십시오. 예를 들어 `--max_memory_usage=1048576`입니다.

<div id="clickhouse-benchmark-environment-variable-options">
  ## 환경 변수 옵션
</div>

사용자 이름, 비밀번호, 호스트는 환경 변수 `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_HOST`를 통해 설정할 수 있습니다.
명령줄 인수 `--user`, `--password`, `--host`는 환경 변수보다 우선 적용됩니다.

<div id="clickhouse-benchmark-output">
  ## 출력
</div>

기본적으로 `clickhouse-benchmark`는 각 `--delay` 인터벌마다 보고서를 출력합니다.

보고서 예시는 다음과 같습니다:

```text
Queries executed: 10.

localhost:9000, queries 10, QPS: 6.772, RPS: 67904487.440, MiB/s: 518.070, result RPS: 67721584.984, result MiB/s: 516.675.

0.000%      0.145 sec.
10.000%     0.146 sec.
20.000%     0.146 sec.
30.000%     0.146 sec.
40.000%     0.147 sec.
50.000%     0.148 sec.
60.000%     0.148 sec.
70.000%     0.148 sec.
80.000%     0.149 sec.
90.000%     0.150 sec.
95.000%     0.150 sec.
99.000%     0.150 sec.
99.900%     0.150 sec.
99.990%     0.150 sec.
```

보고서에서 다음을 확인할 수 있습니다:

* `Queries executed:` 필드의 쿼리 수.

* 다음 항목을 순서대로 포함하는 상태 문자열:

  * ClickHouse 서버의 엔드포인트.
  * 처리된 쿼리 수.
  * QPS: `--delay` 인수로 지정된 주기 동안 서버가 초당 수행한 쿼리 수입니다.
  * RPS: `--delay` 인수로 지정된 주기 동안 서버가 초당 읽은 행 수입니다.
  * MiB/s: `--delay` 인수로 지정된 주기 동안 서버가 초당 읽은 메비바이트 수입니다.
  * result RPS: `--delay` 인수로 지정된 주기 동안 서버가 쿼리 결과로 초당 반환한 행 수입니다.
  * result MiB/s. `--delay` 인수로 지정된 주기 동안 서버가 쿼리 결과로 초당 반환한 메비바이트 수입니다.

* 쿼리 실행 시간의 백분위수.

<div id="clickhouse-benchmark-comparison-mode">
  ## 비교 모드
</div>

`clickhouse-benchmark`는 실행 중인 두 ClickHouse 서버의 성능을 비교할 수 있습니다.

비교 모드를 사용하려면 `--host`, `--port` 키 두 쌍으로 두 서버의 엔드포인트를 지정하십시오. 인수 목록에서는 키의 위치를 기준으로 짝이 맞춰지며, 첫 번째 `--host`는 첫 번째 `--port`와, 그다음 `--host`는 그다음 `--port`와 짝지어집니다. `clickhouse-benchmark`는 두 서버에 연결한 다음 쿼리를 전송합니다. 각 쿼리는 두 서버 중 하나에 무작위로 전송됩니다. 결과는 표로 표시됩니다.

<div id="clickhouse-benchmark-example">
  ## 예시
</div>

```bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark --host=localhost --port=9001 --host=localhost --port=9000 -i 10
```

```text
Loaded 1 queries.

Queries executed: 5.

localhost:9001, queries 2, QPS: 3.764, RPS: 75446929.370, MiB/s: 575.614, result RPS: 37639659.982, result MiB/s: 287.168.
localhost:9000, queries 3, QPS: 3.815, RPS: 76466659.385, MiB/s: 583.394, result RPS: 38148392.297, result MiB/s: 291.049.

0.000%          0.258 sec.      0.250 sec.
10.000%         0.258 sec.      0.250 sec.
20.000%         0.258 sec.      0.250 sec.
30.000%         0.258 sec.      0.267 sec.
40.000%         0.258 sec.      0.267 sec.
50.000%         0.273 sec.      0.267 sec.
60.000%         0.273 sec.      0.267 sec.
70.000%         0.273 sec.      0.267 sec.
80.000%         0.273 sec.      0.269 sec.
90.000%         0.273 sec.      0.269 sec.
95.000%         0.273 sec.      0.269 sec.
99.000%         0.273 sec.      0.269 sec.
99.900%         0.273 sec.      0.269 sec.
99.990%         0.273 sec.      0.269 sec.

No difference proven at 99.5% confidence
```