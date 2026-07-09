---
description: '데이터를 RAM에 버퍼링해 기록한 뒤, 주기적으로 다른 테이블로 플러시합니다. 읽기 작업 중에는 버퍼와 다른 테이블에서 동시에 데이터를 읽습니다.'
sidebar_label: 'Buffer'
sidebar_position: 120
slug: /engines/table-engines/special/buffer
title: 'Buffer 테이블 엔진'
doc_type: '참고'
---

데이터를 RAM에 버퍼링해 기록한 뒤, 주기적으로 다른 테이블로 플러시합니다. 읽기 작업 중에는 버퍼와 다른 테이블에서 동시에 데이터를 읽습니다.

:::note
Buffer 테이블 엔진의 권장되는 대안은 [비동기 삽입](/ko/guides/best-practices/asyncinserts.md)을 활성화하는 것입니다.
:::

```sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes [,flush_time [,flush_rows [,flush_bytes]]])
```

<div id="engine-parameters">
  ### 엔진 매개변수
</div>

<div id="database">
  #### `database`
</div>

`database` – 데이터베이스 이름입니다. `currentDatabase()` 또는 문자열을 반환하는 다른 상수 표현식을 사용할 수 있습니다.

<div id="table">
  #### `table`
</div>

`table` – 데이터를 플러시할 대상 테이블입니다.

<div id="num_layers">
  #### `num_layers`
</div>

`num_layers` – 병렬 처리 계층입니다. 물리적으로 테이블은 서로 독립적인 `num_layers`개의 버퍼로 구성됩니다.

<div id="min_time-max_time-min_rows-max_rows-min_bytes-and-max_bytes">
  #### `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, and `max_bytes`
</div>

버퍼의 데이터를 플러시하는 조건입니다.

<div id="optional-engine-parameters">
  ### 선택적 엔진 매개변수
</div>

<div id="flush_time-flush_rows-and-flush_bytes">
  #### `flush_time`, `flush_rows`, and `flush_bytes`
</div>

백그라운드에서 버퍼의 데이터를 플러시하는 조건입니다(`flush*` 매개변수가 생략되었거나 0이면 `flush*` 매개변수가 없는 것으로 간주됩니다).

모든 `min*` 조건이 충족되거나 `max*` 조건 중 하나 이상이 충족되면, 데이터가 버퍼에서 플러시되어 대상 테이블에 기록됩니다.

또한 `flush*` 조건 중 하나 이상이 충족되면 백그라운드에서 플러시가 시작됩니다. 이는 `max*`와는 다릅니다. `flush*`를 사용하면 백그라운드 플러시를 별도로 구성할 수 있으므로, Buffer 테이블에 `INSERT` 쿼리를 실행할 때 지연 시간이 추가되는 것을 방지할 수 있습니다.

<div id="min_time-max_time-and-flush_time">
  #### `min_time`, `max_time`, and `flush_time`
</div>

버퍼에 처음 쓰기가 수행된 시점부터 경과한 시간(초)에 대한 조건입니다.

<div id="min_rows-max_rows-and-flush_rows">
  #### `min_rows`, `max_rows`, and `flush_rows`
</div>

버퍼의 행 수를 기준으로 하는 조건입니다.

<div id="min_bytes-max_bytes-and-flush_bytes">
  #### `min_bytes`, `max_bytes`, and `flush_bytes`
</div>

버퍼의 바이트 수에 대한 조건입니다.

쓰기 작업 중에는 데이터가 하나 이상의 임의 버퍼(`num_layers`로 구성됨)에 삽입됩니다. 또는 삽입할 데이터 파트(data part)가 충분히 큰 경우(`max_rows` 또는 `max_bytes`보다 큰 경우)에는 버퍼를 거치지 않고 대상 테이블에 직접 기록됩니다.

데이터를 플러시하는 조건은 `num_layers` 버퍼 각각에 대해 별도로 계산됩니다. 예를 들어 `num_layers = 16`이고 `max_bytes = 100000000`이면 최대 RAM 사용량은 1.6 GB입니다.

예시:

```sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 1, 10, 100, 10000, 1000000, 10000000, 100000000)
```

`merge.hits`와 동일한 구조를 가지며 Buffer 엔진을 사용하는 `merge.hits_buffer` 테이블을 생성합니다. 이 테이블에 데이터를 쓰면 데이터가 RAM에 버퍼링된 후 나중에 &#39;merge.hits&#39; 테이블에 기록됩니다. 단일 버퍼가 생성되며, 다음 조건 중 하나를 만족하면 데이터가 플러시됩니다.

* 마지막 플러시 이후 100초가 지났거나 (`max_time`)
* 100만 행이 기록되었거나 (`max_rows`)
* 100 MB의 데이터가 기록되었거나 (`max_bytes`)
* 10초가 지났고 (`min_time`) 10,000행 (`min_rows`) 및 10 MB (`min_bytes`)의 데이터가 기록되었을 때

예를 들어 1행만 기록된 경우에도 100초가 지나면 다른 조건과 관계없이 플러시됩니다. 반면 많은 행이 기록된 경우에는 데이터가 더 빨리 플러시됩니다.

서버가 중지될 때, 또는 `DROP TABLE`이나 `DETACH TABLE`을 수행할 때도 버퍼링된 데이터는 대상 테이블로 플러시됩니다.

데이터베이스 및 테이블 이름으로 작은따옴표 안의 빈 문자열을 설정할 수 있습니다. 이는 대상 테이블이 없음을 의미합니다. 이 경우 데이터 플러시 조건에 도달하면 버퍼는 단순히 비워집니다. 이는 메모리에 일정 범위의 데이터를 유지하는 데 유용할 수 있습니다.

Buffer 테이블에서 읽을 때는 버퍼와 대상 테이블(있는 경우) 모두의 데이터가 처리됩니다.
Buffer 테이블은 인덱스를 지원하지 않는다는 점에 유의하십시오. 즉, 버퍼의 데이터는 전체 스캔되므로 버퍼가 크면 느릴 수 있습니다. (하위 테이블의 데이터에는 해당 테이블이 지원하는 인덱스가 사용됩니다.)

Buffer 테이블의 컬럼 집합이 하위 테이블의 컬럼 집합과 일치하지 않으면, 두 테이블에 모두 존재하는 컬럼의 부분 집합만 삽입됩니다.

Buffer 테이블과 하위 테이블에서 어느 한 컬럼의 타입이라도 일치하지 않으면 서버 로그에 오류 메시지가 기록되고 버퍼가 비워집니다.
버퍼가 플러시될 때 하위 테이블이 존재하지 않는 경우에도 동일하게 동작합니다.

:::note
2021년 10월 26일 이전 릴리스에서는 Buffer 테이블에 대해 ALTER를 실행하면 `Block structure mismatch` 오류가 발생하므로([#15117](https://github.com/ClickHouse/ClickHouse/issues/15117) 및 [#30565](https://github.com/ClickHouse/ClickHouse/pull/30565) 참고), Buffer 테이블을 삭제한 뒤 다시 생성하는 방법만 사용할 수 있습니다. Buffer 테이블에서 ALTER를 실행하기 전에 사용 중인 릴리스에서 이 오류가 수정되었는지 확인하십시오.
:::

서버가 비정상적으로 다시 시작되면 버퍼의 데이터는 손실됩니다.

`FINAL`과 `SAMPLE`은 Buffer 테이블에서 올바르게 작동하지 않습니다. 이러한 조건은 대상 테이블로 전달되지만 버퍼의 데이터를 처리할 때는 사용되지 않습니다. 이러한 기능이 필요하다면, 쓰기에는 Buffer 테이블만 사용하고 읽기는 대상 테이블에서만 수행할 것을 권장합니다.

Buffer 테이블에 데이터를 추가할 때는 버퍼 중 하나가 잠깁니다. 이 때문에 동시에 해당 테이블에서 읽기 작업을 수행하면 지연이 발생합니다.

Buffer 테이블에 삽입된 데이터는 하위 테이블에 다른 순서와 다른 블록으로 기록될 수 있습니다. 이 때문에 Buffer 테이블은 CollapsingMergeTree에 올바르게 쓰는 용도로 사용하기 어렵습니다. 문제를 방지하려면 `num_layers`를 1로 설정할 수 있습니다.

대상 테이블이 복제된 경우, Buffer 테이블에 쓸 때 복제된 테이블에서 기대되는 일부 특성이 사라집니다. 행 순서와 데이터 파트의 크기가 무작위로 바뀌면서 데이터 중복 제거가 더 이상 작동하지 않으므로, 복제된 테이블에 대해 신뢰할 수 있는 &#39;exactly once&#39; 쓰기를 보장할 수 없습니다.

이러한 단점 때문에 Buffer 테이블은 드문 경우에만 사용할 것을 권장합니다.

Buffer 테이블은 짧은 시간 동안 많은 서버에서 지나치게 많은 INSERT를 받을 때 사용됩니다. 즉, 삽입 전에 데이터를 버퍼링할 수 없어 INSERT를 충분히 빠르게 처리할 수 없는 경우에 사용됩니다.

Buffer 테이블이라도 데이터를 한 번에 한 행씩 삽입하는 것은 비효율적이라는 점에 유의하십시오. 이렇게 하면 초당 수천 행 수준의 속도만 낼 수 있지만, 더 큰 데이터 블록을 삽입하면 초당 100만 행이 넘는 속도를 낼 수 있습니다.