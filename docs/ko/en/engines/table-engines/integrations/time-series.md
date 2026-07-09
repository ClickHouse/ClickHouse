---
description: '타임스탬프와 태그(또는 레이블)에 연관된 값의 집합인 시계열을 저장하는 테이블 엔진입니다.'
sidebar_label: 'TimeSeries'
sidebar_position: 60
slug: /engines/table-engines/special/time_series
title: 'TimeSeries 테이블 엔진'
doc_type: '참고'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="timeseries-table-engine">
  # TimeSeries 테이블 엔진
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

타임스탬프와 태그(또는 레이블)에 연결된 값 집합인 시계열 데이터를 저장하는 테이블 엔진입니다:

```sql
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
이는 향후 릴리스에서 하위 호환되지 않는 방식으로 변경될 수 있는 실험적 기능입니다.
[allow&#95;experimental&#95;time&#95;series&#95;table](/ko/operations/settings/settings#allow_experimental_time_series_table) 설정을 사용하여 TimeSeries 테이블 엔진 사용을 활성화하세요.
`set allow_experimental_time_series_table = 1` 명령을 입력하세요.
:::

<div id="syntax">
  ## 구문
</div>

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[TAGS db.tags_table_name | [TAGS INNER COLUMNS (...)] [TAGS INNER ENGINE engine(arguments)]]
[METRICS db.metrics_table_name | [METRICS INNER COLUMNS (...)] [METRICS INNER ENGINE engine(arguments)]]
```

:::note
키워드 `SAMPLES`에는 하위 호환성을 위해 유지되는 별칭 `DATA`가 있습니다.
:::

<div id="usage">
  ## 사용법
</div>

처음에는 모든 설정을 기본값으로 두고 시작하는 것이 더 쉽습니다(`TimeSeries` 테이블은 컬럼 목록을 지정하지 않고도 생성할 수 있습니다):

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

그러면 이 테이블은 다음 프로토콜에서 사용할 수 있습니다(포트는 서버 구성에 할당해야 합니다):

* [prometheus remote-write](/ko/interfaces/prometheus#remote-write)
* [prometheus remote-read](/ko/interfaces/prometheus#remote-read)

<div id="outer-columns">
  ### 외부 컬럼
</div>

TimeSeries 테이블의 컬럼은 자동으로 생성됩니다. 이들은 외부 컬럼으로, 데이터를 저장하지 않으며 SELECT/INSERT를 위한 인터페이스만 제공합니다. 실제 데이터는 [대상 테이블](#target-tables)에 저장됩니다. 다음은 외부 컬럼 목록입니다.

| Name            | Type                                              | Description                                                                                                                            |
| --------------- | ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_name`   | `String`                                          | 메트릭 이름                                                                                                                                 |
| `tags`          | `Map(String, String)`                             | 시계열의 태그(레이블) 맵                                                                                                                         |
| `time_series`   | `Array(Tuple(DateTime64(3), Float64))` by default | 시계열에 대한 (타임스탬프, 값) 쌍의 배열입니다. 튜플의 타임스탬프 및 스칼라 요소 타입은 samples `INNER COLUMNS` 선언에서 유추할 수 있습니다([외부 컬럼 지정](#specifying-outer-columns) 참조). |
| `metric_family` | `String`                                          | 메트릭 패밀리의 이름(메트릭 메타데이터용)                                                                                                                |
| `type`          | `String`                                          | 메트릭 유형(예: &quot;counter&quot;, &quot;gauge&quot;)                                                                                      |
| `unit`          | `String`                                          | 메트릭 단위                                                                                                                                 |
| `help`          | `String`                                          | 메트릭 설명                                                                                                                                 |

예시:

```sql
INSERT INTO my_table (metric_name, tags, time_series) VALUES
    ('cpu_usage', {'job': 'node_exporter', 'instance': 'host1:9100'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.7)])
```

삽입 시 `metric_name`은 비어 있어도 되며, 이는 메트릭 이름이 `tags`의 `__name__`에 지정된다는 뜻입니다. 예시는 다음과 같습니다:

```sql
INSERT INTO my_table (tags, time_series) VALUES
    ({'__name__': 'cpu_usage', 'job': 'test'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5)])
```

메트릭 메타데이터를 삽입하려면 `metric_family`, `type`, `unit`, `help` 컬럼에 값을 삽입합니다:

```sql
INSERT INTO my_table (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'GET'}, [(now64(), 100.0)],
     'http_requests_total', 'counter', 'requests', 'Total HTTP requests')
```

<div id="specifying-outer-columns">
  ### 외부 컬럼 지정
</div>

외부 `time_series` 컬럼은 기본 `Array(Tuple(DateTime64(3), Float64))` 타입을 재정의하기 위해 `CREATE TABLE` 문에 명시적으로 지정할 수 있습니다. ClickHouse는 Tuple에서 타임스탬프와 스칼라 타입을 추출해 내부 Samples table에 전달합니다:

```sql
CREATE TABLE my_table (time_series Array(Tuple(UInt32, Float32))) ENGINE=TimeSeries
```

이는 samples `INNER COLUMNS` 절에 timestamp 및 value의 컬럼 타입을 직접 선언하는 것과 동일합니다:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp UInt32, value Float32)
```

두 형식을 같은 `CREATE TABLE` 문에서 함께 사용하는 경우, 선언된 타입이 일치해야 합니다.

<div id="target-tables">
  ## 대상 테이블
</div>

`TimeSeries` 테이블에는 자체 데이터가 없으며, 모든 데이터는 대상 테이블에 저장됩니다.
이는 [materialized view](../../../sql-reference/statements/create/view#materialized-view)의 동작 방식과 비슷하지만,
materialized view는 대상 테이블이 하나인 반면
`TimeSeries` 테이블에는 [samples](#samples-table), [tags](#tags-table), [metrics](#metrics-table)라는 3개의 대상 테이블이 있습니다.

대상 테이블은 `CREATE TABLE` 쿼리에서 명시적으로 지정할 수도 있고
`TimeSeries` 테이블 엔진이 내부 대상 테이블을 자동으로 생성할 수도 있습니다.

`TimeSeries` 테이블에 삽입된 행은 변환된 뒤 블록으로 분할되어 이 3개의 대상 테이블에 삽입됩니다.

대상 테이블은 다음과 같습니다:

<div id="samples-table">
  ### Samples table
</div>

*samples* 테이블에는 특정 식별자에 연결된 시계열이 포함됩니다.

*samples* 테이블에는 다음 컬럼이 있어야 합니다.

| 이름          | 필수 여부 | 기본 타입           | 가능한 타입                 | 설명                   |
| ----------- | ----- | --------------- | ---------------------- | -------------------- |
| `id`        | [x]   | `UUID`          | any                    | 메트릭 이름과 태그 조합을 식별합니다 |
| `timestamp` | [x]   | `DateTime64(3)` | `DateTime64(X)`        | 시간 시점                |
| `value`     | [x]   | `Float64`       | `Float32` 또는 `Float64` | `timestamp`에 해당하는 값  |

<div id="tags-table">
  ### Tags 테이블
</div>

*tags* 테이블에는 메트릭 이름과 tags의 각 조합에 대해 계산된 식별자가 포함됩니다.

*tags* 테이블에는 다음 컬럼이 있어야 합니다.

| Name                 | Mandatory? | Default type                          | Possible types                                                                                                          | Description                                                                                                           |
| -------------------- | ---------- | ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `id`                 | [x]        | `UUID`                                | any (must match the type of `id` in the [samples](#samples-table) table)                                                | `id`는 메트릭 이름과 tags의 조합을 식별합니다. DEFAULT 표현식은 이러한 식별자를 계산하는 방법을 지정합니다.                                                  |
| `metric_name`        | [x]        | `LowCardinality(String)`              | `String` or `LowCardinality(String)`                                                                                    | 메트릭 이름                                                                                                                |
| `<tag_value_column>` | [ ]        | `String`                              | `String` or `LowCardinality(String)` or `LowCardinality(Nullable(String))`                                              | 특정 tag의 값입니다. tag 이름과 해당 컬럼 이름은 [tags&#95;to&#95;columns](#settings) 설정에서 지정합니다.                                      |
| `tags`               | [x]        | `Map(LowCardinality(String), String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | 메트릭 이름을 담고 있는 tag `__name__` 및 [tags&#95;to&#95;columns](#settings) 설정에 열거된 이름의 tags를 제외한 tags 맵                      |
| `all_tags`           | [ ]        | `Map(String, String)`                 | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | 임시 컬럼으로, 각 행은 메트릭 이름을 담고 있는 tag `__name__`만 제외한 모든 tags의 맵입니다. 이 컬럼의 유일한 용도는 `id`를 계산할 때 사용하는 것입니다.                   |
| `min_time`           | [ ]        | `Nullable(DateTime64(3))`             | `DateTime64(X)` or `Nullable(DateTime64(X))`                                                                            | 해당 `id`를 가진 시계열의 최소 timestamp입니다. [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings)가 `true`이면 이 컬럼이 생성됩니다. |
| `max_time`           | [ ]        | `Nullable(DateTime64(3))`             | `DateTime64(X)` or `Nullable(DateTime64(X))`                                                                            | 해당 `id`를 가진 시계열의 최대 timestamp입니다. [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings)가 `true`이면 이 컬럼이 생성됩니다. |

<div id="metrics-table">
  ### Metrics 테이블
</div>

*metrics* 테이블에는 수집된 메트릭과 해당 메트릭의 유형 및 설명에 관한 정보가 포함됩니다.

*metrics* 테이블에는 다음 컬럼이 있어야 합니다:

| Name                 | Mandatory? | Default type             | Possible types                       | Description                                                                                                                                                 |
| -------------------- | ---------- | ------------------------ | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_family_name` | [x]        | `String`                 | `String` or `LowCardinality(String)` | 메트릭 패밀리의 이름                                                                                                                                                 |
| `type`               | [x]        | `LowCardinality(String)` | `String` or `LowCardinality(String)` | 메트릭 패밀리의 유형으로, &quot;counter&quot;, &quot;gauge&quot;, &quot;summary&quot;, &quot;stateset&quot;, &quot;histogram&quot;, &quot;gaugehistogram&quot; 중 하나입니다 |
| `unit`               | [x]        | `LowCardinality(String)` | `String` or `LowCardinality(String)` | 메트릭에 사용되는 단위                                                                                                                                                |
| `help`               | [x]        | `String`                 | `String` or `LowCardinality(String)` | 메트릭에 대한 설명                                                                                                                                                  |

<div id="creation">
  ## 생성
</div>

`TimeSeries` 테이블 엔진으로 테이블(table)을 생성하는 방법은 여러 가지가 있습니다.
가장 간단한 문

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

실제로는 다음과 같은 테이블이 생성됩니다(`SHOW CREATE TABLE my_table`을 실행하면 확인할 수 있습니다):

```sql
CREATE TABLE my_table
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

따라서 컬럼은 자동으로 생성되며, 각기 고유한 컬럼 정의를 가진 3개의 내부 대상 테이블도
`INNER COLUMNS` 절에 저장됩니다.

내부 대상 테이블의 이름은 `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
와 같고, 각 대상 테이블은 자체 컬럼 집합을 가집니다:

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

```sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
SETTINGS allow_dimensions_outside_sorting_key = 1
```

```sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

<div id="create-as">
  ## 기존 테이블을 AS로 지정하여 테이블 생성하기
</div>

`CREATE TABLE new_table AS existing_table` 구문은 `existing_table`에서 다음 항목을 복사합니다:

* `SETTINGS`
* 각 kind의 `INNER COLUMNS`
* 각 kind의 `INNER ENGINE`

`existing_table`에 외부 대상이 있으면 이 구문은 사용할 수 없습니다.
외부 컬럼 목록은 복사되지 않고 다시 생성됩니다.

<div id="adjusting-column-types">
  ## 컬럼 타입 조정
</div>

`INNER COLUMNS` 절을 사용하면 내부 대상 테이블의 컬럼 타입을 조정할 수 있습니다. 예를 들어, 타임스탬프를 마이크로초 단위로 저장하고 값을 `Float32`로 저장하려면 다음과 같습니다:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

같은 절을 사용해 코덱과 기타 컬럼 속성을 지정할 수 있습니다.

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta))
```

<div id="id-column">
  ## `id` 컬럼
</div>

`id` 컬럼에는 식별자가 저장되며, 각 식별자는 메트릭 이름과 태그의 조합별로 계산됩니다.
식별자 생성에 사용되는 데이터 유형과 `DEFAULT` 표현식은 `TAGS INNER COLUMNS` 절을 통해 사용자 지정할 수 있습니다:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags))
```

`id` 컬럼 타입은 `UUID`, `UInt64`, `UInt128` 또는 `FixedString(16)` 중 하나여야 합니다. `DEFAULT` 표현식이 지정되지 않으면 ClickHouse가 `id` 타입에 따라 자동으로 선택합니다. samples 및 tags 내부 테이블에 선언된 `id` 타입은 서로 일치해야 합니다.

`id_generator` 설정을 사용하면 `INNER COLUMNS` 절을 사용하지 않고도 동일하게 사용자 지정할 수 있습니다:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(metric_name, all_tags)'
```

이 설정이 지정되면, 컬럼의 `DEFAULT`에 다른 표현식이 들어 있어도 `id` 생성에는 이 설정이 사용됩니다.

<div id="tags-and-all-tags">
  ## `tags` 및 `all_tags` 컬럼
</div>

태그 맵을 담고 있는 컬럼은 `tags`와 `all_tags`의 두 가지입니다. 이 예시에서는 두 컬럼의 의미가 같지만, `tags_to_columns` 설정을 사용하면
서로 다를 수 있습니다. 이 설정을 사용하면 특정 태그를 `tags` 컬럼 내부의 맵에 저장하는 대신 별도의 컬럼에 저장하도록 지정할 수
있습니다:

```sql
CREATE TABLE my_table
ENGINE = TimeSeries 
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

이 statement는 내부 [tags](#tags-table) 대상 테이블에 `instance` 및 `job` 컬럼을 추가합니다.
이 경우 `tags` 컬럼에는 `instance` 및 `job` 태그가 포함되지 않지만,
`all_tags` 컬럼에는 포함됩니다. `all_tags` 컬럼은 임시로만 존재하며, `id` 컬럼의 DEFAULT 표현식에서
사용하는 것이 유일한 목적입니다.

<div id="inner-table-engines">
  ## 내부 대상 테이블의 테이블 엔진
</div>

기본적으로 내부 대상 테이블은 다음 테이블 엔진을 사용합니다.

* [samples](#samples-table) 테이블은 [MergeTree](../mergetree-family/mergetree)를 사용합니다.
* [tags](#tags-table) 테이블은 [AggregatingMergeTree](../mergetree-family/aggregatingmergetree)를 사용합니다. 이 테이블에는 동일한 데이터가 여러 번 삽입되는 경우가 많아 중복을 제거할 방법이 필요하며,
  `min_time` 및 `max_time` 컬럼에 대한 집계도 필요하기 때문입니다.
* [metrics](#metrics-table) 테이블은 [ReplacingMergeTree](../mergetree-family/replacingmergetree)를 사용합니다. 이 테이블에도 동일한 데이터가 여러 번 삽입되는 경우가 많아 중복을 제거할 방법이 필요하기 때문입니다.

다음과 같이 지정하면 내부 대상 테이블에 다른 테이블 엔진을 사용할 수도 있습니다.

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

[tags](#tags-table) 테이블은 태그 컬럼(및 `tags`/`all_tags` 맵)을 sorting key 밖에 두는데,
이는 `AggregatingMergeTree`에서 기본적으로 허용되지 않습니다([`allow_dimensions_outside_sorting_key`](../mergetree-family/aggregatingmergetree) 참고).
하지만 여기서는 해당 컬럼들이 sorting key의 일부인 `id`에 함수적으로 종속되므로 안전합니다. 따라서 background 머지로 함께 축약되는 모든
행은 동일한 값을 공유합니다. 내부 tags 테이블이 생성되거나 위와 같이 해당
engine이 Inline으로 지정되면 `TimeSeries`는 해당 테이블에 `allow_dimensions_outside_sorting_key = 1`을 자동으로 설정합니다.
수동으로 생성한 [external](#external-target-tables) 집계용 tags 테이블은 직접 설정해야 합니다.

<div id="external-target-tables">
  ## 외부 대상 테이블
</div>

`TimeSeries` 테이블에서 수동으로 생성한 테이블을 사용하도록 설정할 수 있습니다:

```sql
CREATE TABLE samples_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries SAMPLES samples_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

외부 테이블의 컬럼 타입(`id`, `timestamp`, `value` 및 [`tags_to_columns`](#settings)에 나열된 `<tag_value_column>`들)은 `TimeSeries` 테이블이 내부적으로 생성하는 타입과 일치해야 합니다(타입 제약 조건은 [Samples table](#samples-table), [Tags 테이블](#tags-table), [Metrics 테이블](#metrics-table)을 참조하십시오). 타입 불일치는 `CREATE` 시점에 보고됩니다.

외부 tags 대상의 id 생성기 표현식은 `INSERT` 시점에 다음 순서로 결정됩니다. 먼저 [`id_generator`](#settings) 설정(설정된 경우), 다음으로 외부 테이블의 `id` 컬럼에 선언된 `DEFAULT`(있는 경우), 마지막으로 `id` 타입에서 파생된 정규 생성기입니다. 따라서 이 설정은 외부 테이블에 선언된 `DEFAULT`보다 우선합니다. 자세한 내용은 [The `id` column](#id-column)을 참조하십시오.

<div id="altering-settings">
  ## 설정 변경
</div>

다음 두 가지 설정은 `CREATE` 이후에도 변경할 수 있습니다.

* `id_generator`
* `filter_by_min_time_and_max_time`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)';
ALTER TABLE my_table MODIFY SETTING filter_by_min_time_and_max_time = 0;
```

데이터가 이미 Tags 테이블에 있는 상태에서 `id_generator`를 변경하면 동일한 메트릭+태그 조합에 대해 서로 다른 ID가 생성될 수 있습니다. 기존 행은 기존 ID를 유지하고, 새 행은 새 생성기를 사용합니다.

다른 설정은 `CREATE` 시점에 내부 테이블의 스키마(schema)에 반영되므로 `ALTER ... MODIFY SETTING`으로는 변경할 수 없습니다.

<div id="settings">
  ## 설정
</div>

다음은 `TimeSeries` 테이블을 정의할 때 지정할 수 있는 설정 목록입니다:

| 이름                                   | 유형   | 기본값            | 설명                                                                                                                                                                              |
| ------------------------------------ | ---- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id_generator`                       | 표현식  | `id` 유형에 따라 다름 | 태그로부터 시계열의 식별자(지문)를 계산하는 표현식입니다. 설정하지 않으면 `id` 컬럼의 기본 표현식을 사용합니다. `id` 컬럼의 기본 표현식도 설정되지 않은 경우 표현식이 자동으로 선택됩니다                                                                   |
| `tags_to_columns`                    | 맵    | {}             | [tags](#tags-table) 테이블에서 어떤 태그를 별도의 컬럼으로 분리할지 지정하는 맵입니다. 구문: `{'tag1': 'column1', 'tag2' : column2, ...}`                                                                      |
| `use_all_tags_column_to_generate_id` | Bool | true           | 시계열 식별자를 계산하는 표현식을 생성할 때, 이 플래그를 사용하면 해당 계산에 `all_tags` 컬럼을 사용할 수 있습니다                                                                                                          |
| `store_min_time_and_max_time`        | Bool | true           | true로 설정하면 테이블은 각 시계열에 대해 `min_time` 및 `max_time`을 저장합니다                                                                                                                        |
| `aggregate_min_time_and_max_time`    | Bool | true           | 내부 대상 `tags` 테이블을 생성할 때, 이 플래그를 사용하면 `min_time` 컬럼의 유형으로 `Nullable(DateTime64(3))` 대신 `SimpleAggregateFunction(min, Nullable(DateTime64(3)))`를 사용하며, `max_time` 컬럼에도 동일하게 적용됩니다 |
| `filter_by_min_time_and_max_time`    | Bool | true           | true로 설정하면 테이블은 시계열을 필터링할 때 `min_time` 및 `max_time` 컬럼을 사용합니다                                                                                                                   |

<div id="functions">
  # 함수
</div>

다음은 `TimeSeries` 테이블을 인수로 받는 함수를 나열한 목록입니다:

* [timeSeriesSamples](../../../sql-reference/table-functions/timeSeriesSamples.md)
* [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
* [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)