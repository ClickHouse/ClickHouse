---
description: '가설(what-if) 인덱스 문서'
sidebar_label: '가설 인덱스'
sidebar_position: 47
slug: /sql-reference/statements/hypothetical-index
title: '가설 인덱스'
doc_type: 'reference'
---

<div id="hypothetical-indexes">
  # 가설 인덱스
</div>

가설 인덱스는 실제로 생성하거나 저장하지 않은 상태로 `MergeTree` 계열 테이블에 ATTACH할 수 있는, 세션 범위의 가상 스킵 인덱스입니다. 이 인덱스는 현재 세션 내에서만 존재하며, 실제 스킵 인덱스가 쿼리에 어떤 영향을 미칠지 [`EXPLAIN WHATIF`](/ko/sql-reference/statements/explain#explain-whatif)로 추정할 때 사용됩니다. 일반적으로 스킵 비율(건너뛸 수 있는 마크의 비율)과 마크 및 바이트 기준의 대략적인 비용을 추정합니다.

가설 인덱스를 사용하면 디스크에 구체화하기 전에, 그에 따른 비용을 들이지 않고도 후보 인덱스를 평가할 수 있습니다.

<div id="create-hypothetical-index">
  ## CREATE HYPOTHETICAL INDEX
</div>

```sql
CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name
    ON [db.]table_name (expression) TYPE type[(args)] [GRANULARITY value]
```

구문은 `ALTER TABLE ... ADD INDEX`와 동일하지만, 인덱스는 생성되거나 기록되지 않으며 현재 세션에는 인덱스 설명만 저장됩니다.

* `name` — 인덱스 이름입니다. 이 세션에서 `(database, table)` 내에서는 고유해야 합니다.
* `expression` — 인덱싱할 컬럼 또는 표현식입니다.
* `TYPE type` — `minmax`, `set(N)`, `bloom_filter(p)`, `ngrambf_v1(...)`, `tokenbf_v1(...)`입니다. `text` 및 `vector_similarity`는 지원되지 않으므로 `CREATE` 시점에 거부됩니다. 실제 `ALTER TABLE ... ADD INDEX` 검사는 세션 전용 저장소가 복제할 수 없는 테이블 수준 설정에 의존하기 때문입니다.
* `GRANULARITY value` — 인덱스 그래뉼당 데이터 그래뉼 수입니다. 기본값은 1입니다.

대상 테이블은 `Atomic` 데이터베이스의 `MergeTree` 계열 테이블이어야 합니다(UUID가 있어야 합니다). UUID가 없는 테이블(예: 레거시 `Ordinary` 데이터베이스의 테이블 또는 구형 구문 `MergeTree`)은 세션 저장소가 테이블 UUID를 기준으로 가설 인덱스를 관리하므로 거부됩니다.

**예시**

```sql
CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
```

<div id="evaluating-a-hypothetical-index-with-explain-whatif">
  ## EXPLAIN WHATIF로 가설 인덱스 평가하기
</div>

가설 인덱스를 정의하는 것만으로는 아무런 효과가 없습니다. 쿼리에 어떤 영향을 미치는지 확인하려면 대표적인 `SELECT` 쿼리에 대해 [`EXPLAIN WHATIF`](/ko/sql-reference/statements/explain#explain-whatif)를 실행하십시오. 이 추정기는 각 후보 인덱스의 적용 가능성, 읽게 될 마크, 산출된 건너뛰기 비율(skip ratio), 그리고 추정값이 어떤 방식으로 산출되었는지(`empirical`, `statistical`, 또는 `applicability_only`)를 보고합니다.

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

결과:

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100
  est_bytes:   85.52 KiB

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    875.00 B
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
  elapsed_us:       631
```

`est_bytes`는 테이블의 평균 행 크기를 바탕으로 추정한 값이므로, 정확한 수치는 저장 방식과 압축에 따라 달라집니다.

인메모리 실측 스캔을 건너뛰고 대신 [컬럼 통계(column statistics)](/ko/engines/table-engines/mergetree-family/mergetree#column-statistics)를 기준으로 추정하려면, 먼저 관련 컬럼에 통계를 정의하고(기본적으로 비활성화됨), 구체화 mutation이 완료될 때까지 기다린 다음, 실측 경로를 비활성화하십시오:

```sql
ALTER TABLE t ADD STATISTICS b TYPE TDigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;

EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    1.66 KiB
  skip_ratio:   99.9%

Estimation:
  source:           statistical
  empirical_status: disabled
```

전체 출력 스키마와 설정은 [`EXPLAIN WHATIF`](/ko/sql-reference/statements/explain#explain-whatif) 참고 문서를 참조하십시오.

<div id="drop-hypothetical-index">
  ## DROP HYPOTHETICAL INDEX
</div>

```sql
DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table_name
```

현재 세션에 있는 가설 인덱스를 제거합니다.

<div id="drop-all-hypothetical-indexes">
  ## DROP ALL HYPOTHETICAL INDEXES
</div>

```sql
DROP ALL HYPOTHETICAL INDEXES
```

현재 세션에 정의된 모든 가설 인덱스를 테이블과 관계없이 제거합니다.

<div id="scope-and-lifetime">
  ## 범위와 수명
</div>

* 가설 인덱스는 **현재 세션**에만 존재하며, 다른 세션에서는 볼 수 없고 세션이 종료되면 폐기됩니다.
* 가설 인덱스를 정의하거나 삭제해도 실제 인덱스가 생성되지는 않으며, 테이블에 대한 일반 쿼리에는 전혀 영향을 주지 않습니다. 다만 empirical `EXPLAIN WHATIF`는 메모리에서 후보 인덱스를 구축하기 위해 테이블 데이터를 읽으며, 이 스캔은 세션의 읽기 제한과 쿼터에 포함됩니다.
* 현재 세션의 가설 인덱스는 [`system.hypothetical_indexes`](/ko/operations/system-tables/hypothetical_indexes)에서 확인하십시오.

<div id="limitations">
  ## 제한 사항
</div>

`text` 및 `vector_similarity` 후보는 `CREATE HYPOTHETICAL INDEX` 시점에 거부됩니다. 실제 검증은 세션 전용 저장소가 복제할 수 없는 테이블 수준 설정에 따라 달라지기 때문입니다.

`EXPLAIN WHATIF`는 `FINAL`이 포함된 쿼리에 대해 `status: not_applicable`를 보고합니다(스킵 인덱스 프루닝이 `PrimaryKeyExpand`와 상호작용하기 때문입니다). 또한 쿼리가 프로젝션에서 처리되면 `NOT_IMPLEMENTED` 오류를 반환합니다(부모 테이블 인덱스는 프로젝션 파트에 구체화되지 않습니다).

경험적 `skip_ratio`는 **상한**입니다. 이는 살아남은 각 granule을 개별적으로 계산하며, seek gap 병합(`merge_tree_min_rows_for_seek` / `merge_tree_min_bytes_for_seek`)이나 분리(`OR`) 프레디케이트에서 후보와 기존 스킵 인덱스의 조합은 모델링하지 않습니다. 따라서 실제 구체화된 인덱스는 약간 더 많이 읽을 수도 있고, 반대로 추정치에 나타나지 않는 경우에도 프루닝할 수 있습니다.

<div id="required-privileges">
  ## 필요한 권한
</div>

`CREATE HYPOTHETICAL INDEX`에는 인덱스 표현식에서 참조하는 컬럼에 대한 `SELECT` 권한이 필요합니다 — 컬럼 수준의 `SELECT`(예: `GRANT SELECT(b)`)만으로도 충분합니다 — 실제 `EXPLAIN WHATIF` 실행 시 해당 컬럼을 읽기 때문입니다.

`DROP HYPOTHETICAL INDEX`와 `DROP ALL HYPOTHETICAL INDEXES`에는 추가 권한이 필요하지 않습니다. 세션 로컬 저장소에서 항목만 제거합니다.

<div id="see-also">
  ## 관련 항목
</div>

* [`EXPLAIN WHATIF`](/ko/sql-reference/statements/explain#explain-whatif)
* [`system.hypothetical_indexes`](/ko/operations/system-tables/hypothetical_indexes)
* [데이터 스키핑 인덱스](/ko/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)