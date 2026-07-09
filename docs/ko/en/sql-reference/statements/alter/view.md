---
description: 'ALTER TABLE ... MODIFY QUERY SQL 문에 대한 문서'
sidebar_label: 'VIEW'
sidebar_position: 50
slug: /sql-reference/statements/alter/view
title: 'ALTER TABLE ... MODIFY QUERY SQL 문'
doc_type: 'reference'
---

수집 과정을 중단하지 않고, [materialized view](/ko/sql-reference/statements/create/view#materialized-view)를 생성할 때 지정한 `SELECT` 쿼리를 `ALTER TABLE ... MODIFY QUERY` SQL 문으로 수정할 수 있습니다.

이 명령은 `TO [db.]name` 절을 사용해 생성한 materialized view를 변경하기 위해 만들어졌습니다. 기반 저장소 테이블의 구조는 변경하지 않으며 materialized view의 컬럼 정의도 바꾸지 않으므로, `TO [db.]name` 절 없이 생성된 materialized view에는 적용 범위가 매우 제한적입니다.

**TO 테이블 사용 예시**

```sql
CREATE TABLE events (ts DateTime, event_type String)
ENGINE = MergeTree ORDER BY (event_type, ts);

CREATE TABLE events_by_day (ts DateTime, event_type String, events_cnt UInt64)
ENGINE = SummingMergeTree ORDER BY (event_type, ts);

CREATE MATERIALIZED VIEW mv TO events_by_day AS
SELECT toStartOfDay(ts) ts, event_type, count() events_cnt
FROM events
GROUP BY ts, event_type;

INSERT INTO events
SELECT DATE '2020-01-01' + interval number * 900 second,
       ['imp', 'click'][number%2+1]
FROM numbers(100);

SELECT ts, event_type, sum(events_cnt)
FROM events_by_day
GROUP BY ts, event_type
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─sum(events_cnt)─┐
│ 2020-01-01 00:00:00 │ click      │              48 │
│ 2020-01-01 00:00:00 │ imp        │              48 │
│ 2020-01-02 00:00:00 │ click      │               2 │
│ 2020-01-02 00:00:00 │ imp        │               2 │
└─────────────────────┴────────────┴─────────────────┘

-- Let's add the new measurement `cost`
-- and the new dimension `browser`.

ALTER TABLE events
  ADD COLUMN browser String,
  ADD COLUMN cost Float64;

-- Column do not have to match in a materialized view and TO
-- (destination table), so the next alter does not break insertion.

ALTER TABLE events_by_day
    ADD COLUMN cost Float64,
    ADD COLUMN browser String after event_type,
    MODIFY ORDER BY (event_type, ts, browser);

INSERT INTO events
SELECT Date '2020-01-02' + interval number * 900 second,
       ['imp', 'click'][number%2+1],
       ['firefox', 'safary', 'chrome'][number%3+1],
       10/(number+1)%33
FROM numbers(100);

-- New columns `browser` and `cost` are empty because we did not change Materialized View yet.

SELECT ts, event_type, browser, sum(events_cnt) events_cnt, round(sum(cost),2) cost
FROM events_by_day
GROUP BY ts, event_type, browser
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─browser─┬─events_cnt─┬─cost─┐
│ 2020-01-01 00:00:00 │ click      │         │         48 │    0 │
│ 2020-01-01 00:00:00 │ imp        │         │         48 │    0 │
│ 2020-01-02 00:00:00 │ click      │         │         50 │    0 │
│ 2020-01-02 00:00:00 │ imp        │         │         50 │    0 │
│ 2020-01-03 00:00:00 │ click      │         │          2 │    0 │
│ 2020-01-03 00:00:00 │ imp        │         │          2 │    0 │
└─────────────────────┴────────────┴─────────┴────────────┴──────┘

ALTER TABLE mv MODIFY QUERY
  SELECT toStartOfDay(ts) ts, event_type, browser,
  count() events_cnt,
  sum(cost) cost
  FROM events
  GROUP BY ts, event_type, browser;

INSERT INTO events
SELECT Date '2020-01-03' + interval number * 900 second,
       ['imp', 'click'][number%2+1],
       ['firefox', 'safary', 'chrome'][number%3+1],
       10/(number+1)%33
FROM numbers(100);

SELECT ts, event_type, browser, sum(events_cnt) events_cnt, round(sum(cost),2) cost
FROM events_by_day
GROUP BY ts, event_type, browser
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─browser─┬─events_cnt─┬──cost─┐
│ 2020-01-01 00:00:00 │ click      │         │         48 │     0 │
│ 2020-01-01 00:00:00 │ imp        │         │         48 │     0 │
│ 2020-01-02 00:00:00 │ click      │         │         50 │     0 │
│ 2020-01-02 00:00:00 │ imp        │         │         50 │     0 │
│ 2020-01-03 00:00:00 │ click      │ firefox │         16 │  6.84 │
│ 2020-01-03 00:00:00 │ click      │         │          2 │     0 │
│ 2020-01-03 00:00:00 │ click      │ safary  │         16 │  9.82 │
│ 2020-01-03 00:00:00 │ click      │ chrome  │         16 │  5.63 │
│ 2020-01-03 00:00:00 │ imp        │         │          2 │     0 │
│ 2020-01-03 00:00:00 │ imp        │ firefox │         16 │ 15.14 │
│ 2020-01-03 00:00:00 │ imp        │ safary  │         16 │  6.14 │
│ 2020-01-03 00:00:00 │ imp        │ chrome  │         16 │  7.89 │
│ 2020-01-04 00:00:00 │ click      │ safary  │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ click      │ firefox │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ imp        │ firefox │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ imp        │ chrome  │          1 │   0.1 │
└─────────────────────┴────────────┴─────────┴────────────┴───────┘

-- !!! During `MODIFY ORDER BY` PRIMARY KEY was implicitly introduced.

SHOW CREATE TABLE events_by_day FORMAT TSVRaw

CREATE TABLE test.events_by_day
(
    `ts` DateTime,
    `event_type` String,
    `browser` String,
    `events_cnt` UInt64,
    `cost` Float64
)
ENGINE = SummingMergeTree
PRIMARY KEY (event_type, ts)
ORDER BY (event_type, ts, browser)

-- !!! The columns' definition is unchanged but it does not matter, we are not querying
-- MATERIALIZED VIEW, we are querying TO (storage) table.
-- SELECT section is updated.

SHOW CREATE TABLE mv FORMAT TSVRaw;

CREATE MATERIALIZED VIEW test.mv TO test.events_by_day
(
    `ts` DateTime,
    `event_type` String,
    `events_cnt` UInt64
) AS
SELECT
    toStartOfDay(ts) AS ts,
    event_type,
    browser,
    count() AS events_cnt,
    sum(cost) AS cost
FROM test.events
GROUP BY
    ts,
    event_type,
    browser
```

**TO 테이블이 없는 예시**

새로운 컬럼을 추가하지 않는 한 `SELECT` 섹션만 변경할 수 있어 활용 범위가 매우 제한적입니다.

```sql
CREATE TABLE src_table (`a` UInt32) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv (`a` UInt32) ENGINE = MergeTree ORDER BY a AS SELECT a FROM src_table;
INSERT INTO src_table (a) VALUES (1), (2);
SELECT * FROM mv;
```

```text
┌─a─┐
│ 1 │
│ 2 │
└───┘
```

```sql
ALTER TABLE mv MODIFY QUERY SELECT a * 2 as a FROM src_table;
INSERT INTO src_table (a) VALUES (3), (4);
SELECT * FROM mv;
```

```text
┌─a─┐
│ 6 │
│ 8 │
└───┘
┌─a─┐
│ 1 │
│ 2 │
└───┘
```

<div id="alter-table--modify-refresh-statement">
  ## ALTER TABLE ... MODIFY REFRESH SQL 문
</div>

`ALTER TABLE ... MODIFY REFRESH`는 [갱신 가능 구체화 뷰](../create/view.md#refreshable-materialized-view)의 갱신 관련 매개변수를 변경합니다. 여기에는 스케줄, 의존성, 랜덤화, 그리고 [갱신 설정](../create/view.md#refresh-settings)이 포함됩니다.

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

일정(`EVERY` 또는 `AFTER`)은 필수입니다. 이 구문은 *모든* 갱신 매개변수를 한 번에 대체합니다. `RANDOMIZE FOR`, `DEPENDS ON`, `SETTINGS` 중 지정하지 않은 절은 제거되거나 기본값으로 재설정됩니다. 갱신 설정만 변경하려면 현재 일정을 다시 지정하십시오.

```sql
-- Change the schedule.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change retry settings (schedule must be repeated).
ALTER TABLE rmv MODIFY REFRESH EVERY 1 HOUR
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Add or keep a dependency.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

제한 사항:

* materialized view에서는 `ALTER TABLE ... MODIFY SETTING`을 지원하지 않으며, 갱신 설정은 `MODIFY REFRESH`로만 변경할 수 있습니다.
* `APPEND`의 추가 또는 제거는 지원되지 않습니다.
* `all_replicas` 갱신 설정은 뷰를 생성한 이후에는 변경할 수 없습니다.

전체 갱신 설정 목록은 [Refresh Settings](../create/view.md#refresh-settings)에 설명되어 있습니다. 현재 적용된 설정을 포함한 갱신 상태는 [`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md)에서 확인할 수 있습니다.