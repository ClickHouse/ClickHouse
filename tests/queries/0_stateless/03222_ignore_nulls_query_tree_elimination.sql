#!/usr/bin/env -S ${HOME}/clickhouse-client --queries-file

DROP TABLE IF EXISTS with_fill_date__fuzz_0;

CREATE TABLE with_fill_date__fuzz_0
(
    `d` Date,
    `d32` Nullable(Int32),
    `d33` Int32
)
ENGINE = Memory;


INSERT INTO with_fill_date__fuzz_0 VALUES  (toDate('2020-03-03'), 1, 3), (toDate('2020-03-03'), NULL, 3), (toDate('2020-02-05'), 1, 1);


SELECT count()
FROM with_fill_date__fuzz_0
ORDER BY
    count(),
    count() IGNORE NULLS,
    max(d)
WITH FILL STEP toIntervalDay(10)
;


SELECT count()
FROM with_fill_date__fuzz_0
ORDER BY
    any(d32) RESPECT NULLS,
    any_respect_nulls(d32),
    max(d)
WITH FILL STEP toIntervalDay(10)
;


SELECT count()
FROM with_fill_date__fuzz_0
ORDER BY
    any(d32),
    any(d32) IGNORE NULLS,
    any(d32) RESPECT NULLS,
    any_respect_nulls(d32) IGNORE NULLS,
    any_respect_nulls(d32),
    sum(d33),
    sum(d33) IGNORE NULLS,
    max(d)
WITH FILL STEP toIntervalDay(10)
;


