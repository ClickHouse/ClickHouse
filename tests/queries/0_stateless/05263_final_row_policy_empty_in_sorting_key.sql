-- A FINAL read with a row policy that tests a sorting-key column against an empty list returns the
-- same result with and without removing unused columns from the query plan (issue #122299).

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c1 UUID, c2 LowCardinality(String), c3 Date, c6 Decimal(15, 2), c7 UUID, c8 UUID, c9 Nullable(UUID))
ENGINE = ReplacingMergeTree PARTITION BY (c2, toYYYYMM(c3)) PRIMARY KEY (c2, c7, c8, c3) ORDER BY (c2, c7, c8, c3, c1);

INSERT INTO t1 SELECT toUUID(concat('00000000-0000-0000-0002-', leftPad(toString(number), 12, '0'))), 'o1', toDate('2026-01-01') + (number % 200), number % 100, toUUID('00000000-0000-0000-0000-000000000001'), toUUID(concat('00000000-0000-0000-0001-', leftPad(toString(intDiv(number, 7) % 3), 12, '0'))), toUUID('00000000-0000-0000-0000-00000000000a') FROM numbers(1000);
INSERT INTO t1 SELECT toUUID(concat('00000000-0000-0000-0002-', leftPad(toString(number), 12, '0'))), 'o1', toDate('2026-01-01') + (number % 200), number % 50, toUUID('00000000-0000-0000-0000-000000000001'), toUUID(concat('00000000-0000-0000-0001-', leftPad(toString(intDiv(number, 7) % 3), 12, '0'))), toUUID('00000000-0000-0000-0000-00000000000b') FROM numbers(1000, 1000);

DROP ROW POLICY IF EXISTS p2 ON t1;
CREATE ROW POLICY p2 ON t1 AS PERMISSIVE FOR SELECT USING (toString(c7) IN CAST('[]', 'Array(String)')) OR (toString(c9) IN CAST('[''00000000-0000-0000-0000-00000000000a'',''00000000-0000-0000-0000-00000000000b'']', 'Array(String)')) TO ALL;

SELECT MIN(c3) AS a1, MAX(c3) AS a2, COUNT() AS a3, COUNTDistinct(c8) AS a4, SUM(c6) AS a5 FROM t1 FINAL WHERE (c3 >= '2026-01-01') AND (c3 < '2026-08-25') AND (c8 = toUUID('00000000-0000-0000-0001-000000000001')) SETTINGS query_plan_remove_unused_columns = 1;
SELECT MIN(c3) AS a1, MAX(c3) AS a2, COUNT() AS a3, COUNTDistinct(c8) AS a4, SUM(c6) AS a5 FROM t1 FINAL WHERE (c3 >= '2026-01-01') AND (c3 < '2026-08-25') AND (c8 = toUUID('00000000-0000-0000-0001-000000000001')) SETTINGS query_plan_remove_unused_columns = 0;

DROP ROW POLICY p2 ON t1;
DROP TABLE t1;
