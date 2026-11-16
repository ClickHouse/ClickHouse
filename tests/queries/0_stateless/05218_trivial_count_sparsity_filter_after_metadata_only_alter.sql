-- A metadata-only widening to Nullable does not rewrite the part, so the recorded number of
-- default values still counts zeros (or empty strings), not NULLs.

CREATE TABLE t (k UInt64, v UInt64, s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO t SELECT number, number % 10, if(number % 10 = 0, '', 'x') FROM numbers(100);

ALTER TABLE t MODIFY COLUMN v Nullable(UInt64) SETTINGS mutations_sync = 2;
ALTER TABLE t MODIFY COLUMN s Nullable(String) SETTINGS mutations_sync = 2;

SELECT count() FROM t WHERE v IS NULL;
SELECT count() FROM t WHERE v IS NOT NULL;
SELECT count() FROM t WHERE s IS NULL;
SELECT count() FROM t WHERE s IS NOT NULL;

-- The stats are usable again once the part is rewritten with the new type.
OPTIMIZE TABLE t FINAL;
SELECT count() FROM t WHERE v IS NULL;
SELECT count() FROM t WHERE v IS NOT NULL;

DROP TABLE t;
