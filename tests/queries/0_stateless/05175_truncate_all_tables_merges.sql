-- Tags: no-replicated-database
-- `TRUNCATE ALL TABLES` is not supported for `Replicated` databases.

CREATE TABLE truncate_merges (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO truncate_merges VALUES (100);

TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE:Identifier};
SELECT count() FROM truncate_merges;

INSERT INTO truncate_merges VALUES (7);
INSERT INTO truncate_merges VALUES (16);
OPTIMIZE TABLE truncate_merges FINAL;

SELECT count(), sum(id) FROM truncate_merges;
SELECT count(), sum(rows) FROM system.parts
WHERE database = currentDatabase() AND table = 'truncate_merges' AND active;

DROP TABLE truncate_merges;
