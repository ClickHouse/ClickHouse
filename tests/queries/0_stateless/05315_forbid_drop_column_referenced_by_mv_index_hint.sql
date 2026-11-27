-- A source column used only inside `indexHint` of a materialized view query cannot be dropped.

DROP TABLE IF EXISTS mv_index_hint;
DROP TABLE IF EXISTS src_index_hint;

CREATE TABLE src_index_hint (n1 Int8, n2 Int8, n3 Int8) ENGINE = MergeTree ORDER BY n1;
CREATE MATERIALIZED VIEW mv_index_hint ENGINE = Memory AS SELECT n1 FROM src_index_hint WHERE indexHint(n2 > 0);

ALTER TABLE src_index_hint DROP COLUMN n2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE src_index_hint DROP COLUMN n3;

INSERT INTO src_index_hint VALUES (1, 1);
SELECT n1 FROM mv_index_hint ORDER BY n1;

DROP TABLE mv_index_hint;
DROP TABLE src_index_hint;
