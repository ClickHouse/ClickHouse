SET enable_analyzer = 1;

CREATE TABLE mister_table
(
    c0 Nullable(Int),
    CONSTRAINT c1 check c0.null
)
ENGINE = Memory();

CREATE TABLE mister_table_2 (c0 Nullable(Int)) ENGINE = MergeTree() ORDER BY tuple();
CREATE MATERIALIZED VIEW mister_view ENGINE = MergeTree() ORDER BY tuple() AS (SELECT mister_table_2.c0.null AS c0 FROM mister_table_2);
INSERT INTO mister_table_2 VALUES (1);
