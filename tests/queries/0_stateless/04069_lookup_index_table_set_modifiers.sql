SET enable_analyzer = 1;
SET allow_experimental_lookup_index = 1;

DROP TABLE IF EXISTS table_lookup_dim_final SYNC;
DROP TABLE IF EXISTS table_lookup_fact_final SYNC;

CREATE TABLE table_lookup_dim_final
(
    id UInt64,
    ver UInt64,
    deleted UInt8,
    LOOKUP INDEX idx_set (id) TYPE table_set
)
ENGINE = ReplacingMergeTree(ver, deleted)
ORDER BY id;

CREATE TABLE table_lookup_fact_final
(
    id UInt64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO table_lookup_dim_final VALUES (1, 1, 0), (1, 2, 1), (2, 1, 0);
INSERT INTO table_lookup_fact_final VALUES (1), (2);

SELECT id
FROM table_lookup_fact_final
WHERE id IN (SELECT id FROM table_lookup_dim_final)
ORDER BY id;

SELECT '--';

SELECT id
FROM table_lookup_fact_final
WHERE id IN (SELECT id FROM table_lookup_dim_final FINAL)
ORDER BY id;

DROP TABLE table_lookup_dim_final SYNC;
DROP TABLE table_lookup_fact_final SYNC;
