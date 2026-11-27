-- Tags: no-parallel-replicas
-- no-parallel-replicas - optimization not supported yet

DROP TABLE IF EXISTS tab1;

CREATE TABLE tab1
(
    id UInt32,
    v UInt32,
    a UInt32,
    INDEX vindex v TYPE minmax
)
Engine=MergeTree
ORDER BY id;

INSERT INTO tab1 SELECT number, number, number FROM numbers(10000);

-- optimization is not applicable for below examples - all should return 0 rows
SELECT trimLeft(explain) AS explain FROM (EXPLAIN indexes=1 SELECT * FROM tab1 ORDER BY rand() LIMIT 5 SETTINGS use_skip_indexes_for_top_k=1) WHERE explain LIKE '%TopK%';
SELECT trimLeft(explain) AS explain FROM (EXPLAIN indexes=1 SELECT * FROM tab1 ORDER BY mortonEncode(v, v) LIMIT 5 SETTINGS use_skip_indexes_for_top_k=1) WHERE explain LIKE '%TopK%';
SELECT trimLeft(explain) AS explain FROM (EXPLAIN indexes=1 SELECT * FROM tab1 ORDER BY a LIMIT 5 SETTINGS use_skip_indexes_for_top_k=1) WHERE explain LIKE '%TopK%';

-- optimization not applicable if LIMIT 0
SELECT trimLeft(explain) AS explain FROM (EXPLAIN indexes=1 SELECT * FROM tab1 ORDER BY v LIMIT 0 SETTINGS use_skip_indexes_for_top_k=1) WHERE explain LIKE '%TopK%';

-- only 3rd query will return rows
SELECT trimLeft(explain) AS explain FROM (EXPLAIN actions=1,header=1 SELECT * FROM tab1 ORDER BY rand() LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1) WHERE explain LIKE '%topKFilter';
SELECT trimLeft(explain) AS explain FROM (EXPLAIN actions=1,header=1 SELECT * FROM tab1 ORDER BY mortonEncode(v, v) LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1) WHERE explain LIKE '%topKFilter%';
SELECT trimLeft(explain) AS explain FROM (EXPLAIN actions=1,header=1 SELECT * FROM tab1 ORDER BY a LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1) WHERE explain LIKE '%topKFilter%';

DROP TABLE tab1;
