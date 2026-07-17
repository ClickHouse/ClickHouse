-- Redundant parentheses in key clauses and other definition expressions are stripped when the
-- AST is read into the storage descriptions, so the stored and compared metadata is canonical
-- and tables defined with and without the parentheses are interchangeable. The query text
-- itself is kept as written (`SHOW CREATE` preserves the parentheses).

DROP TABLE IF EXISTS t_src_04604;
DROP TABLE IF EXISTS t_dst_04604;

CREATE TABLE t_src_04604 (a UInt32, b UInt32, i UInt32 DEFAULT (a + 1), INDEX ix (b) TYPE minmax, CONSTRAINT cc CHECK (a > 0)) ENGINE = MergeTree PARTITION BY (a) PRIMARY KEY (a) ORDER BY (a, b) SAMPLE BY (a);
CREATE TABLE t_dst_04604 (a UInt32, b UInt32, i UInt32 DEFAULT a + 1, INDEX ix b TYPE minmax, CONSTRAINT cc CHECK a > 0) ENGINE = MergeTree PARTITION BY a PRIMARY KEY a ORDER BY (a, b) SAMPLE BY a;

INSERT INTO t_src_04604 (a, b) VALUES (1, 1), (1, 2), (2, 1);

-- The keys compare as equal, so moving data between the tables works.
ALTER TABLE t_dst_04604 ATTACH PARTITION 1 FROM t_src_04604;
SELECT a, b, i FROM t_dst_04604 ORDER BY a, b;

-- The metadata visible in system tables is canonical for both tables.
SELECT partition_key, sorting_key, primary_key, sampling_key FROM system.tables WHERE database = currentDatabase() AND name LIKE 't\\_%\\_04604' ORDER BY name;
SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND name = 'i' AND table LIKE 't\\_%\\_04604' ORDER BY table;

-- The query text is kept as written.
SELECT create_table_query LIKE '%PARTITION BY (a)%' FROM system.tables WHERE database = currentDatabase() AND name = 't_src_04604';

DROP TABLE t_src_04604;
DROP TABLE t_dst_04604;
