SET enable_analyzer = 1;
SET analyzer_inline_views = 1;

DROP TABLE IF EXISTS view_types_src;

CREATE TABLE view_types_src (a UInt16, b String, c Float32) ENGINE = MergeTree() ORDER BY ();
INSERT INTO view_types_src VALUES (1, 'hello', 3.14), (2, 'world', 2.72);

-- View with explicitly wider column types than the source table.
DROP VIEW IF EXISTS view_wider_types;
CREATE VIEW view_wider_types (a UInt64, b String, c Float64) AS SELECT a, b, c FROM view_types_src;

-- Column types must match the VIEW declaration, not the underlying table.
SELECT toTypeName(a), toTypeName(b), toTypeName(c) FROM view_wider_types;

-- Values must be correct.
SELECT a, b, c FROM view_wider_types ORDER BY a;

-- The query tree must contain a _CAST wrapper for mismatched types.
EXPLAIN QUERY TREE
SELECT a FROM view_wider_types;

-- When types already match, no wrapper should be added.
DROP VIEW IF EXISTS view_same_types;
CREATE VIEW view_same_types AS SELECT a, b, c FROM view_types_src;

-- No _CAST in the tree when types match.
EXPLAIN QUERY TREE
SELECT a FROM view_same_types;

DROP VIEW view_wider_types;
DROP VIEW view_same_types;
DROP TABLE view_types_src;
