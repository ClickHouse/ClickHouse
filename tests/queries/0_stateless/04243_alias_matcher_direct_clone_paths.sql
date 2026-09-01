SET enable_named_columns_in_function_tuple = 1;

DROP TABLE IF EXISTS alias_matcher_direct_clone_read_order;
DROP TABLE IF EXISTS alias_matcher_direct_clone_prewhere;
DROP TABLE IF EXISTS alias_matcher_direct_clone_merge;
DROP TABLE IF EXISTS alias_matcher_direct_clone_merge_src;

CREATE TABLE alias_matcher_direct_clone_read_order
(
    a UInt8,
    b String ALIAS toJSONString(tuple(COLUMNS('^a$')))
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO alias_matcher_direct_clone_read_order (a) VALUES (1), (2);
SELECT b FROM alias_matcher_direct_clone_read_order ORDER BY b
SETTINGS allow_experimental_analyzer = 0, optimize_read_in_order = 1, optimize_respect_aliases = 1;
SELECT b FROM alias_matcher_direct_clone_read_order ORDER BY b
SETTINGS allow_experimental_analyzer = 1, optimize_read_in_order = 1, optimize_respect_aliases = 1;

CREATE TABLE alias_matcher_direct_clone_prewhere
(
    a UInt8,
    b String ALIAS toJSONString(tuple(COLUMNS('^a$')))
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO alias_matcher_direct_clone_prewhere (a) VALUES (1), (2);
SELECT b FROM alias_matcher_direct_clone_prewhere PREWHERE b != '' ORDER BY a
SETTINGS allow_experimental_analyzer = 0, optimize_respect_aliases = 1;

CREATE TABLE alias_matcher_direct_clone_merge_src
(
    a UInt8,
    b String ALIAS toJSONString(tuple(COLUMNS('^a$')))
)
ENGINE = MergeTree
ORDER BY a;

CREATE TABLE alias_matcher_direct_clone_merge
(
    a UInt8,
    b String
)
ENGINE = Merge(currentDatabase(), '^alias_matcher_direct_clone_merge_src$');

INSERT INTO alias_matcher_direct_clone_merge_src (a) VALUES (1), (2);
SELECT b FROM alias_matcher_direct_clone_merge ORDER BY a
SETTINGS allow_experimental_analyzer = 0;

DROP TABLE alias_matcher_direct_clone_merge;
DROP TABLE alias_matcher_direct_clone_merge_src;
DROP TABLE alias_matcher_direct_clone_prewhere;
DROP TABLE alias_matcher_direct_clone_read_order;
