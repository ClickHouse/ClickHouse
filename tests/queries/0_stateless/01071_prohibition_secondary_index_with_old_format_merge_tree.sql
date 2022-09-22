-- Tags: no-parallel

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE old_syntax_01071_test (date Date, id UInt8) ENGINE = MergeTree(date, id, 8192);
ALTER TABLE old_syntax_01071_test ADD INDEX  id_minmax id TYPE minmax GRANULARITY 1; -- { serverError 36 }
CREATE TABLE new_syntax_01071_test (date Date, id UInt8) ENGINE = MergeTree() ORDER BY id;
ALTER TABLE new_syntax_01071_test ADD INDEX  id_minmax id TYPE minmax GRANULARITY 1;
DETACH TABLE new_syntax_01071_test;
ATTACH TABLE new_syntax_01071_test;
DROP TABLE IF EXISTS old_syntax_01071_test;
DROP TABLE IF EXISTS new_syntax_01071_test;
