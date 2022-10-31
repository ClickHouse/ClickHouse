-- If underlying table doesn't support FINAL, select from merge engine should throw exception
CREATE TABLE table_1(`id` Int32,`column1` Nullable(String),  `column2` Nullable(String)) ENGINE = MergeTree ORDER BY (id, assumeNotNull(column2));
CREATE TABLE merge_table as table_1 ENGINE = Merge(currentDatabase(), '^table_\\d+$');
SELECT column1 FROM db.merge_table FINAL PREWHERE column2 = 'def'; -- {serverError 181}

-- PREWHERE with FINAL https://github.com/ClickHouse/ClickHouse/issues/35307
CREATE TABLE replace_table_1(`id` Int32,`column1` Nullable(String),  `column2` Nullable(String)) ENGINE = ReplacingMergeTree ORDER BY (id, assumeNotNull(column2));
CREATE TABLE replace_table_2(`id` Int32,`column1` Nullable(String),  `column2` Nullable(String)) ENGINE = ReplacingMergeTree ORDER BY (id, assumeNotNull(column2));
INSERT INTO replace_table_1 (id, column1, column2)  VALUES  (1, 'abc', 'def');
INSERT INTO replace_table_2 (id, column1, column2)  VALUES (2, 'uvw','xyz');
CREATE TABLE replace_merge_table as replace_table_1 ENGINE = Merge(currentDatabase(), '^replace_table_\\d+$');
SELECT column1 FROM replace_merge_table FINAL PREWHERE column2 = 'def';
