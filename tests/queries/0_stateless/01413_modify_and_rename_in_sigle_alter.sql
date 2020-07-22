DROP TABLE IF EXISTS test_modify_and_rename;

CREATE TABLE test_modify_and_rename(modify_and_rename_column Nullable(String), rename_and_modify_column Nullable(String)) ENGINE = MergeTree() PARTITION BY tuple() ORDER BY tuple();

DESC test_modify_and_rename;

ALTER TABLE test_modify_and_rename MODIFY COLUMN modify_and_rename_column String, RENAME COLUMN modify_and_rename_column TO modify_and_rename_column_new;

DESC test_modify_and_rename;

ALTER TABLE test_modify_and_rename RENAME COLUMN rename_and_modify_column TO rename_and_modify_column_new, MODIFY COLUMN rename_and_modify_column String;-- { serverError 10 }

ALTER TABLE test_modify_and_rename RENAME COLUMN rename_and_modify_column TO rename_and_modify_column_new, MODIFY COLUMN rename_and_modify_column_new String;

DESC test_modify_and_rename;

