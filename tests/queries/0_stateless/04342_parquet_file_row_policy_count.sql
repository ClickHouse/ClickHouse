-- Tags: no-fasttest
-- no-fasttest: `Parquet` format is not supported in fasttest.

DROP ROW POLICY IF EXISTS 04342_row_policy ON 04342_file_row_policy_count;
DROP TABLE IF EXISTS 04342_file_row_policy_count;

CREATE TABLE 04342_file_row_policy_count
(
    id UInt64,
    visible UInt8
)
ENGINE = File(Parquet)
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO 04342_file_row_policy_count VALUES (1, 1), (2, 0), (3, 1);

CREATE ROW POLICY 04342_row_policy
ON 04342_file_row_policy_count
FOR SELECT
USING visible = 1
TO ALL;

SELECT count()
FROM 04342_file_row_policy_count
SETTINGS optimize_count_from_files = 1;

SELECT count()
FROM 04342_file_row_policy_count
WHERE id > 0
SETTINGS optimize_count_from_files = 1;

DROP ROW POLICY 04342_row_policy ON 04342_file_row_policy_count;
DROP TABLE 04342_file_row_policy_count;
