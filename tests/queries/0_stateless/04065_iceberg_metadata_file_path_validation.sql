-- Tags: no-fasttest
-- Verify that invalid iceberg_metadata_file_path values produce proper errors
-- instead of crashing the server (std::stoi, std::length_error, etc.).
-- See: https://github.com/ClickHouse/ClickHouse/issues/100473

-- Filename starting with dash: find_first_of('-') returns 0, empty version_str passes
-- vacuous all_of check, then stoi("") throws std::invalid_argument.
SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = '-21474836.47'); -- { serverError BAD_ARGUMENTS }

-- Filename with no dash: find_first_of('-') returns npos, string constructor with npos
-- offset causes std::length_error.
SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = '.*'); -- { serverError BAD_ARGUMENTS }

-- Another dash-prefixed variant from AST fuzzer.
SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = '-838:59:59.999999999'); -- { serverError BAD_ARGUMENTS }

-- Non-numeric version after v prefix.
SELECT * FROM icebergS3('http://localhost:11111/test/est', 'clickhouse', 'clickhouse', SETTINGS iceberg_metadata_file_path = 'vABC.metadata.json'); -- { serverError BAD_ARGUMENTS }
