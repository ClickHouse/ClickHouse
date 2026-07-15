-- Tags: no-async-insert
-- Verify the default of `input_format_column_name_matching_mode` and its
-- compatibility rollback. See PR #104320.

-- 1. Default behavior (no SET).
--    1a. Value-level: system.settings reports the new default `auto`.
--    1b. Behavior-level: under `auto`, the input field "ID" falls back to the
--        table column `id` via case-insensitive match.
SELECT value FROM system.settings WHERE name = 'input_format_column_name_matching_mode';

DROP TABLE IF EXISTS test_default;
CREATE TABLE test_default (id Int, name String) ENGINE = Memory;

INSERT INTO test_default FORMAT JSONEachRow {"ID": 1, "name": "a"};

SELECT id, name FROM test_default;

DROP TABLE test_default;

-- 2. Compatibility rollback to a pre-26.5 version must restore `match_case`.
--    2a. Value-level: system.settings reports the restored old default.
--    2b. Behavior-level: "ID" no longer matches `id` and is treated as an
--        unknown field (skipped under the default
--        `input_format_skip_unknown_fields = true`); `id` defaults to 0.
SET compatibility = '26.4';

SELECT value FROM system.settings WHERE name = 'input_format_column_name_matching_mode';

DROP TABLE IF EXISTS test_compat;
CREATE TABLE test_compat (id Int, name String) ENGINE = Memory;

INSERT INTO test_compat FORMAT JSONEachRow {"ID": 1, "name": "a"};

SELECT id, name FROM test_compat;

DROP TABLE test_compat;
