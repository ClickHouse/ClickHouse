-- An empty `cypress_path` used to reach `YTsaurusSourceFactory::createPipe`, which reported it as a `LOGICAL_ERROR`
-- while reading. It is a user error, so it is rejected as `BAD_ARGUMENTS` while the configuration is parsed.

SET allow_experimental_ytsaurus_table_engine = 1;
SET allow_experimental_ytsaurus_table_function = 1;

SELECT * FROM ytsaurus('http://localhost:8000', '', 'token', 'x UInt64'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_yt_empty_path (x UInt64) ENGINE = YTsaurus('http://localhost:8000', '', 'token'); -- { serverError BAD_ARGUMENTS }

-- A non-empty path is still accepted (`CREATE` does not connect to YTsaurus).
CREATE TABLE t_yt_ok_path (x UInt64) ENGINE = YTsaurus('http://localhost:8000', '//tmp/t', 'token');
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_yt_ok_path';
DROP TABLE t_yt_ok_path;
