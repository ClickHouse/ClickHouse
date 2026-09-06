-- The settings `lookup_throttler_max_requests_per_second` and `lookup_max_rows_per_query` only control the
-- dictionary-source lookup path. The `YTsaurus` table engine and the `ytsaurus` table function do full-table
-- reads and never issue lookups, so these settings must be rejected there instead of being silently ignored.

SET allow_experimental_ytsaurus_table_engine = 1;
SET allow_experimental_ytsaurus_table_function = 1;

DROP TABLE IF EXISTS t_yt_ok;

-- Lookup-only settings are rejected on the table engine.
CREATE TABLE t_yt_lookup_1 (x UInt64) ENGINE = YTsaurus('http://localhost:8000', '//tmp/t', 'token')
SETTINGS lookup_throttler_max_requests_per_second = 100; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_yt_lookup_2 (x UInt64) ENGINE = YTsaurus('http://localhost:8000', '//tmp/t', 'token')
SETTINGS lookup_max_rows_per_query = 10; -- { serverError BAD_ARGUMENTS }

-- A genuine table-engine setting is still accepted (CREATE does not connect to YTsaurus).
CREATE TABLE t_yt_ok (x UInt64) ENGINE = YTsaurus('http://localhost:8000', '//tmp/t', 'token')
SETTINGS max_streams = 8;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_yt_ok';
DROP TABLE t_yt_ok;

-- Lookup-only settings are rejected on the table function as well (before any connection is attempted).
SELECT * FROM ytsaurus('http://localhost:8000', '//tmp/t', 'token', 'x UInt64',
    SETTINGS lookup_throttler_max_requests_per_second = 100); -- { serverError BAD_ARGUMENTS }

SELECT * FROM ytsaurus('http://localhost:8000', '//tmp/t', 'token', 'x UInt64',
    SETTINGS lookup_max_rows_per_query = 10); -- { serverError BAD_ARGUMENTS }
