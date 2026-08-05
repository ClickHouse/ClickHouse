-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/85313
-- SELECT * REPLACE should affect the WHERE clause when the analyzer is enabled.

DROP TABLE IF EXISTS t_replace_where;

CREATE TABLE t_replace_where
(
    app_id UInt64,
    network String,
    name_asset_group_id UInt64,
    date Date
) ENGINE = Memory;

INSERT INTO t_replace_where VALUES (0, 'snapchat', 123, '2023-08-04'), (42, 'snapchat', 0, '2023-08-04');

-- With the analyzer, the REPLACE should apply to WHERE as well,
-- so both rows should be returned (app_id=0 becomes 1, which is != 0).
SELECT * REPLACE (if(app_id = 0, 1, app_id) AS app_id)
FROM t_replace_where
WHERE app_id != 0
ORDER BY app_id;

DROP TABLE t_replace_where;
