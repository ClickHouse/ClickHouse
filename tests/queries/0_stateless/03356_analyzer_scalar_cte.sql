-- issue https://github.com/ClickHouse/ClickHouse/issues/53752 https://github.com/ClickHouse/ClickHouse/issues/53753
WITH (SELECT throwIf(sleepEachRow(2) = 0)) AS res SELECT * FROM system.one;
