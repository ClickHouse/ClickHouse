-- https://github.com/ClickHouse/ClickHouse/issues/56815
SELECT transform(arrayJoin([NULL, NULL]), [NULL, NULL], [NULL]) GROUP BY GROUPING SETS (('0.1'), ('-0.2147483647'));
