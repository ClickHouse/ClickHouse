#!/usr/bin/env bash
# Regression test for assert_cast exception when ALTER TABLE MODIFY COLUMN changes
# a column with STATISTICS from Int16 to Time. The statistics data_type was not
# updated, causing assert_cast failure during INSERT when building statistics.
# https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=96985&sha=4578478336752478268ac55674d83f21af468856&name_0=PR&name_1=BuzzHouse%20%28arm_asan%29

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --multiquery "
SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

CREATE TABLE t_stats_time_mismatch
(
    c0 Int8,
    c1 Int16 STATISTICS(MinMax, Uniq),
    c5 Date
) ENGINE = MergeTree()
ORDER BY tuple();

ALTER TABLE t_stats_time_mismatch MODIFY COLUMN c1 Time;

INSERT INTO TABLE t_stats_time_mismatch (c1, c0, c5)
SELECT CAST(number AS Time), 51, CAST(number AS Date) FROM numbers(100);

SELECT count() FROM t_stats_time_mismatch;
"
