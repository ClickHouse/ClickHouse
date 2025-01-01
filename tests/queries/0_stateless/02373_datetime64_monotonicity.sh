#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for tz in Asia/Tehran UTC Canada/Atlantic Europe/Berlin
do
    echo "$tz"
    TZ=$tz $CLICKHOUSE_LOCAL --multiline "
      drop table if exists dt64_monotonicity_test;
      drop table if exists dt64_monotonicity_test_string;
      CREATE TABLE dt64_monotonicity_test (date_time DateTime64(3, 'Europe/Berlin'), id String) ENGINE = MergeTree PARTITION BY toDate(date_time, 'Europe/Berlin') ORDER BY date_time;
      insert into dt64_monotonicity_test select toDateTime64('2020-01-01 00:00:00.000', 3) + number, '' from numbers(10);

      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime(date_time) >= toDateTime('2020-01-01 00:00:00') SETTINGS force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) >= '2020-01-01 00:00:01.111' SETTINGS force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) >= '2020-01-01 00:00:00.000' SETTINGS force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) >= toDateTime64('2020-01-01 00:00:00.000001', 3) SETTINGS force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) >= toDateTime64('2020-01-01 00:00:00.000001', 3, 'Europe/Berlin') SETTINGS force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) >= toDateTime64('2020-01-01 00:00:00.000001', 6) SETTINGS force_index_by_date = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) >= toDateTime64('2020-01-01 00:00:00.000001', 6, 'Europe/Berlin') SETTINGS force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) >= toDateTime64('2020-01-01 00:00:00.000001', 6) SETTINGS force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) <= toDateTime64('2020-01-01 00:00:00.000001', 3, 'Europe/Berlin') settings force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) <= toDateTime64('2020-01-01 00:00:00.000001', 3) settings force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) = toDateTime64('2020-01-01 00:00:00.000000', 6);
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 3) = toDateTime64('2020-01-01 00:00:00.000000', 6, 'Europe/Berlin');
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 6) = toDateTime64('2020-01-01 00:00:00.000000', 6) settings force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 6) = toDateTime64('2020-01-01 00:00:00.000001', 6, 'Europe/Berlin') settings force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 6) > toDateTime64('2020-01-01 00:00:00.000001', 6, 'Europe/Berlin') settings force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 6) >= toDateTime64('2020-01-01 00:00:00.000001', 6, 'Europe/Berlin') settings force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 6) >= toDateTime64('2020-01-01 00:00:00.000001', 6) settings force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 0) >= toDateTime64('2020-01-01 00:00:00.000001', 0, 'Europe/Berlin') settings force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 0) >= toDateTime64('2020-01-01 00:00:00.000001', 0) settings force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 0) >= '2020-01-01 00:00:00' settings force_index_by_date = 1, force_primary_key = 1;
      SELECT count() FROM dt64_monotonicity_test WHERE toDateTime64(date_time, 0) >= '2020-01-01 00:00:01.1' settings force_index_by_date = 1, force_primary_key = 1;

      create table dt64_monotonicity_test_string(date_time String, x String) Engine=MergeTree order by date_time;
      insert into dt64_monotonicity_test_string select '2020-01-01 00:00:00.000000001', '' from numbers(1);
      insert into dt64_monotonicity_test_string select '2020-01-01 00:00:00.000', '' from numbers(10);

      SELECT count() FROM dt64_monotonicity_test_string WHERE toDateTime64(date_time,9) = '2020-01-01 00:00:00.000000000';
      SELECT count() FROM dt64_monotonicity_test_string WHERE toDateTime64(date_time,3) = '2020-01-01 00:00:00.000000001';
      SELECT count() FROM dt64_monotonicity_test_string WHERE toDateTime64(date_time,9) = '2020-01-01 00:00:00';

      drop table dt64_monotonicity_test;
      drop table dt64_monotonicity_test_string;
    "
    echo ""
done
