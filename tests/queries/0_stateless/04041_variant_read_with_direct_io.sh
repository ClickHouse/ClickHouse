#!/usr/bin/env bash
# Tags: long

# Regression test for incorrect seek in AsynchronousReadBufferFromFileDescriptor
# with O_DIRECT (min_bytes_to_use_direct_io=1). The bug was that getPosition()
# and seek NOOP/in-buffer checks did not account for bytes_to_ignore set by
# O_DIRECT alignment, causing corrupted reads of Variant subcolumns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_suspicious_variant_types=1 --max_threads 2 --min_bytes_to_use_direct_io 1"

$CH_CLIENT -q "drop table if exists test_variant_direct_io;"

$CH_CLIENT -q "create table test_variant_direct_io (id UInt64, v Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, index_granularity_bytes=10485760, index_granularity=8192;"

$CH_CLIENT -mq "insert into test_variant_direct_io select number, NULL from numbers(100000);
insert into test_variant_direct_io select number + 100000, number from numbers(100000);
insert into test_variant_direct_io select number + 200000, ('str_' || toString(number))::Variant(String) from numbers(100000);
insert into test_variant_direct_io select number + 300000, ('lc_str_' || toString(number))::LowCardinality(String) from numbers(100000);
insert into test_variant_direct_io select number + 400000, tuple(number, number + 1)::Tuple(a UInt32, b UInt32) from numbers(100000);
insert into test_variant_direct_io select number + 500000, range(number % 20 + 1)::Array(UInt64) from numbers(100000);"

$CH_CLIENT -q "optimize table test_variant_direct_io final settings mutations_sync=1;"

# Without the fix, reading v.String here would fail with:
# "Size of deserialized variant column less than the limit"
$CH_CLIENT -q "select v.String from test_variant_direct_io format Null;"

# Also check that subcolumn reads return the correct count
$CH_CLIENT -q "select count() from test_variant_direct_io where v is not null;"
$CH_CLIENT -q "select count() from test_variant_direct_io where v.String is not null;"
$CH_CLIENT -q "select count() from test_variant_direct_io where v.UInt64 is not null;"

$CH_CLIENT -q "drop table test_variant_direct_io;"
