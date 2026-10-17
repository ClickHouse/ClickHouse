#!/usr/bin/expect -f
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest

log_user 0
set timeout 60
match_max 10000000

# Run query that filters all rows in PREWHERE
spawn clickhouse-local --progress -m -n --query "CREATE TABLE test_progress(n UInt64) ENGINE=MergeTree ORDER BY tuple() SETTINGS index_granularity=10 AS SELECT number FROM numbers(10000); SELECT count() FROM test_progress PREWHERE sleepEachRow(0.01) OR n > 1000000 SETTINGS max_block_size=10;"

# Expect that progress is updated
expect {
    "10.00 rows," { exit 0 }
    "20.00 rows," { exit 0 }
    "30.00 rows," { exit 0 }
    "40.00 rows," { exit 0 }
    "50.00 rows," { exit 0 }
    "60.00 rows," { exit 0 }
    "70.00 rows," { exit 0 }
    "80.00 rows," { exit 0 }
    "90.00 rows," { exit 0 }
    timeout { exit 1 }
}
