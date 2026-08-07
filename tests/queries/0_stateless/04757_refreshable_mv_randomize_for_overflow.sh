#!/usr/bin/env bash
# Tags: memory-engine, atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RANDOMIZE FOR takes an unbounded UInt64, and the random offset derived from it used to be
# computed in double and then forced through two narrow integer domains and an unchecked
# time_point addition, so a large window was undefined behaviour (the server aborts under
# -fno-sanitize-recover=all).
#
# The oracle is sign-independent on purpose: the random offset is drawn uniformly over both
# signs at construction, and with a wide window a negative draw legitimately puts the next
# refresh in the past, so neither status nor next_refresh_time can be asserted. Each arm only
# has to be accepted, and the server has to still be answering afterwards.

# Arm A: the offset in milliseconds is representable, but widening it to the microseconds of
# system_clock::duration is not.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_a refresh every 1 year randomize for 10000000000000000 second
        (x Int64) engine Memory as select 1 as x;"

# Arm B: the offset itself is outside Int64, so even the conversion out of double was undefined.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_b refresh every 1 year randomize for 10000000000000000000 second
        (x Int64) engine Memory as select 1 as x;"

# Arm C: the offset is representable in both domains, but adding it to a next-refresh time near
# the end of the representable range overflowed. Only a positive draw overflows and the sign is
# random, so this arm is probabilistic by nature: pre-fix each view aborts with probability ~1/2
# (measured 3 of 6), so 30 views leave a ~1e-9 chance of missing the abort. The deterministic pin
# for this domain is in gtest_refresh_schedule.cpp, which drives `when` to both ends of the range
# with a fixed randomness.
for i in {1..30}; do
    $CLICKHOUSE_CLIENT -q "
        create materialized view rmv_c_$i refresh after 9200000000000 second randomize for 18000000000000 second
            (x Int64) engine Memory as select 1 as x;"
done

# Arms E and F: a calendar-unit window reaches the offset through the months term of
# CalendarTimeInterval::minSeconds instead of its seconds term, which every arm above uses.
# Deterministic pre-fix: escaping needs |randomness| <= 1015 (E) or <= 6044 (F) out of 1e9.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_e refresh every 1 year randomize for 1000000000000000000 year
        (x Int64) engine Memory as select 1 as x;"
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_f refresh every 1 year randomize for 1000000000000000000 month
        (x Int64) engine Memory as select 1 as x;"

# Arm D: an ordinary spread still works.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_d refresh every 1 hour randomize for 4 day 1 hour
        (x Int64) engine Memory as select 1 as x;"

# The views were created, and the server is still alive to say so. Scheduling runs in a
# background thread, so give it a moment to reach the offset computation for every view.
sleep 3
$CLICKHOUSE_CLIENT -q "select 'views', count() from system.view_refreshes where database = currentDatabase()"
$CLICKHOUSE_CLIENT -q "select 'alive', 1"

for i in {1..30}; do
    $CLICKHOUSE_CLIENT -q "drop table rmv_c_$i"
done
$CLICKHOUSE_CLIENT -q "drop table rmv_a; drop table rmv_b; drop table rmv_d; drop table rmv_e; drop table rmv_f"
