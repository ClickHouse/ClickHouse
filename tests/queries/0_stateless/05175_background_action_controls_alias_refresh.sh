#!/usr/bin/env bash
# Tags: atomic-database, memory-engine

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_alias_table_engine=1 --multiquery -q "
    CREATE TABLE src (value UInt64) ENGINE = Memory;
    INSERT INTO src VALUES (0);
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR (value UInt64) ENGINE = Memory EMPTY AS SELECT value FROM src;
    CREATE TABLE alias_a ENGINE = Alias(mv);
    CREATE TABLE alias_b ENGINE = Alias(mv);
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;"

value=0
check_resume()
{
    local starter=$1 label=$2
    local next_value=$((value + 1))
    # A stopped or paused view must defer the refresh. `START` through any name must
    # release it, and repeated `START` must not interfere with subsequent controls.
    $CLICKHOUSE_CLIENT --multiquery -q "
        TRUNCATE TABLE src;
        INSERT INTO src VALUES ($next_value);
        SYSTEM REFRESH VIEW mv;
        SYSTEM WAIT VIEW mv;
        SELECT '$label deferred', groupArray(value) = [$value] FROM mv;
        SYSTEM START VIEW $starter;
        SYSTEM START VIEW $starter;
        SYSTEM REFRESH VIEW mv;
        SYSTEM WAIT VIEW mv;
        SELECT '$label resumed', groupArray(value) = [$next_value] FROM mv;"
    value=$next_value
}

# Cover the target, the same alias, and a different alias for both controls.
for action in STOP PAUSE; do
    for stopper in mv alias_a alias_b; do
        for starter in mv alias_a alias_b; do
            $CLICKHOUSE_CLIENT --multiquery -q "SYSTEM $action VIEW $stopper; SYSTEM $action VIEW $stopper;"
            check_resume "$starter" "$action $stopper -> $starter"
        done
    done
done

# Both controls may be registered together through several names, in either order.
for actions in 'STOP PAUSE' 'PAUSE STOP'; do
    for starter in mv alias_a alias_b; do
        for action in $actions; do
            for stopper in mv alias_a alias_b; do
                $CLICKHOUSE_CLIENT --multiquery -q "SYSTEM $action VIEW $stopper; SYSTEM $action VIEW $stopper;"
            done
        done
        check_resume "$starter" "$actions all -> $starter"
    done
done

$CLICKHOUSE_CLIENT --multiquery -q "DROP TABLE alias_b; DROP TABLE alias_a; DROP TABLE mv; DROP TABLE src;"
