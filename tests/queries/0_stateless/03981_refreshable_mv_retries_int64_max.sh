#!/usr/bin/env bash
# Tags: memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A refresh that always fails, with the maximum number of retries, so the task
# keeps retrying and reaches attempt_number > 1 (the formatting branch in
# RefreshTask::executeRefresh). throwIf reads a column so it throws at refresh
# execution, not at view creation.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv refresh after 1 year settings refresh_retries = 9223372036854775807
        (x Int64) engine Memory as select throwIf(number = 0) as x from numbers(1);"

# Wait until the second attempt has started (retry counter advanced past the
# first attempt). This is exactly when executeRefresh formats the
# '(attempt N/total)' comment.
for _ in {1..200}; do
    retry=$($CLICKHOUSE_CLIENT -q "select retry from system.view_refreshes where view = 'rmv' and database = currentDatabase()" | xargs)
    if [ "$retry" -ge 1 ] 2>/dev/null; then
        break
    fi
    sleep 0.2
done

# Server is alive and the second attempt ran without UB.
$CLICKHOUSE_CLIENT -q "select 'ok', retry >= 1 from system.view_refreshes where view = 'rmv' and database = currentDatabase()"

$CLICKHOUSE_CLIENT -q "drop table rmv"
