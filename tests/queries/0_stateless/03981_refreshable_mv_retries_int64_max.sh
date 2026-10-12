#!/usr/bin/env bash
# Tags: memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A refresh that always fails, with the maximum number of retries, so the task
# keeps retrying and reaches attempt_number > 1 (the formatting branch in
# RefreshTask::executeRefresh). throwIf reads a column so it throws at refresh
# execution, not at view creation.
# APPEND keeps the view creatable on a Replicated database: a non-APPEND
# refreshable view over a non-replicated inner table is refused there. The retry
# machinery does not read the append flag, so the formatting branch is reached
# either way.
# all_replicas = 1 keeps refresh uncoordinated, so the replica this client is
# connected to runs its own refresh. Without it, an APPEND view on a Replicated
# database refreshes on one arbitrarily chosen same-shard replica: the retry
# counter would still be visible here because it lives in shared Keeper state,
# but query_log is replica-local, so the formatted comment below could be written
# on another replica and never observed by this client.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv refresh after 1 year settings refresh_retries = 9223372036854775807, all_replicas = 1 append
        (x Int64) engine Memory as select throwIf(number = 0) as x from numbers(1);"

# Wait until attempt 2 has entered executeRefresh. system.view_refreshes.retry is
# attempt_number minus one while a refresh is in flight, so retry = 1 is also
# reached before the second attempt starts; retry >= 2 is reachable only once
# attempt 2 has entered executeRefresh, which is where the '(attempt N/total)'
# comment is formatted.
for _ in {1..200}; do
    retry=$($CLICKHOUSE_CLIENT -q "select retry from system.view_refreshes where view = 'rmv' and database = currentDatabase()" | xargs)
    if [ "$retry" -ge 2 ] 2>/dev/null; then
        break
    fi
    sleep 0.2
done

# Server is alive and the second attempt ran without UB.
$CLICKHOUSE_CLIENT -q "select 'ok', retry >= 2 from system.view_refreshes where view = 'rmv' and database = currentDatabase()"

# The comment the guarded branch produced. 9223372036854775808 is
# static_cast<UInt64>(INT64_MAX) + 1, i.e. the value the guard computes; a signed
# overflow there would print something else.
$CLICKHOUSE_CLIENT -q "system flush logs query_log"
$CLICKHOUSE_CLIENT -q "select 'formatted', count() > 0 from system.query_log where current_database = currentDatabase() and log_comment like 'refresh of %rmv (attempt %/9223372036854775808)'"

$CLICKHOUSE_CLIENT -q "drop table rmv"
