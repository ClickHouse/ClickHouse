#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A typo in `trace_profile_events_list` should give a readable error, not `std::out_of_range`.
error=$($CLICKHOUSE_CLIENT --trace_profile_events 1 --trace_profile_events_list 'DiskS3PutObject,DiskS3CommitBlockList' --query "SELECT 1" 2>&1)

echo "$error" | grep -qF 'Unknown profile event: DiskS3CommitBlockList' && echo 'the message names the unknown event'
echo "$error" | grep -qF 'Maybe you meant' && echo 'the message suggests the closest names'
echo "$error" | grep -qF 'DiskAzureCommitBlockList' && echo 'the suggestions contain the closest name'
echo "$error" | grep -qF 'out_of_range' && echo 'FAIL: std::out_of_range leaked into the message'

# Spaces around the names and a trailing comma are allowed, and only the named events are traced.
query_id_listed="listed-$CLICKHOUSE_DATABASE-$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$query_id_listed" --trace_profile_events 1 --trace_profile_events_list ' Query, SelectQuery, ' --query "SELECT 2 FORMAT Null"

# A list of only separators and spaces means "trace everything", exactly as an empty list does.
query_id_normalized_empty="normalized-empty-$CLICKHOUSE_DATABASE-$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$query_id_normalized_empty" --trace_profile_events 1 --trace_profile_events_list ' , ' --query "SELECT 3 FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS trace_log"

$CLICKHOUSE_CLIENT --query "
    SELECT arraySort(groupUniqArray(event))
    FROM system.trace_log
    WHERE event_date >= yesterday() AND query_id = '$query_id_listed' AND trace_type = 'ProfileEvent'"

$CLICKHOUSE_CLIENT --query "
    SELECT countIf(event = 'Query') > 0, countIf(event = 'SelectQuery') > 0, uniqExact(event) > 2
    FROM system.trace_log
    WHERE event_date >= yesterday() AND query_id = '$query_id_normalized_empty' AND trace_type = 'ProfileEvent'"
