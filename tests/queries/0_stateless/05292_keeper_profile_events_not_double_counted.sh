#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# Tag no-parallel: `srst` sent by other tests resets the server-wide Keeper counters used as the reference here

# Keeper ProfileEvents were incremented twice per packet: once for the per-connection
# stats and once for the server-wide stats. Compare their growth with the server-wide
# counters exposed as asynchronous metrics, which count each packet once.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Prints received and sent packets as: ProfileEvents before the asynchronous metrics refresh,
# asynchronous metrics, ProfileEvents after the refresh. The metrics are sampled between the two reads.
function snapshot()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT sumIf(value, event = 'KeeperPacketsReceived'), sumIf(value, event = 'KeeperPacketsSent') FROM system.events;
        SYSTEM RELOAD ASYNCHRONOUS METRICS;
        SELECT toUInt64(sumIf(value, metric = 'KeeperPacketsReceived')), toUInt64(sumIf(value, metric = 'KeeperPacketsSent')) FROM system.asynchronous_metrics;
        SELECT sumIf(value, event = 'KeeperPacketsReceived'), sumIf(value, event = 'KeeperPacketsSent') FROM system.events;
    " | tr '\n\t' '  '
}

# Arguments: event before refresh, metric, event after refresh; first for the initial snapshot, then for the final one.
# The growth of the event is known to be within [lower, upper], and it must match the growth of the metric.
function check()
{
    local metric=$(( $5 - $2 )) lower=$(( $4 - $3 )) upper=$(( $6 - $1 ))
    if (( metric < 1000 )); then
        echo "fail (metric grew by $metric)"
    elif (( 10 * (upper - lower) > metric )); then
        echo retry
    elif (( 10 * lower >= 9 * metric && 10 * upper <= 11 * metric )); then
        echo pass
    elif (( 10 * upper < 9 * metric || 10 * lower > 11 * metric )); then
        echo "fail (metric grew by $metric, event by $lower..$upper)"
    else
        echo retry
    fi
}

requests=$(printf 'exists "/"; %.0s' {1..1000})

for _ in {1..10}; do
    read -r -a before <<< "$(snapshot)"
    $CLICKHOUSE_KEEPER_CLIENT -q "$requests" > /dev/null
    read -r -a after <<< "$(snapshot)"

    received=$(check "${before[0]}" "${before[2]}" "${before[4]}" "${after[0]}" "${after[2]}" "${after[4]}")
    sent=$(check "${before[1]}" "${before[3]}" "${before[5]}" "${after[1]}" "${after[3]}" "${after[5]}")

    if [[ $received == pass && $sent == pass ]]; then
        echo OK
        exit 0
    fi
    if [[ $received == fail* || $sent == fail* ]]; then
        echo "KeeperPacketsReceived: $received, KeeperPacketsSent: $sent"
        exit 0
    fi
done

echo "Inconclusive, too much concurrent Keeper traffic: before ${before[*]}, after ${after[*]}"
