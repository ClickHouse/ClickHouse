#!/usr/bin/env bash
# Tags: no-debug

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS kill_mutation"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE kill_mutation(d Date, x UInt32, s String) ENGINE MergeTree ORDER BY x PARTITION BY d"

${CLICKHOUSE_CLIENT} --query="INSERT INTO kill_mutation VALUES ('2000-01-01', 1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO kill_mutation VALUES ('2001-01-01', 2, 'b')"

${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill a single invalid mutation ***'"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE kill_mutation DELETE WHERE toUInt32(s) = 1 SETTINGS mutations_sync = 1" 2>/dev/null

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation' and is_done = 0"

${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation'"

${CLICKHOUSE_CLIENT} --query="SELECT mutation_id FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation'"

${CLICKHOUSE_CLIENT} --query="SELECT '*** Create and kill invalid mutation that blocks another mutation ***'"

# Note: there is a benign race condition.
# The mutation can fail with the message
# "Cannot parse string 'a' as UInt32"
# or
# "Cannot parse string 'b' as UInt32"
# depending on which parts are processed first.
# The mutations are also coalesced together, and the subsequent mutation inherits the failure status of the original mutation.
# When we are waiting for mutations, we are listing all the mutations with identical error messages.
# But due to a race condition and to repeated runs, the original and subsequent mutations can have different error messages,
# therefore the original mutation will not be included in the list.

# Originally, there was grep "happened during execution of mutations 'mutation_4.txt, mutation_5.txt'",
# but due to this race condition, I've replaced it to grep "happened during execution of mutation"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE kill_mutation DELETE WHERE toUInt32(s) = 1"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE kill_mutation DELETE WHERE x = 1 SETTINGS mutations_sync = 1" 2>&1 | grep -o "happened during execution of mutation" | head -n 1

# but exception doesn't stop mutations, and we will still see them in system.mutations
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation' AND mutation_id = 'mutation_4.txt'" # 1

# waiting	test	kill_mutation	mutation_4.txt	DELETE WHERE toUInt32(s) = 1
${CLICKHOUSE_CLIENT} --query="KILL MUTATION WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'kill_mutation' AND mutation_id = 'mutation_4.txt'"

# just to wait previous mutation to finish (and don't poll system.mutations), doesn't affect data
${CLICKHOUSE_CLIENT} --query="ALTER TABLE kill_mutation DELETE WHERE x = 1 SETTINGS mutations_sync = 1"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM kill_mutation" # 2001-01-01	2	b

# must always be empty
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.mutations WHERE table = 'kill_mutation' AND database = '$CLICKHOUSE_DATABASE' AND is_done = 0"

${CLICKHOUSE_CLIENT} --query="DROP TABLE kill_mutation"
