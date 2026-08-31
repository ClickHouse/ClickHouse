#!/usr/bin/env bash
# An Alias whose target does not exist yet is accepted, so the target can later become an Alias
# itself. Loading such stored metadata must succeed: on that path a rejection fails the whole
# metadata load, not the one table.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A full-definition ATTACH in an Atomic database requires an explicit UUID; generate a random one
# to avoid collisions between concurrent runs of this test.
UUID_ALIAS_TARGET=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
UUID_SELF_REF=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS base_table;
DROP TABLE IF EXISTS outer_alias;
DROP TABLE IF EXISTS inner_alias;

CREATE TABLE base_table (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO base_table VALUES (1), (2);
"

# The target is absent, so nothing rejects this yet.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE outer_alias ENGINE = Alias(currentDatabase(), inner_alias);
CREATE TABLE inner_alias ENGINE = Alias(currentDatabase(), base_table);
"

echo '-- a stored Alias-to-Alias definition loads'
$CLICKHOUSE_CLIENT -q "
DETACH TABLE outer_alias;
ATTACH TABLE outer_alias;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'outer_alias';
"

echo '-- CREATE still rejects an Alias target'
# -m1 because the error message contains the error code name multiple times.
$CLICKHOUSE_CLIENT -q "CREATE TABLE rejected_at_create ENGINE = Alias(currentDatabase(), inner_alias);" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

echo '-- a full-definition ATTACH still rejects an Alias target'
# `send_logs_level=fatal` suppresses the "full table definition is not recommended" warning.
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "ATTACH TABLE rejected_at_full_attach UUID '${UUID_ALIAS_TARGET}' ENGINE = Alias(currentDatabase(), inner_alias);" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

echo '-- CREATE still rejects a self-reference'
$CLICKHOUSE_CLIENT -q "CREATE TABLE self_ref ENGINE = Alias(currentDatabase(), self_ref);" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

echo '-- a full-definition ATTACH still rejects a self-reference'
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "ATTACH TABLE self_ref_attach UUID '${UUID_SELF_REF}' ENGINE = Alias(currentDatabase(), self_ref_attach);" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

echo '-- reading through the chain reaches the target'
$CLICKHOUSE_CLIENT -q "SELECT * FROM outer_alias ORDER BY a;"

$CLICKHOUSE_CLIENT -q "
DROP TABLE outer_alias;
DROP TABLE inner_alias;
DROP TABLE base_table;
"
