#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `ip_trie` dictionary rejects `Nullable` attributes, but it does support `Array` ones.
# Pin the wording of the diagnostic so it cannot regress to also claiming arrays are unsupported.

$CLICKHOUSE_CLIENT -q "
CREATE TABLE ip_trie_source (prefix String, nullable_value Nullable(Int64), array_value Array(String)) ENGINE = TinyLog;
" < /dev/null

$CLICKHOUSE_CLIENT -q "INSERT INTO ip_trie_source VALUES ('127.0.0.0/8', 1, ['a', 'b'])" < /dev/null

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY ip_trie_nullable_dictionary
(
    prefix String,
    nullable_value Nullable(Int64) DEFAULT NULL
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_source' DB currentDatabase()))
LIFETIME(MIN 0 MAX 0)
LAYOUT(IP_TRIE());

CREATE DICTIONARY ip_trie_array_dictionary
(
    prefix String,
    array_value Array(String)
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_source' DB currentDatabase()))
LIFETIME(MIN 0 MAX 0)
LAYOUT(IP_TRIE());
" < /dev/null

echo '--- A Nullable attribute is rejected, and the message says only that:'
$CLICKHOUSE_CLIENT -q "SELECT dictGet(currentDatabase() || '.ip_trie_nullable_dictionary', 'nullable_value', tuple(IPv4StringToNum('127.0.0.1')))" < /dev/null 2>&1 \
    | grep -oE ': [a-z ]*attributes not supported for dictionary of type Trie' | sed 's/^: //' | sort -u

echo '--- An Array attribute is supported:'
$CLICKHOUSE_CLIENT -q "SELECT dictGet(currentDatabase() || '.ip_trie_array_dictionary', 'array_value', tuple(IPv4StringToNum('127.0.0.1')))" < /dev/null
