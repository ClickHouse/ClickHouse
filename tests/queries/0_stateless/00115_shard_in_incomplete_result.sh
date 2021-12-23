#!/usr/bin/env bash
# Tags: shard

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

$CLICKHOUSE_CLIENT -n --query="
    DROP TABLE IF EXISTS users;
    CREATE TABLE users (UserID UInt64) ENGINE = Log;
    INSERT INTO users VALUES (1468013291393583084);
    INSERT INTO users VALUES (1321770221388956068);
";

for _ in {1..10}; do seq 1 10 | sed "s/.*/SELECT count() FROM (SELECT * FROM remote('127.0.0.{2,3}', ${CLICKHOUSE_DATABASE}, users) WHERE UserID IN (SELECT arrayJoin([1468013291393583084, 1321770221388956068])));/" | $CLICKHOUSE_CLIENT -n | grep -vE '^4$' && echo 'Fail!' && break; echo -n '.'; done; echo

$CLICKHOUSE_CLIENT --query="DROP TABLE users;";
