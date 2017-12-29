#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -o errexit
set -o pipefail

$CLICKHOUSE_CLIENT -n --query="
    DROP TABLE IF EXISTS test.users;
    CREATE TABLE test.users (UserID UInt64) ENGINE = Log;
    INSERT INTO test.users VALUES (1468013291393583084);
    INSERT INTO test.users VALUES (1321770221388956068);
";

for i in {1..10}; do seq 1 10 | sed "s/.*/SELECT count() FROM (SELECT * FROM remote('127.0.0.{1,2}', test, users) WHERE UserID IN (SELECT arrayJoin([1468013291393583084, 1321770221388956068])));/" | $CLICKHOUSE_CLIENT -n | grep -vE '^4$' && echo 'Fail!' && break; echo -n '.'; done; echo

$CLICKHOUSE_CLIENT --query="DROP TABLE test.users;";
