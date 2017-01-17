#!/usr/bin/env bash

set -o errexit
set -o pipefail

clickhouse-client -n --query="
    DROP TABLE IF EXISTS test.users;
    CREATE TABLE test.users (UserID UInt64) ENGINE = Log;
    INSERT INTO test.users VALUES (1468013291393583084);
    INSERT INTO test.users VALUES (1321770221388956068);
";

for i in {1..10}; do seq 1 10 | sed "s/.*/SELECT count() FROM (SELECT * FROM remote('127.0.0.{1,2}', test, users) WHERE UserID IN (SELECT arrayJoin([1468013291393583084, 1321770221388956068])));/" | clickhouse-client -n | grep -vE '^4$' && echo 'Fail!' && break; echo -n '.'; done; echo

clickhouse-client --query="DROP TABLE test.users;";
