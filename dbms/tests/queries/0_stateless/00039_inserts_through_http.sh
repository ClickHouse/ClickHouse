#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo 'DROP TABLE IF EXISTS test.long_insert' | curl -sSg 'http://localhost:8123' -d @-
echo 'CREATE TABLE test.long_insert (a String) ENGINE = Memory' | curl -sSg 'http://localhost:8123' -d @-
for string_size in 1 10 100 1000 10000 100000 1000000; do
    # Если не указать LC_ALL=C, то Perl будет ругаться на некоторых плохо настроенных системах.
    LC_ALL=C perl -we 'for my $letter ("a" .. "z") { print(($letter x '$string_size') . "\n") }' | curl -sSg 'http://localhost:8123/?query=INSERT+INTO+test.long_insert+FORMAT+TabSeparated' --data-binary @-
    echo 'SELECT substring(a, 1, 1) AS c, length(a) AS l FROM test.long_insert ORDER BY c, l' | curl -sSg 'http://localhost:8123' -d @-
done
