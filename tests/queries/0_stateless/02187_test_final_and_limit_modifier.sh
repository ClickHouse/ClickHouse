#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create table and fill it
$CLICKHOUSE_CLIENT -nm <<EOF
DROP TABLE IF EXISTS test_02187;
CREATE TABLE test_02187 (
    id String,
    num Int32,
    finalDate DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(finalDate)
ORDER BY id;
INSERT INTO TABLE test_02187(id, num) VALUES ('1', 0);
INSERT INTO TABLE test_02187(id, num) VALUES ('1', 1);
EOF

# Get answers for queries
answer_first=`$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02187 FINAL"`
answer_second=`$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02187 FINAL LIMIT 1"`

# Check that they are equal and not empty.
if [[ "$answer_first" = "$answer_second" && -n "$answer_first" ]]
then 
echo "Ok"
else 
echo "Fail"
fi
