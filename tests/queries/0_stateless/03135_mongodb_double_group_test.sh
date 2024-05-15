#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Mutable operations with clickhouse-client
$CLICKHOUSE_CLIENT --multiquery <<EOF
CREATE TABLE aggr (_id Int32, name String, age Int32) Engine = Memory;
INSERT INTO aggr VALUES (0, 'Alex', 20), (1, 'Alex', 15), (2, 'John', 15), (3, 'John', 20), (4, 'John', 20);
EOF

cat <<EOF | ${MONGODB_CLIENT} --quiet
use ${CLICKHOUSE_DATABASE};
db.aggr.aggregate([{\$group: {_id: {name: "\$name", age: "\$age"}}}, {\$group: {_id: "\$_id.age", c: {\$sum : 1}}}, {\$sort : {c: -1, age: -1}}]);
db.aggr.aggregate([{\$match: {name : {\$ne: "Alex"}}},{\$group: {_id: {name: "\$name", age: "\$age"}}}, {\$group: {_id: "\$_id.age", c: {\$sum : 1}}}, {\$sort : {c : -1}}]);
EOF