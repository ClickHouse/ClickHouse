#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Mutable operations with clickhouse-client
$CLICKHOUSE_CLIENT --multiquery <<EOF
CREATE TABLE aggr (_id Int32, name String, age Int32, money Float32, city String) Engine = Memory;
INSERT INTO aggr VALUES (0, 'Alex', 10, 2.3, 'Moscow'), (1, 'Max', 20, 100.1, 'Moscow'), (2, 'Mole', 40, 10000.123, 'Kazan'), (3, 'John', 100, 102, 'Brest');
EOF

cat <<EOF | ${MONGODB_CLIENT} --quiet
use ${CLICKHOUSE_DATABASE};
db.aggr.aggregate([{\$group: {_id: "\$name", c: { \$avg: "\$age" },},},{ \$match: { c: { \$gt: 20 } } },]);
EOF