#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
CREATE TABLE mongodb_test (id Int32 PRIMARY KEY, name String, age Int32) Engine=MergeTree;

INSERT INTO mongodb_test VALUES (1, 'Alex', 15);
INSERT INTO mongodb_test VALUES (2, 'John', 23);
INSERT INTO mongodb_test VALUES (3, 'Mike', 32);
INSERT INTO mongodb_test VALUES (4, 'George', 51);
INSERT INTO mongodb_test VALUES (5, 'George', 28);
EOF

cat <<EOF | ${MONGODB_CLIENT} --quiet
use ${CLICKHOUSE_DATABASE}
db.mongodb_test.find().sort({id: 1});
EOF

cat <<EOF | ${MONGODB_CLIENT} --quiet
use ${CLICKHOUSE_DATABASE}
db.mongodb_test.find().sort({id: -1});
EOF

cat <<EOF | ${MONGODB_CLIENT} --quiet
use ${CLICKHOUSE_DATABASE}
db.mongodb_test.find().sort({name: 1});
EOF

cat <<EOF | ${MONGODB_CLIENT} --quiet
use ${CLICKHOUSE_DATABASE}
db.mongodb_test.find().sort({name: -1});
EOF

cat <<EOF | ${MONGODB_CLIENT} --quiet
use ${CLICKHOUSE_DATABASE}
db.mongodb_test.find().sort({name: 1, age: -1});
EOF

cat <<EOF | ${MONGODB_CLIENT} --quiet
use ${CLICKHOUSE_DATABASE}
db.mongodb_test.find().sort({name: 1, age: 1});
EOF