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
db.aggr.aggregate([{\$group: {_id: "\$city", sum_money: {\$sum: "\$money"}, avg_age: {\$avg: "\$age" }, c: {\$sum: 1}}}, {\$sort: { c: -1, avg_age: -1}}, { \$limit: 10}]);
db.aggr.aggregate([{ \$project: { _id: 1 } },{ \$count: "c" }]);
db.aggr.aggregate([{ \$match: { age: { \$ne: 10 } } },{ \$count: "c"}])
db.aggr.aggregate([{\$group: {_id: null, sum_age: { \$sum: "\$age" },c: { \$sum: 1 },avg_money: { \$avg: "\$money" },},},])
db.aggr.aggregate([{ \$group: { _id: "\$city" } }, { \$count: "c" }, {\$sort: {city: 1}}])
db.aggr.aggregate([{ \$match: { name: { \$ne: "Alex" } } },{ \$group: { _id: "\$city", c: { \$sum: 1 } } },{ \$sort: { c: -1, city: 1} },])
db.aggr.aggregate([{ \$group: { _id: { city: "\$city", name: "\$name" } } },{ \$group: { _id: "\$_id.city", u: { \$sum: 1 } } },{ \$sort: { u: -1, city: 1 } },{ \$limit: 10 },])
db.aggr.aggregate([{ \$group: { _id: "\$city", c: { \$sum: 1} } },{ \$sort: { c: -1, city: 1 } },{ \$limit: 10 },])
db.aggr.aggregate([{ \$match: { city: { \$ne: "" } } },{ \$group: { _id: { city: "\$city", name: "\$name" } } },{ \$group: { _id: "\$_id.city", u: { \$sum: 1 } } },{ \$sort: { u: -1, city: 1 } },])
EOF