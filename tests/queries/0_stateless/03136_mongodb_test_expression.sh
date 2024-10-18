#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Mutable operations with clickhouse-client
$CLICKHOUSE_CLIENT --multiquery <<EOF
CREATE TABLE aggr (_id Int32, name String, age Int32, date DateTime) Engine = Memory;
INSERT INTO aggr VALUES (0, 'Alexa', 20, '2024-03-12 23:14:15'), (1, 'Alexa', 15, '2016-11-29 11:18:00'), (2, 'John', 15, '2000-01-12 00:14:15'), (3, 'John', 20, '2003-08-14 12:12:12'), (4, 'John', 100, '2001-08-14 13:12:34');
EOF

cat <<EOF | ${MONGODB_CLIENT} --quiet
use ${CLICKHOUSE_DATABASE};
db.aggr.aggregate([{\$group : {_id: null, a: {\$avg: {\$toDecimal: "\$age"}}}}]);
db.aggr.aggregate([{\$group : {_id: {name: "\$name", m: {\$minute: "\$date"}}, c: {\$sum : 1}}}, {\$sort: {c : -1, name: 1}}, {\$limit: 10}]);
db.aggr.aggregate([{\$group : {_id: {m: {\$minute: "\$date"}}, c: {\$sum : 1}}}, {\$sort: {c : -1, m: 1}}, {\$limit: 10}]);
db.aggr.aggregate([{ \$match: { name: { \$ne: "" } } },{\$group: {_id: "\$age", l: { \$avg: { \$strLenBytes: "\$name" } },c: { \$sum: 1 },},},{ \$sort: { l: -1, age: -1 } },{ \$limit: 25 },]);
db.aggr.aggregate([{ \$project: { _id: 0, long_age: { "\$toLong": "\$age" } } },{ \$group: {_id : "\$long_age"} }]);
db.aggr.aggregate([{\$match: {date: { \$gte: ISODate("2000-01-01"), \$lte: ISODate("2010-06-15") },},}]);
EOF