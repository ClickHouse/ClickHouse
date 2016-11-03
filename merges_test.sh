#!/bin/bash
set -e

pre=
chunk=8192
rows=$((90 * chunk))

clickhouse-client --query "DROP TABLE IF EXISTS test.merges"
clickhouse-client --query "DROP TABLE IF EXISTS test.merges_ref"
#sudo rm -r /opt/clickhouse/data/test/merges

clickhouse-client --query "CREATE TABLE test.merges (part Date, id UInt64, data UInt64) ENGINE = MergeTree(part, id, 8192)"
clickhouse-client --query "CREATE TABLE test.merges_ref (part Date, id UInt64, data UInt64) ENGINE = Memory"

iter=0
for i in $(seq 0 $chunk $((rows-1)) ); do
	iter=$((iter+1))
	clickhouse-client --query "insert into test.merges select toDate(0) as part, intHash64(number) as id, number as data from system.numbers limit $i, $chunk"
	clickhouse-client --query "insert into test.merges_ref select toDate(0) as part, intHash64(number) as id, number as data from system.numbers limit $i, $chunk"
	
	parts=$( clickhouse-client --query "SELECT count(*) FROM system.parts WHERE table = 'merges' AND active = 1" )
	echo "i=$iter p=$parts [$i, $chunk) / $rows"
done

clickhouse-client --query "SELECT name, active, min_block_number, max_block_number FROM system.parts WHERE table = 'merges' AND active = 1 FORMAT PrettyCompact"
clickhouse-client --query "OPTIMIZE TABLE test.merges" || echo "OPTIMIZE FAIL!!!"
clickhouse-client --query "SELECT name, active, min_block_number, max_block_number FROM system.parts WHERE table = 'merges' AND active = 1 FORMAT PrettyCompact"

#clickhouse-client --query "insert into test.merges_ref SELECT * FROM (select toDate(0) as part, intHash64(number) as id, number as data from system.numbers limit 0, $rows) ORDER BY id"
#clickhouse-client --query "SELECT id, data FROM test.merges_ref ORDER BY id" > tmp_ref
clickhouse-client --query "SELECT id, data FROM (select toDate(0) as part, intHash64(number) as id, number as data from system.numbers limit $rows) ORDER BY id" > tmp_ref
clickhouse-client -n --query "SET max_threads = 1; SELECT id, data FROM test.merges;" > tmp_src

cmp tmp_ref tmp_src || echo "FAIL"

#clickhouse-client --query "CREATE TEMPORARY TABLE test.merges_union (id ALIAS test.merges.id, id_ref ALIAS test.merges_ref.id, data ALIAS test.merges.data, data_ref ALIAS test.merges_ref.id)"
#clickhouse-client --query "SELECT sum(abs(id - id_ref)), sum(abs(data - data_ref)) FROM test.merges_union"