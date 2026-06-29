-- Tags: distributed, no-parallel

-- just a smoke test

-- quirk for ON CLUSTER does not uses currentDatabase()
drop database if exists {CLICKHOUSE_DATABASE_1:Identifier};
create database {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
set distributed_ddl_output_mode='throw';

drop table if exists {CLICKHOUSE_DATABASE_1:Identifier}.dist_01294;
create table {CLICKHOUSE_DATABASE_1:Identifier}.dist_01294 as system.one engine=Distributed(test_shard_localhost, system, one);
-- flush
system flush distributed {CLICKHOUSE_DATABASE_1:Identifier}.dist_01294;
system flush distributed on cluster test_shard_localhost {CLICKHOUSE_DATABASE_1:Identifier}.dist_01294;
-- stop
system stop distributed sends {CLICKHOUSE_DATABASE_1:Identifier}.dist_01294;
system stop distributed sends on cluster test_shard_localhost {CLICKHOUSE_DATABASE_1:Identifier}.dist_01294;
-- start
system start distributed sends {CLICKHOUSE_DATABASE_1:Identifier}.dist_01294;
system start distributed sends on cluster test_shard_localhost {CLICKHOUSE_DATABASE_1:Identifier}.dist_01294;

drop database {CLICKHOUSE_DATABASE_1:Identifier};
