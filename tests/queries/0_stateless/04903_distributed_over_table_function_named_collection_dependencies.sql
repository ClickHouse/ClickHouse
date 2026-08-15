-- Tags: no-parallel
-- no-parallel: CREATE/DROP NAMED COLLECTION mutate global server state shared by concurrent tests.

-- A `Distributed` engine persists its table-function target, unlike a query-local `remote` call. Therefore a
-- named collection used by that target must stay alive for the lifetime of the table, including after its
-- metadata is loaded again. Otherwise a later read would fail after `DROP NAMED COLLECTION`.
DROP NAMED COLLECTION IF EXISTS nc_04903_distributed_target;
CREATE NAMED COLLECTION nc_04903_distributed_target AS addresses_expr = '127.0.0.1', database = '', table = 'src_04903';
CREATE TABLE src_04903 (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE dist_04903 ENGINE = Distributed(test_shard_localhost, remote(nc_04903_distributed_target));

DROP NAMED COLLECTION nc_04903_distributed_target; -- { serverError NAMED_COLLECTION_IS_USED }
DETACH TABLE dist_04903;
ATTACH TABLE dist_04903;
DROP NAMED COLLECTION nc_04903_distributed_target; -- { serverError NAMED_COLLECTION_IS_USED }

DROP TABLE dist_04903;
DROP NAMED COLLECTION nc_04903_distributed_target;
DROP TABLE src_04903;
