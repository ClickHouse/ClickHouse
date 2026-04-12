-- Tags: no-parallel

-- SQL catalog: CREATE / ALTER SHARD and CLUSTER, system.shards + system.clusters, SHOW CREATE, teardown.

DROP CLUSTER IF EXISTS sqlcat_04071_cluster;
DROP SHARD IF EXISTS sqlcat_04071_shard2;
DROP SHARD IF EXISTS sqlcat_04071_shard;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep1;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep2;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep_alt;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep_extra;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep3;

CREATE NAMED COLLECTION sqlcat_04071_rep1 AS host = '127.0.0.1', port = 9000, user = 'default';
CREATE NAMED COLLECTION sqlcat_04071_rep2 AS host = '127.0.0.1', port = 9000, user = 'default';
CREATE NAMED COLLECTION sqlcat_04071_rep_alt AS host = '127.0.0.1', port = 9000, user = 'default';

-- --- CREATE SHARD: `REPLICA a, b` (multiline PROPERTIES) | `REPLICA (a, b)` | `shard(rep...)` ---
-- Skipped until CREATE REPLICA is implemented (next commit): multiline PROPERTIES form.
-- Placeholder: same endpoint as the planned REPLICA would create.
CREATE NAMED COLLECTION sqlcat_04071_rep3 AS host = '127.0.0.1', port = 9000, user = 'default';

CREATE SHARD sqlcat_04071_syn2 REPLICA sqlcat_04071_rep1, sqlcat_04071_rep3
PROPERTIES
    weight = 1,
    internal_replication = false
;

CREATE SHARD sqlcat_04071_syn3 REPLICA (sqlcat_04071_rep2, sqlcat_04071_rep3) PROPERTIES (weight = 2, internal_replication = true);

CREATE CLUSTER sqlcat_04071_syn_cluster (sqlcat_04071_syn2)
PROPERTIES secret = 'sqlcat04071_test_secret', allow_distributed_ddl_queries = false;

SELECT name, replica_collections, weight, internal_replication, arraySort(referenced_by_clusters) FROM system.shards WHERE name IN ('sqlcat_04071_syn2', 'sqlcat_04071_syn3') ORDER BY name;
SHOW CREATE SHARD sqlcat_04071_syn2;
SHOW CREATE SHARD sqlcat_04071_syn3;
SHOW CREATE CLUSTER sqlcat_04071_syn_cluster;

DROP CLUSTER IF EXISTS sqlcat_04071_syn_cluster;
DROP SHARD IF EXISTS sqlcat_04071_syn3;
DROP SHARD IF EXISTS sqlcat_04071_syn2;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep3;
-- Not covered here yet (interpreter NOT_IMPLEMENTED): ALTER CLUSTER MODIFY|RENAME SHARD; ALTER SHARD MODIFY|RENAME REPLICA.
-- --- end syntax snippet ---

CREATE SHARD sqlcat_04071_shard REPLICA sqlcat_04071_rep1, sqlcat_04071_rep2 PROPERTIES weight = 2, internal_replication = true;
CREATE CLUSTER sqlcat_04071_cluster (sqlcat_04071_shard);

DROP NAMED COLLECTION sqlcat_04071_rep1; -- { serverError NAMED_COLLECTION_IS_REFERENCED }
DROP SHARD sqlcat_04071_shard; -- { serverError SHARD_IS_REFERENCED }

SELECT name, replica_collections, weight, internal_replication, arraySort(referenced_by_clusters) FROM system.shards WHERE name = 'sqlcat_04071_shard' ORDER BY name;
SELECT cluster, shard_num, replica_num, host_name, port, user, internal_replication, shard_weight FROM system.clusters WHERE cluster = 'sqlcat_04071_cluster' ORDER BY shard_num, replica_num FORMAT TabSeparated;
SHOW CREATE SHARD sqlcat_04071_shard;
SHOW CREATE CLUSTER sqlcat_04071_cluster;

ALTER SHARD sqlcat_04071_shard MODIFY PROPERTIES (weight = 3, internal_replication = false);
SELECT name, replica_collections, weight, internal_replication, arraySort(referenced_by_clusters) FROM system.shards WHERE name = 'sqlcat_04071_shard' ORDER BY name;
SELECT cluster, shard_num, replica_num, host_name, port, user, internal_replication, shard_weight FROM system.clusters WHERE cluster = 'sqlcat_04071_cluster' ORDER BY shard_num, replica_num FORMAT TabSeparated;
SHOW CREATE SHARD sqlcat_04071_shard;

-- Unknown shard-level key in MODIFY PROPERTIES must fail without mutating the catalog.
ALTER SHARD sqlcat_04071_shard MODIFY PROPERTIES (weight = 3, internal_replication = true, bad_property = 1); -- { serverError BAD_ARGUMENTS }

-- `internal_replication` only allows true/false, 0/1, 'true'/'false' (not arbitrary integers).
ALTER SHARD sqlcat_04071_shard MODIFY PROPERTIES (weight = 3, internal_replication = 1255); -- { serverError BAD_ARGUMENTS }

-- ALTER SHARD: REPLACE ... TO ... (lists, parentheses, chained REPLACE, optional MODIFY PROPERTIES).
ALTER SHARD sqlcat_04071_shard REPLACE sqlcat_04071_rep1 TO sqlcat_04071_rep_alt;
SELECT name, replica_collections, weight, internal_replication FROM system.shards WHERE name = 'sqlcat_04071_shard' ORDER BY name;
ALTER SHARD sqlcat_04071_shard REPLACE (sqlcat_04071_rep_alt, sqlcat_04071_rep2) TO (sqlcat_04071_rep2, sqlcat_04071_rep_alt);
SELECT name, replica_collections, weight, internal_replication FROM system.shards WHERE name = 'sqlcat_04071_shard' ORDER BY name;
ALTER SHARD sqlcat_04071_shard REPLACE sqlcat_04071_rep2, sqlcat_04071_rep_alt TO sqlcat_04071_rep_alt, sqlcat_04071_rep2;
SELECT name, replica_collections, weight, internal_replication FROM system.shards WHERE name = 'sqlcat_04071_shard' ORDER BY name;
ALTER SHARD sqlcat_04071_shard REPLACE sqlcat_04071_rep_alt TO sqlcat_04071_rep1, REPLACE sqlcat_04071_rep2 TO sqlcat_04071_rep_alt MODIFY PROPERTIES weight = 7, internal_replication = false;
SELECT name, replica_collections, weight, internal_replication FROM system.shards WHERE name = 'sqlcat_04071_shard' ORDER BY name;
SHOW CREATE SHARD sqlcat_04071_shard;
ALTER SHARD sqlcat_04071_shard REPLACE sqlcat_04071_rep_alt TO sqlcat_04071_rep2 MODIFY PROPERTIES (weight = 3, internal_replication = true);
SELECT name, replica_collections, weight, internal_replication FROM system.shards WHERE name = 'sqlcat_04071_shard' ORDER BY name;

-- ALTER SHARD: MODIFY PROPERTIES only (replica list unchanged).
ALTER SHARD sqlcat_04071_shard MODIFY PROPERTIES (internal_replication = true);
SELECT name, replica_collections, weight, internal_replication, arraySort(referenced_by_clusters) FROM system.shards WHERE name = 'sqlcat_04071_shard' ORDER BY name;
SELECT cluster, shard_num, replica_num, host_name, port, user, internal_replication, shard_weight FROM system.clusters WHERE cluster = 'sqlcat_04071_cluster' ORDER BY shard_num, replica_num FORMAT TabSeparated;
SHOW CREATE SHARD sqlcat_04071_shard;

-- ALTER SHARD: ADD REPLICA / DROP REPLICA (named collection must already exist).
CREATE NAMED COLLECTION sqlcat_04071_rep_extra AS host = '127.0.0.1', port = 9000, user = 'default';
ALTER SHARD sqlcat_04071_shard ADD REPLICA sqlcat_04071_rep_extra;
SELECT name, replica_collections, weight, internal_replication, arraySort(referenced_by_clusters) FROM system.shards WHERE name = 'sqlcat_04071_shard' ORDER BY name;
SHOW CREATE SHARD sqlcat_04071_shard;
ALTER SHARD sqlcat_04071_shard DROP REPLICA sqlcat_04071_rep_extra;
SELECT name, replica_collections, weight, internal_replication, arraySort(referenced_by_clusters) FROM system.shards WHERE name = 'sqlcat_04071_shard' ORDER BY name;
SHOW CREATE SHARD sqlcat_04071_shard;

CREATE SHARD sqlcat_04071_shard2(sqlcat_04071_rep1) PROPERTIES weight = 1, internal_replication = false;
ALTER CLUSTER sqlcat_04071_cluster ADD SHARD sqlcat_04071_shard2;
ALTER CLUSTER sqlcat_04071_cluster REPLACE sqlcat_04071_shard, sqlcat_04071_shard2 TO sqlcat_04071_shard2, sqlcat_04071_shard;
SELECT name, replica_collections, weight, internal_replication, arraySort(referenced_by_clusters) FROM system.shards WHERE name IN ('sqlcat_04071_shard', 'sqlcat_04071_shard2') ORDER BY name;
SELECT cluster, shard_num, replica_num, host_name, port, user, internal_replication, shard_weight FROM system.clusters WHERE cluster = 'sqlcat_04071_cluster' ORDER BY shard_num, replica_num FORMAT TabSeparated;
SHOW CREATE CLUSTER sqlcat_04071_cluster;

-- ALTER CLUSTER: ADD SHARD / DROP SHARD / REPLACE ... TO ... [, REPLACE ...] [MODIFY PROPERTIES].
ALTER CLUSTER sqlcat_04071_cluster DROP SHARD sqlcat_04071_shard2;
SHOW CREATE CLUSTER sqlcat_04071_cluster;
ALTER CLUSTER sqlcat_04071_cluster ADD SHARD sqlcat_04071_shard2;
ALTER CLUSTER sqlcat_04071_cluster REPLACE sqlcat_04071_shard, sqlcat_04071_shard2 TO sqlcat_04071_shard2, sqlcat_04071_shard;
ALTER CLUSTER sqlcat_04071_cluster REPLACE sqlcat_04071_shard2, sqlcat_04071_shard TO sqlcat_04071_shard, sqlcat_04071_shard2 MODIFY PROPERTIES (allow_distributed_ddl_queries = false);
SHOW CREATE CLUSTER sqlcat_04071_cluster;
ALTER CLUSTER sqlcat_04071_cluster REPLACE sqlcat_04071_shard TO sqlcat_04071_shard MODIFY PROPERTIES (allow_distributed_ddl_queries = true);
SHOW CREATE CLUSTER sqlcat_04071_cluster;

DROP CLUSTER IF EXISTS sqlcat_04071_cluster;
DROP SHARD IF EXISTS sqlcat_04071_shard2;
DROP SHARD IF EXISTS sqlcat_04071_shard;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep1;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep2;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep_alt;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep_extra;
DROP NAMED COLLECTION IF EXISTS sqlcat_04071_rep3;

SELECT count() FROM system.shards WHERE name IN ('sqlcat_04071_shard', 'sqlcat_04071_shard2');
