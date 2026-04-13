DROP NAMED COLLECTION IF EXISTS nc_replica_for_alter_drop;
DROP NAMED COLLECTION IF EXISTS nc_replica_for_type_mix;

CREATE REPLICA nc_replica_for_alter_drop
PROPERTIES (host = '127.0.0.1', port = 9000, secure = 0, priority = 1);

CREATE REPLICA nc_replica_for_alter_drop
PROPERTIES (host = '127.0.0.1', port = 9000); -- { serverError NAMED_COLLECTION_ALREADY_EXISTS }

ALTER REPLICA nc_replica_for_alter_drop
MODIFY PROPERTIES (secure = 1, priority = 2);

SELECT name, secure, priority
FROM system.replicas_collection
WHERE name = 'nc_replica_for_alter_drop';

DROP REPLICA nc_replica_not_exist; -- { serverError NAMED_COLLECTION_DOESNT_EXIST }
DROP REPLICA IF EXISTS nc_replica_not_exist;

CREATE REPLICA nc_replica_bad_property
PROPERTIES (host = '127.0.0.1', port = 9000, bad_property = 1); -- { serverError BAD_ARGUMENTS }

CREATE REPLICA nc_replica_bad_port_zero
PROPERTIES (host = '127.0.0.1', port = 0); -- { serverError BAD_ARGUMENTS }

CREATE REPLICA nc_replica_bad_port_large
PROPERTIES (host = '127.0.0.1', port = 70000); -- { serverError BAD_ARGUMENTS }

CREATE REPLICA nc_replica_bad_host_type
PROPERTIES (host = 127001, port = 9000); -- { serverError BAD_ARGUMENTS }

CREATE REPLICA nc_replica_bad_host_empty
PROPERTIES (host = '', port = 9000); -- { serverError BAD_ARGUMENTS }

CREATE REPLICA nc_replica_bad_duplicate_key
PROPERTIES (host = '127.0.0.1', port = 9000, secure = 0, secure = 1); -- { serverError BAD_ARGUMENTS }

CREATE REPLICA nc_replica_missing_host
PROPERTIES (port = 9000, secure = 1); -- { serverError BAD_ARGUMENTS }

CREATE REPLICA nc_replica_missing_port
PROPERTIES (host = '127.0.0.1', secure = 1); -- { serverError BAD_ARGUMENTS }

CREATE REPLICA nc_replica_for_type_mix
PROPERTIES (host = '127.0.0.3', port = '9001', secure = true, compression = 1, priority = '3');

SELECT
    name,
    host = '127.0.0.3',
    port = '9001',
    (secure = '1' OR lowerUTF8(secure) = 'true'),
    (compression = '1' OR lowerUTF8(compression) = 'true'),
    priority = '3'
FROM system.replicas_collection
WHERE name = 'nc_replica_for_type_mix';

DROP REPLICA nc_replica_for_alter_drop;
DROP REPLICA nc_replica_for_type_mix;

SELECT count()
FROM system.named_collections
WHERE name IN ('nc_replica_for_alter_drop', 'nc_replica_for_type_mix');
