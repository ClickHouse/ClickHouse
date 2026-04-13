DROP NAMED COLLECTION IF EXISTS nc_plain_type_test;
DROP NAMED COLLECTION IF EXISTS nc_replica_type_test;

CREATE NAMED COLLECTION nc_plain_type_test AS host = '127.0.0.1';
CREATE NAMED COLLECTION nc_replica_type_test TYPE REPLICA AS host = '127.0.0.2', port = 9000;

SELECT name, type
FROM system.named_collections
WHERE name IN ('nc_plain_type_test', 'nc_replica_type_test')
ORDER BY name;

SELECT positionCaseInsensitive(create_query, ' TYPE ') > 0
FROM system.named_collections
WHERE name IN ('nc_plain_type_test', 'nc_replica_type_test')
ORDER BY name;

SELECT name, host, port
FROM system.replicas_collection
WHERE name = 'nc_replica_type_test';

DROP NAMED COLLECTION nc_plain_type_test;
DROP NAMED COLLECTION nc_replica_type_test;
