-- Contract for the reserved-key mechanism that underlies SQL replicas (see
-- `Common/NamedCollections/NamedCollectionReservedKeys.h`):
--   * user DDL cannot populate / modify / delete keys in the `__` namespace;
--   * the `__type__` tag injected by `CREATE REPLICA` is hidden from `system.named_collections`;
--   * `system.replicas_collection` exposes replicas discovered via that tag;
--   * plain named collections cannot be upgraded into replicas, and vice versa.
DROP NAMED COLLECTION IF EXISTS nc_plain_reserved;
DROP NAMED COLLECTION IF EXISTS nc_replica_reserved;

-- Reserved keys are rejected in CREATE / ALTER NAMED COLLECTION (any `__`-prefixed name).
CREATE NAMED COLLECTION nc_plain_reserved AS __type__ = 'replica', host = '127.0.0.1'; -- { serverError BAD_ARGUMENTS }
CREATE NAMED COLLECTION nc_plain_reserved AS __anything = 1, host = '127.0.0.1'; -- { serverError BAD_ARGUMENTS }

CREATE NAMED COLLECTION nc_plain_reserved AS host = '127.0.0.1';
ALTER NAMED COLLECTION nc_plain_reserved SET __type__ = 'replica'; -- { serverError BAD_ARGUMENTS }
ALTER NAMED COLLECTION nc_plain_reserved DELETE __type__; -- { serverError BAD_ARGUMENTS }

-- `CREATE REPLICA` injects `__type__='replica'` internally; the tag must be hidden from
-- `system.named_collections` (both from the `collection` map and from `create_query`).
CREATE REPLICA nc_replica_reserved PROPERTIES (host = '127.0.0.2', port = 9000);

SELECT name,
       has(mapKeys(collection), '__type__') AS leaks_tag_in_map,
       positionCaseInsensitive(create_query, '__type__') > 0 AS leaks_tag_in_query
FROM system.named_collections
WHERE name IN ('nc_plain_reserved', 'nc_replica_reserved')
ORDER BY name;

-- Replica is discoverable via `system.replicas_collection` using the tag, not any user-visible field.
SELECT name, host, port
FROM system.replicas_collection
WHERE name = 'nc_replica_reserved';

-- Only replica-tagged NCs accept ALTER REPLICA / DROP REPLICA.
ALTER REPLICA nc_plain_reserved MODIFY PROPERTIES (host = 'x', port = 1); -- { serverError BAD_ARGUMENTS }
DROP REPLICA nc_plain_reserved; -- { serverError BAD_ARGUMENTS }

DROP REPLICA nc_replica_reserved;
DROP NAMED COLLECTION nc_plain_reserved;
