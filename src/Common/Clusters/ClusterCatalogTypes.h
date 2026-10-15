#pragma once

#include <Core/Types.h>

#include <vector>

namespace DB
{

/// **Replica** (endpoint / named-collection body, e.g. `CREATE REPLICA`, `ALTER SHARD ... MODIFY REPLICA`):
/// | key | role |
/// |-----|------|
/// | host | remote host (required for a usable endpoint) |
/// | port | TCP port (required) |
/// | user | default `default` |
/// | password | default empty |
/// | secure | SSL/TLS, default false |
/// | compression | default true |
/// | priority | load balancing, default 1 |
/// | bind_host | optional source bind (IPv4 in typical configs) |
/// | default_database | optional default DB for the endpoint |
struct EndpointCatalogDefinition
{
    static constexpr UInt64 SERIALIZE_VERSION = 1;

    String host;
    UInt16 port = 9000;
    String user = "default";
    String password;
    String default_database;
    bool secure = false;
    bool compression = true;
    Int64 priority = 1;
    String bind_host;

    String serialize() const;
    static EndpointCatalogDefinition deserialize(const String & data);
};

struct EndpointCatalogSystemTableRow
{
    String name;
    EndpointCatalogDefinition endpoint;
    std::vector<String> bound_shards;
};

/// **Shard** (`CREATE SHARD`, `ALTER CLUSTER ... ADD|MODIFY SHARD` shard clause):
/// | key | role |
/// |-----|------|
/// | weight | relative write weight, default 1 |
/// | internal_replication | default false; only `true` / `false`, `0` / `1`, or `'true'` / `'false'` |

/// Parsed body of a SQL `CREATE SHARD` row in the shard catalog (memory image after load / before persist).
struct ShardCatalogDefinition
{
    static constexpr UInt64 SERIALIZE_VERSION = 1;

    String name;
    /// Ordered endpoint names referenced by this shard (`CREATE SHARD ... USING ENDPOINTS (...)`).
    std::vector<String> endpoint_names;
    std::vector<EndpointCatalogDefinition> endpoints;
    UInt32 weight = 1;
    bool internal_replication = false;
    std::vector<String> referenced_by_clusters;

    String serialize() const;
    static ShardCatalogDefinition deserialize(const String & data);
};

/// **Cluster** (`CREATE CLUSTER ... PROPERTIES`):
/// | key | role |
/// |-----|------|
/// | secret | inter-node auth for Distributed |
/// | allow_distributed_ddl_queries | default true in XML docs; enforced when wired to SQL |

/// Parsed body of a SQL `CREATE CLUSTER` row in the cluster catalog.
struct ClusterCatalogDefinition
{
    static constexpr UInt64 SERIALIZE_VERSION = 1;

    std::vector<String> members;
    /// From cluster-level `PROPERTIES secret = ...` (empty if unset / no verification).
    String secret;
    /// From `PROPERTIES allow_distributed_ddl_queries` (default true, matches `remote_servers`).
    bool allow_distributed_ddl_queries = true;

    String serialize() const;
    static ClusterCatalogDefinition deserialize(const String & data);
};

}
