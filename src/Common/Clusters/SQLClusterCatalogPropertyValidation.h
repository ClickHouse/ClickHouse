#pragma once

/// Semantic validation for SQL cluster catalog `PROPERTIES` lists **after** parsing.
///
/// `ParserSQLClusterCatalogProperties.h` only performs **syntax**: `PROPERTIES` + `name = value` lists into `SettingsChanges`.
/// This file applies **meaning** per statement kind (allowed keys, duplicates, required fields, numeric ranges), in line with
/// `remote_servers` concepts:
///
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
///
/// **Shard** (`CREATE SHARD`, `ALTER CLUSTER ... ADD|MODIFY SHARD` shard clause):
/// | key | role |
/// |-----|------|
/// | weight | relative write weight, default 1 |
/// | internal_replication | default false; only `true` / `false`, `0` / `1`, or `'true'` / `'false'` |
///
/// **Cluster** (`CREATE CLUSTER ... PROPERTIES`):
/// | key | role |
/// |-----|------|
/// | secret | inter-node auth for Distributed |
/// | allow_distributed_ddl_queries | default true in XML docs; enforced when wired to SQL |
///
/// Nested `shard` / `replica` layout in XML is expressed in SQL via `CREATE SHARD` / member lists, not as arbitrary keys here.

#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SettingsChanges.h>
#include <Core/Field.h>

#include <cctype>
#include <string_view>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace SQLClusterCatalogPropertyValidationDetail
{
inline void assertNoDuplicatePropertyNames(const SettingsChanges & changes)
{
    std::unordered_set<std::string_view> seen;
    for (const auto & ch : changes)
    {
        if (!seen.insert(std::string_view(ch.name)).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate property `{}` in PROPERTIES", ch.name);
    }
}

inline UInt64 parseUnsignedIntegerPropertyValue(const Field & value, std::string_view property_name)
{
    if (value.getType() == Field::Types::String)
    {
        const auto & text = value.safeGet<String>();
        if (text.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` must not be empty", property_name);

        UInt64 result = 0;
        for (const auto ch : text)
        {
            if (!std::isdigit(static_cast<unsigned char>(ch)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` must be an unsigned integer, got `{}`", property_name, text);
            result = result * 10 + static_cast<UInt64>(ch - '0');
        }
        return result;
    }

    return applyVisitor(FieldVisitorConvertToNumber<UInt64>(), value);
}
}

/// Parse shard-level `internal_replication` only from boolean literals, integer 0 or 1, or string `true` / `false`.
inline void parseShardCatalogInternalReplicationValue(const Field & value, bool & internal_replication)
{
    switch (value.getType())
    {
        case Field::Types::Bool:
            internal_replication = value.safeGet<bool>();
            return;
        case Field::Types::UInt64:
        {
            const auto u = value.safeGet<UInt64>();
            if (u == 0)
            {
                internal_replication = false;
                return;
            }
            if (u == 1)
            {
                internal_replication = true;
                return;
            }
            break;
        }
        case Field::Types::Int64:
        {
            const auto i = value.safeGet<Int64>();
            if (i == 0)
            {
                internal_replication = false;
                return;
            }
            if (i == 1)
            {
                internal_replication = true;
                return;
            }
            break;
        }
        case Field::Types::UInt128:
        {
            const auto u = value.safeGet<UInt128>();
            if (u == 0)
            {
                internal_replication = false;
                return;
            }
            if (u == 1)
            {
                internal_replication = true;
                return;
            }
            break;
        }
        case Field::Types::Int128:
        {
            const auto i = value.safeGet<Int128>();
            if (i == 0)
            {
                internal_replication = false;
                return;
            }
            if (i == 1)
            {
                internal_replication = true;
                return;
            }
            break;
        }
        case Field::Types::String:
        {
            const String & s = value.safeGet<String>();
            if (s == "true")
            {
                internal_replication = true;
                return;
            }
            if (s == "false")
            {
                internal_replication = false;
                return;
            }
            break;
        }
        default:
            break;
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Property `internal_replication` must be boolean true or false, integer 0 or 1, or string literal 'true' or 'false', got {}",
        applyVisitor(FieldVisitorToString(), value));
}

/// **Shard**-level `PROPERTIES` (`CREATE SHARD`, `ALTER SHARD ... MODIFY PROPERTIES`, optional tail on `ALTER SHARD ... REPLACE ...`, `ALTER CLUSTER` shard clauses): `weight`, `internal_replication`.
inline void validateAndExtractShardLevelProperties(
    const SettingsChanges & changes, UInt32 & weight, bool & internal_replication)
{
    SQLClusterCatalogPropertyValidationDetail::assertNoDuplicatePropertyNames(changes);

    weight = 1;
    internal_replication = false;

    for (const auto & ch : changes)
    {
        if (ch.name == "weight")
        {
            weight = static_cast<UInt32>(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), ch.value));
            if (weight == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `weight` in shard-level PROPERTIES must be greater than zero");
        }
        else if (ch.name == "internal_replication")
            parseShardCatalogInternalReplicationValue(ch.value, internal_replication);
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown property `{}` in shard-level PROPERTIES (allowed: weight, internal_replication). "
                "Replica keys (host, port, …) belong in `CREATE REPLICA` / replica-level PROPERTIES.",
                ch.name);
        }
    }
}

/// For `ALTER SHARD ... MODIFY PROPERTIES` (standalone) and for the optional `MODIFY PROPERTIES` tail on `ALTER SHARD ... REPLACE ...`: non-empty assignment list, known keys only (merge semantics in `ClusterFactory::updateShardPropertiesFromSQL` / `replaceShardReplicasFromSQL`).
inline void validateShardLevelPropertyPatchAssignments(const SettingsChanges & changes)
{
    if (changes.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "ALTER SHARD ... MODIFY PROPERTIES requires at least one shard-level assignment (allowed: weight, internal_replication)");

    SQLClusterCatalogPropertyValidationDetail::assertNoDuplicatePropertyNames(changes);

    for (const auto & ch : changes)
    {
        if (ch.name != "weight" && ch.name != "internal_replication")
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown property `{}` in ALTER SHARD ... MODIFY PROPERTIES (allowed: weight, internal_replication)",
                ch.name);
        }
    }
}

/// **Replica**-level `PROPERTIES`: allowed keys only (values validated in `validateReplicaLevelPropertiesForSQLReplica`).
inline void validateReplicaLevelPropertyKeys(const SettingsChanges & changes)
{
    static const std::unordered_set<std::string_view> allowed{
        "host",
        "port",
        "user",
        "password",
        "secure",
        "compression",
        "priority",
        "bind_host",
        "default_database",
    };

    SQLClusterCatalogPropertyValidationDetail::assertNoDuplicatePropertyNames(changes);

    for (const auto & ch : changes)
    {
        if (!allowed.contains(std::string_view(ch.name)))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown property `{}` in replica-level PROPERTIES (allowed: host, port, user, password, secure, compression, priority, bind_host, default_database).",
                ch.name);
        }
    }
}

/// After `validateReplicaLevelPropertyKeys`, require `host` / `port` suitable for a replica endpoint named collection.
inline void validateReplicaLevelPropertiesForSQLReplica(const SettingsChanges & changes)
{
    validateReplicaLevelPropertyKeys(changes);

    const String * host = nullptr;
    const Field * port_value = nullptr;

    for (const auto & ch : changes)
    {
        if (ch.name == "host")
        {
            if (ch.value.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `host` in replica-level PROPERTIES must be a string");
            host = &ch.value.safeGet<String>();
        }
        else if (ch.name == "port")
            port_value = &ch.value;
    }

    if (!host || host->empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica-level PROPERTIES require non-empty `host`");

    if (!port_value)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica-level PROPERTIES require `port`");

    UInt64 port = SQLClusterCatalogPropertyValidationDetail::parseUnsignedIntegerPropertyValue(*port_value, "port");
    if (port == 0 || port > 65535)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica-level PROPERTIES require `port` between 1 and 65535, got {}", port);
}

/// **Cluster**-level `PROPERTIES` (`CREATE CLUSTER ... PROPERTIES`, optional tail on `ALTER CLUSTER ... REPLACE ... MODIFY PROPERTIES`): `secret`, `allow_distributed_ddl_queries`.
/// Callers that only need validation may pass dummy `String` / `bool` out-parameters and ignore them after the call.
inline void validateAndExtractClusterLevelProperties(
    const SettingsChanges & changes, String & secret, bool & allow_distributed_ddl_queries)
{
    SQLClusterCatalogPropertyValidationDetail::assertNoDuplicatePropertyNames(changes);

    secret.clear();
    allow_distributed_ddl_queries = true;

    for (const auto & ch : changes)
    {
        if (ch.name == "secret")
        {
            if (ch.value.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `secret` in cluster-level PROPERTIES must be a string");
            secret = ch.value.safeGet<String>();
        }
        else if (ch.name == "allow_distributed_ddl_queries")
        {
            if (ch.value.getType() == Field::Types::Bool)
                allow_distributed_ddl_queries = ch.value.safeGet<bool>();
            else
                allow_distributed_ddl_queries = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), ch.value) != 0;
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown property `{}` in cluster-level PROPERTIES (allowed: secret, allow_distributed_ddl_queries). "
                "Shard layout uses `CREATE SHARD` / member lists; replica keys belong under `CREATE REPLICA`.",
                ch.name);
        }
    }
}

/// For optional cluster-level `MODIFY PROPERTIES` tail on `ALTER CLUSTER ... REPLACE ...`: non-empty list, known keys only (merge semantics in `ClusterFactory::replaceClusterMembersFromSQL`).
inline void validateClusterLevelPropertyPatchAssignments(const SettingsChanges & changes)
{
    if (changes.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "ALTER CLUSTER ... MODIFY PROPERTIES requires at least one cluster-level assignment (allowed: secret, allow_distributed_ddl_queries)");

    SQLClusterCatalogPropertyValidationDetail::assertNoDuplicatePropertyNames(changes);

    for (const auto & ch : changes)
    {
        if (ch.name != "secret" && ch.name != "allow_distributed_ddl_queries")
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown property `{}` in ALTER CLUSTER ... MODIFY PROPERTIES (allowed: secret, allow_distributed_ddl_queries)",
                ch.name);
        }
    }
}

}
