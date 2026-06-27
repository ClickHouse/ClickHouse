#include <Common/Clusters/ClusterCatalogTypes.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

String EndpointCatalogDefinition::serialize() const
{
    WriteBufferFromOwnString wb;
    writeVarUInt(EndpointCatalogDefinition::SERIALIZE_VERSION, wb);
    writeStringBinary(host, wb);
    writeBinary(port, wb);
    writeStringBinary(user, wb);
    writeStringBinary(password, wb);
    writeStringBinary(default_database, wb);
    writeStringBinary(bind_host, wb);
    writeBinary(secure, wb);
    writeBinary(compression, wb);
    writeBinary(priority, wb);
    return wb.str();
}

EndpointCatalogDefinition EndpointCatalogDefinition::deserialize(const String & data)
{
    ReadBufferFromString rb(data);
    UInt64 version = 0;
    readVarUInt(version, rb);
    if (version != EndpointCatalogDefinition::SERIALIZE_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown EndpointCatalogDefinition format version {}", version);

    EndpointCatalogDefinition r;
    readStringBinary(r.host, rb);
    readBinary(r.port, rb);
    readStringBinary(r.user, rb);
    readStringBinary(r.password, rb);
    readStringBinary(r.default_database, rb);
    readStringBinary(r.bind_host, rb);
    readBinary(r.secure, rb);
    readBinary(r.compression, rb);
    readBinary(r.priority, rb);
    if (!rb.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Trailing data in EndpointCatalogDefinition blob");
    return r;
}

String ShardCatalogDefinition::serialize() const
{
    /// Only normalized (authoritative) fields are persisted. The resolved `endpoints` snapshot is
    /// derived from `endpoint_names` at load time, and `referenced_by_clusters` is a derived reverse
    /// index. Persisting them would make the blob (and therefore the snapshot digest) depend on data
    /// that is never rewritten on `ALTER ENDPOINT`, causing digests to diverge between a node that
    /// applies incrementally and a node that reloads the snapshot from Keeper.
    WriteBufferFromOwnString wb;
    writeVarUInt(ShardCatalogDefinition::SERIALIZE_VERSION, wb);
    writeStringBinary(name, wb);
    writeVectorBinary(endpoint_names, wb);
    writeBinary(weight, wb);
    writeBinary(internal_replication, wb);
    return wb.str();
}

ShardCatalogDefinition ShardCatalogDefinition::deserialize(const String & data)
{
    ReadBufferFromString rb(data);
    UInt64 version = 0;
    readVarUInt(version, rb);
    if (version != ShardCatalogDefinition::SERIALIZE_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown ShardCatalogDefinition format version {}", version);

    ShardCatalogDefinition s;
    readStringBinary(s.name, rb);
    readVectorBinary(s.endpoint_names, rb);
    readBinary(s.weight, rb);
    readBinary(s.internal_replication, rb);
    if (!rb.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Trailing data in ShardCatalogDefinition blob");
    return s;
}

String ClusterCatalogDefinition::serialize() const
{
    WriteBufferFromOwnString wb;
    writeVarUInt(ClusterCatalogDefinition::SERIALIZE_VERSION, wb);
    writeVectorBinary(members, wb);
    writeStringBinary(secret, wb);
    writeBinary(allow_distributed_ddl_queries, wb);
    return wb.str();
}

ClusterCatalogDefinition ClusterCatalogDefinition::deserialize(const String & data)
{
    ReadBufferFromString rb(data);
    UInt64 version = 0;
    readVarUInt(version, rb);
    if (version != ClusterCatalogDefinition::SERIALIZE_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown ClusterCatalogDefinition format version {}", version);

    ClusterCatalogDefinition c;
    readVectorBinary(c.members, rb);
    readStringBinary(c.secret, rb);
    readBinary(c.allow_distributed_ddl_queries, rb);
    if (!rb.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Trailing data in ClusterCatalogDefinition blob");
    return c;
}

}
