#include <Common/Clusters/ClusterMetadataMutation.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

ClusterMetadataMutation makeMutation(
    ClusterMetadataMutation::Type type,
    const String & name,
    String definition_data = {})
{
    return ClusterMetadataMutation{
        .type = type,
        .name = name,
        .definition_data = std::move(definition_data),
    };
}

}

ClusterMetadataMutation ClusterMetadataMutation::createEndpoint(const String & name, const EndpointCatalogDefinition & definition)
{
    return makeMutation(Type::CreateEndpoint, name, definition.serialize());
}

ClusterMetadataMutation ClusterMetadataMutation::dropEndpoint(const String & name)
{
    return makeMutation(Type::DropEndpoint, name);
}

ClusterMetadataMutation ClusterMetadataMutation::alterEndpoint(const String & name, const EndpointCatalogDefinition & definition)
{
    return makeMutation(Type::AlterEndpoint, name, definition.serialize());
}

ClusterMetadataMutation ClusterMetadataMutation::createShard(const ShardCatalogDefinition & definition)
{
    return makeMutation(Type::CreateShard, definition.name, definition.serialize());
}

ClusterMetadataMutation ClusterMetadataMutation::dropShard(const String & name)
{
    return makeMutation(Type::DropShard, name);
}

ClusterMetadataMutation ClusterMetadataMutation::alterShard(const ShardCatalogDefinition & definition)
{
    return makeMutation(Type::AlterShard, definition.name, definition.serialize());
}

ClusterMetadataMutation ClusterMetadataMutation::createCluster(const String & name, const ClusterCatalogDefinition & definition)
{
    return makeMutation(Type::CreateCluster, name, definition.serialize());
}

ClusterMetadataMutation ClusterMetadataMutation::dropCluster(const String & name)
{
    return makeMutation(Type::DropCluster, name);
}

ClusterMetadataMutation ClusterMetadataMutation::alterCluster(const String & name, const ClusterCatalogDefinition & definition)
{
    return makeMutation(Type::AlterCluster, name, definition.serialize());
}

String ClusterMetadataMutation::serialize() const
{
    WriteBufferFromOwnString wb;
    writeVarUInt(SERIALIZE_VERSION, wb);
    writeBinary(static_cast<UInt8>(type), wb);
    writeStringBinary(name, wb);
    writeStringBinary(definition_data, wb);
    return wb.str();
}

ClusterMetadataMutation ClusterMetadataMutation::deserialize(const String & data)
{
    ReadBufferFromString rb(data);
    UInt64 version = 0;
    readVarUInt(version, rb);
    if (version != SERIALIZE_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown ClusterMetadataMutation format version {}", version);

    UInt8 type = 0;
    readBinary(type, rb);

    ClusterMetadataMutation mutation;
    mutation.type = static_cast<Type>(type);
    readStringBinary(mutation.name, rb);
    readStringBinary(mutation.definition_data, rb);

    if (!rb.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Trailing data in ClusterMetadataMutation blob");

    switch (mutation.type)
    {
        case Type::CreateEndpoint:
        case Type::DropEndpoint:
        case Type::AlterEndpoint:
        case Type::CreateShard:
        case Type::DropShard:
        case Type::AlterShard:
        case Type::CreateCluster:
        case Type::DropCluster:
        case Type::AlterCluster:
            return mutation;
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown ClusterMetadataMutation type {}", static_cast<UInt64>(type));
}

}
