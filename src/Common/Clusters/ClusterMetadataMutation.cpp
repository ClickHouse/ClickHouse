#include <Common/Clusters/ClusterMetadataMutation.h>
#include <Common/Exception.h>
#include <Core/Field.h>
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
    String definition_data = {},
    bool if_exists = false,
    bool if_not_exists = false)
{
    return ClusterMetadataMutation{
        .type = type,
        .name = name,
        .definition_data = std::move(definition_data),
        .if_exists = if_exists,
        .if_not_exists = if_not_exists,
    };
}

String serializeSettingsChanges(const SettingsChanges & properties)
{
    WriteBufferFromOwnString wb;
    writeVarUInt(properties.size(), wb);
    for (const auto & change : properties)
    {
        writeStringBinary(change.name, wb);
        writeFieldBinary(change.value, wb);
    }
    return wb.str();
}

SettingsChanges deserializeSettingsChangesPayload(ReadBuffer & rb)
{
    UInt64 size = 0;
    readVarUInt(size, rb);

    SettingsChanges properties;
    properties.reserve(size);
    for (UInt64 i = 0; i < size; ++i)
    {
        String name;
        readStringBinary(name, rb);
        properties.emplace_back(name, readFieldBinary(rb));
    }
    return properties;
}

String serializeStringList(const std::vector<String> & values)
{
    WriteBufferFromOwnString wb;
    writeVectorBinary(values, wb);
    return wb.str();
}

String serializeReplacements(
    const std::vector<ClusterMetadataMutation::Replacement> & replacements,
    const SettingsChanges & properties)
{
    WriteBufferFromOwnString wb;
    writeVarUInt(replacements.size(), wb);
    for (const auto & replacement : replacements)
    {
        writeStringBinary(replacement.from, wb);
        writeStringBinary(replacement.to, wb);
    }
    writeStringBinary(serializeSettingsChanges(properties), wb);
    return wb.str();
}

}

ClusterMetadataMutation ClusterMetadataMutation::createEndpoint(
    const String & name,
    const EndpointCatalogDefinition & definition,
    bool if_not_exists)
{
    return makeMutation(Type::CreateEndpoint, name, definition.serialize(), /*if_exists=*/false, if_not_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::dropEndpoint(const String & name, bool if_exists)
{
    return makeMutation(Type::DropEndpoint, name, {}, if_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::alterEndpoint(const String & name, const EndpointCatalogDefinition & definition)
{
    return makeMutation(Type::AlterEndpoint, name, definition.serialize());
}

ClusterMetadataMutation ClusterMetadataMutation::createShard(const ShardCatalogDefinition & definition, bool if_not_exists)
{
    return makeMutation(Type::CreateShard, definition.name, definition.serialize(), /*if_exists=*/false, if_not_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::dropShard(const String & name, bool if_exists)
{
    return makeMutation(Type::DropShard, name, {}, if_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::alterShard(const ShardCatalogDefinition & definition)
{
    return makeMutation(Type::AlterShard, definition.name, definition.serialize());
}

ClusterMetadataMutation ClusterMetadataMutation::createCluster(
    const String & name,
    const ClusterCatalogDefinition & definition,
    bool if_not_exists)
{
    return makeMutation(Type::CreateCluster, name, definition.serialize(), /*if_exists=*/false, if_not_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::dropCluster(const String & name, bool if_exists)
{
    return makeMutation(Type::DropCluster, name, {}, if_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::alterCluster(const String & name, const ClusterCatalogDefinition & definition)
{
    return makeMutation(Type::AlterCluster, name, definition.serialize());
}

ClusterMetadataMutation ClusterMetadataMutation::modifyEndpointProperties(const String & name, const SettingsChanges & properties)
{
    return makeMutation(Type::ModifyEndpointProperties, name, serializeSettingsChanges(properties));
}

ClusterMetadataMutation ClusterMetadataMutation::modifyShardProperties(
    const String & name,
    const SettingsChanges & properties,
    bool if_exists)
{
    return makeMutation(Type::ModifyShardProperties, name, serializeSettingsChanges(properties), if_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::addShardReplicas(
    const String & name,
    const std::vector<String> & endpoint_names,
    bool if_exists)
{
    return makeMutation(Type::AddShardReplicas, name, serializeStringList(endpoint_names), if_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::dropShardReplicas(
    const String & name,
    const std::vector<String> & endpoint_names,
    bool if_exists)
{
    return makeMutation(Type::DropShardReplicas, name, serializeStringList(endpoint_names), if_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::replaceShardReplicas(
    const String & name,
    const std::vector<Replacement> & replacements,
    const SettingsChanges & properties,
    bool if_exists)
{
    return makeMutation(Type::ReplaceShardReplicas, name, serializeReplacements(replacements, properties), if_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::addClusterMembers(
    const String & name,
    const std::vector<String> & shard_names,
    bool if_exists)
{
    return makeMutation(Type::AddClusterMembers, name, serializeStringList(shard_names), if_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::dropClusterMembers(
    const String & name,
    const std::vector<String> & shard_names,
    bool if_exists)
{
    return makeMutation(Type::DropClusterMembers, name, serializeStringList(shard_names), if_exists);
}

ClusterMetadataMutation ClusterMetadataMutation::replaceClusterMembers(
    const String & name,
    const std::vector<Replacement> & replacements,
    const SettingsChanges & properties,
    bool if_exists)
{
    return makeMutation(Type::ReplaceClusterMembers, name, serializeReplacements(replacements, properties), if_exists);
}

SettingsChanges ClusterMetadataMutation::deserializeSettingsChanges() const
{
    ReadBufferFromString rb(definition_data);
    auto properties = deserializeSettingsChangesPayload(rb);
    if (!rb.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Trailing data in ClusterMetadataMutation settings payload");
    return properties;
}

std::vector<String> ClusterMetadataMutation::deserializeStringList() const
{
    ReadBufferFromString rb(definition_data);
    std::vector<String> values;
    readVectorBinary(values, rb);
    if (!rb.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Trailing data in ClusterMetadataMutation string list payload");
    return values;
}

std::vector<ClusterMetadataMutation::Replacement> ClusterMetadataMutation::deserializeReplacements(SettingsChanges * properties) const
{
    ReadBufferFromString rb(definition_data);
    UInt64 size = 0;
    readVarUInt(size, rb);

    std::vector<Replacement> replacements;
    replacements.reserve(size);
    for (UInt64 i = 0; i < size; ++i)
    {
        Replacement replacement;
        readStringBinary(replacement.from, rb);
        readStringBinary(replacement.to, rb);
        replacements.push_back(std::move(replacement));
    }

    String properties_data;
    readStringBinary(properties_data, rb);
    if (properties)
    {
        ReadBufferFromString properties_buffer(properties_data);
        *properties = deserializeSettingsChangesPayload(properties_buffer);
        if (!properties_buffer.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Trailing data in ClusterMetadataMutation replacement properties payload");
    }

    if (!rb.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Trailing data in ClusterMetadataMutation replacement payload");
    return replacements;
}

String ClusterMetadataMutation::serialize() const
{
    WriteBufferFromOwnString wb;
    writeVarUInt(SERIALIZE_VERSION, wb);
    writeBinary(static_cast<UInt8>(type), wb);
    writeStringBinary(name, wb);
    writeStringBinary(definition_data, wb);
    writeBinary(static_cast<UInt8>(if_exists ? 1 : 0), wb);
    writeBinary(static_cast<UInt8>(if_not_exists ? 1 : 0), wb);
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
    UInt8 if_exists = 0;
    readBinary(if_exists, rb);
    mutation.if_exists = if_exists != 0;
    UInt8 if_not_exists = 0;
    readBinary(if_not_exists, rb);
    mutation.if_not_exists = if_not_exists != 0;

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
        case Type::ModifyEndpointProperties:
        case Type::ModifyShardProperties:
        case Type::AddShardReplicas:
        case Type::DropShardReplicas:
        case Type::ReplaceShardReplicas:
        case Type::AddClusterMembers:
        case Type::DropClusterMembers:
        case Type::ReplaceClusterMembers:
            return mutation;
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown ClusterMetadataMutation type {}", static_cast<UInt64>(type));
}

}
