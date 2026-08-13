#pragma once

#include <Common/Clusters/ClusterCatalogTypes.h>
#include <Common/SettingsChanges.h>

namespace DB
{

struct ClusterMetadataMutation
{
    static constexpr UInt64 SERIALIZE_VERSION = 1;

    enum class Type : UInt8
    {
        CreateEndpoint,
        DropEndpoint,
        AlterEndpoint,
        CreateShard,
        DropShard,
        AlterShard,
        CreateCluster,
        DropCluster,
        AlterCluster,
        ModifyEndpointProperties,
        ModifyShardProperties,
        AddShardReplicas,
        DropShardReplicas,
        ReplaceShardReplicas,
        AddClusterMembers,
        DropClusterMembers,
        ReplaceClusterMembers,
    };

    struct Replacement
    {
        String from;
        String to;
    };

    Type type = {};
    String name;
    String definition_data;
    /// When true, prepare/apply treat a missing target object as a successful no-op.
    bool if_exists = false;
    /// When true, prepare treats an already-existing target as a successful no-op (CREATE IF NOT EXISTS).
    bool if_not_exists = false;

    static ClusterMetadataMutation createEndpoint(
        const String & name,
        const EndpointCatalogDefinition & definition,
        bool if_not_exists = false);
    static ClusterMetadataMutation dropEndpoint(const String & name, bool if_exists = false);
    static ClusterMetadataMutation alterEndpoint(const String & name, const EndpointCatalogDefinition & definition);
    static ClusterMetadataMutation createShard(const ShardCatalogDefinition & definition, bool if_not_exists = false);
    static ClusterMetadataMutation dropShard(const String & name, bool if_exists = false);
    static ClusterMetadataMutation alterShard(const ShardCatalogDefinition & definition);
    static ClusterMetadataMutation createCluster(
        const String & name,
        const ClusterCatalogDefinition & definition,
        bool if_not_exists = false);
    static ClusterMetadataMutation dropCluster(const String & name, bool if_exists = false);
    static ClusterMetadataMutation alterCluster(const String & name, const ClusterCatalogDefinition & definition);
    static ClusterMetadataMutation modifyEndpointProperties(const String & name, const SettingsChanges & properties);
    static ClusterMetadataMutation modifyShardProperties(
        const String & name,
        const SettingsChanges & properties,
        bool if_exists = false);
    static ClusterMetadataMutation addShardReplicas(
        const String & name,
        const std::vector<String> & endpoint_names,
        bool if_exists = false);
    static ClusterMetadataMutation dropShardReplicas(
        const String & name,
        const std::vector<String> & endpoint_names,
        bool if_exists = false);
    static ClusterMetadataMutation replaceShardReplicas(
        const String & name,
        const std::vector<Replacement> & replacements,
        const SettingsChanges & properties,
        bool if_exists = false);
    static ClusterMetadataMutation addClusterMembers(
        const String & name,
        const std::vector<String> & shard_names,
        bool if_exists = false);
    static ClusterMetadataMutation dropClusterMembers(
        const String & name,
        const std::vector<String> & shard_names,
        bool if_exists = false);
    static ClusterMetadataMutation replaceClusterMembers(
        const String & name,
        const std::vector<Replacement> & replacements,
        const SettingsChanges & properties,
        bool if_exists = false);

    SettingsChanges deserializeSettingsChanges() const;
    std::vector<String> deserializeStringList() const;
    std::vector<Replacement> deserializeReplacements(SettingsChanges * properties = nullptr) const;

    String serialize() const;
    static ClusterMetadataMutation deserialize(const String & data);
};

}
