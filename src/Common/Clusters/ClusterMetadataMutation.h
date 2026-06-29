#pragma once

#include <Common/Clusters/ClusterCatalogTypes.h>

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
    };

    Type type;
    String name;
    String definition_data;

    static ClusterMetadataMutation createEndpoint(const String & name, const EndpointCatalogDefinition & definition);
    static ClusterMetadataMutation dropEndpoint(const String & name);
    static ClusterMetadataMutation alterEndpoint(const String & name, const EndpointCatalogDefinition & definition);
    static ClusterMetadataMutation createShard(const ShardCatalogDefinition & definition);
    static ClusterMetadataMutation dropShard(const String & name);
    static ClusterMetadataMutation alterShard(const ShardCatalogDefinition & definition);
    static ClusterMetadataMutation createCluster(const String & name, const ClusterCatalogDefinition & definition);
    static ClusterMetadataMutation dropCluster(const String & name);
    static ClusterMetadataMutation alterCluster(const String & name, const ClusterCatalogDefinition & definition);

    String serialize() const;
    static ClusterMetadataMutation deserialize(const String & data);
};

}
