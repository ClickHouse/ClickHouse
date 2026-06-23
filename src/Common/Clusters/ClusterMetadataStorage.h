#pragma once

#include <Common/Clusters/ClusterCatalogTypes.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <memory>
#include <unordered_map>
#include <vector>

namespace DB
{

/// Keeper-backed persistence for one resolved cluster metadata replica group.
///
/// `replica_group_root` is the full root of a single group, for example
/// `/clickhouse/clusters_metadata/group_b`.
class ClusterMetadataStorage
{
public:
    struct Snapshot
    {
        std::unordered_map<String, EndpointCatalogDefinition> endpoints;
        std::unordered_map<String, ShardCatalogDefinition> shards;
        std::unordered_map<String, ClusterCatalogDefinition> clusters;
        String digest;
    };

    explicit ClusterMetadataStorage(
        zkutil::ZooKeeperPtr zookeeper_,
        String replica_group_root_,
        bool encrypted_ = false,
        String encryption_key_hex_ = {},
        String encryption_algorithm_ = {});

    void initLayout();

    /// Whether this replica group's metadata tree has been initialized in Keeper. Used by read-only
    /// importers to skip groups that do not exist yet (importers must never create their layout).
    bool metadataInitialized() const;

    Snapshot readSnapshot() const;
    String calculateDigest(const Snapshot & snapshot) const;
    String readSnapshotDigest() const;
    void writeSnapshotDigest(const String & digest);

    void appendCreateEndpointOps(Coordination::Requests & ops, const String & name, const EndpointCatalogDefinition & definition) const;
    void appendUpsertEndpointOps(Coordination::Requests & ops, const String & name, const EndpointCatalogDefinition & definition) const;
    void appendDropEndpointOps(Coordination::Requests & ops, const String & name) const;
    EndpointCatalogDefinition readEndpoint(const String & name) const;
    bool endpointExists(const String & name) const;
    std::vector<String> listEndpointNames() const;

    void appendCreateShardOps(Coordination::Requests & ops, const String & name, const ShardCatalogDefinition & definition) const;
    void appendUpsertShardOps(Coordination::Requests & ops, const String & name, const ShardCatalogDefinition & definition) const;
    void appendDropShardOps(Coordination::Requests & ops, const String & name) const;
    ShardCatalogDefinition readShard(const String & name) const;
    bool shardExists(const String & name) const;
    std::vector<String> listShardNames() const;

    void appendCreateClusterOps(Coordination::Requests & ops, const String & name, const ClusterCatalogDefinition & definition) const;
    void appendUpsertClusterOps(Coordination::Requests & ops, const String & name, const ClusterCatalogDefinition & definition) const;
    void appendDropClusterOps(Coordination::Requests & ops, const String & name) const;
    ClusterCatalogDefinition readCluster(const String & name) const;
    bool clusterExists(const String & name) const;
    std::vector<String> listClusterNames() const;

    void validateEndpointsExist(const std::vector<String> & endpoint_names) const;
    void validateShardsExist(const std::vector<String> & shard_names) const;

    const String & getReplicaGroupRoot() const { return replica_group_root; }
    String getMetadataRoot() const;
    String encodePayloadForKeeper(const String & data) const;
    String decodePayloadFromKeeper(const String & data) const;

private:
    zkutil::ZooKeeperPtr zookeeper;
    String replica_group_root;
    bool encrypted = false;
    String encryption_key;
    String encryption_algorithm;
    UInt128 encryption_key_fingerprint{};

    String metadataRoot() const;
    String endpointsRoot() const;
    String shardsRoot() const;
    String clustersRoot() const;
    String snapshotDigestPath() const;

    String endpointPath(const String & name) const;
    String shardPath(const String & name) const;
    String clusterPath(const String & name) const;

    String readData(const String & path) const;
    void createOrUpdateData(const String & path, const String & data);

    String encodeData(const String & data) const;
    String decodeData(const String & data) const;
};

using ClusterMetadataStoragePtr = std::shared_ptr<ClusterMetadataStorage>;

}
