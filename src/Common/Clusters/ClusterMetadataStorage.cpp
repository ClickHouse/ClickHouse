#include <Common/Clusters/ClusterMetadataStorage.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#if USE_SSL
#    include <IO/FileEncryptionCommon.h>
#    include <boost/algorithm/hex.hpp>
#endif

#include <algorithm>
#include <filesystem>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int BAD_CLUSTER_DEFINITION;
    extern const int LOGICAL_ERROR;
    extern const int SHARD_DOESNT_EXIST;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

constexpr std::string_view FORMAT_VERSION = "1";

String joinPath(const String & left, const String & right)
{
    return (fs::path(left) / right).string();
}

void validateEntityName(const String & name, std::string_view entity)
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} name cannot be empty", entity);
    if (name.find('/') != String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} name cannot contain '/': `{}`", entity, name);
}

template <typename Map, typename SerializeFunc>
void updateHashWithMap(SipHash & hash, std::string_view kind, const Map & map, SerializeFunc serialize)
{
    std::vector<String> names;
    names.reserve(map.size());
    for (const auto & [name, _] : map)
        names.push_back(name);
    std::sort(names.begin(), names.end());

    hash.update(kind.data(), kind.size());
    for (const auto & name : names)
    {
        const auto data = serialize(map.at(name));
        hash.update(name.data(), name.size());
        hash.update(data.data(), data.size());
    }
}

}

ClusterMetadataStorage::ClusterMetadataStorage(
    zkutil::ZooKeeperPtr zookeeper_,
    String replica_group_root_,
    bool encrypted_,
    String encryption_key_hex_,
    String encryption_algorithm_)
    : zookeeper(std::move(zookeeper_))
    , replica_group_root(std::move(replica_group_root_))
    , encrypted(encrypted_)
    , encryption_algorithm(std::move(encryption_algorithm_))
{
#if USE_SSL
    if (encrypted)
    {
        try
        {
            encryption_key = boost::algorithm::unhex(encryption_key_hex_);
            encryption_key_fingerprint = FileEncryption::calculateKeyFingerprint(encryption_key);
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read `cluster_metadata.key_hex`, check for valid characters [0-9a-fA-F] and length");
        }
    }
#else
    (void)encryption_key_hex_;
    (void)encryption_key_fingerprint;
    if (encrypted)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cluster metadata encryption requires building with SSL support");
#endif

    if (!zookeeper)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataStorage requires non-null ZooKeeper pointer");
    if (replica_group_root.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cluster metadata replica group root cannot be empty");
    if (replica_group_root.front() != '/')
        replica_group_root = "/" + replica_group_root;
    while (replica_group_root.size() > 1 && replica_group_root.ends_with('/'))
        replica_group_root.pop_back();
}

void ClusterMetadataStorage::initLayout()
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataStorage::initLayout");
    zookeeper->createAncestors(replica_group_root);
    zookeeper->createIfNotExists(replica_group_root, "");
    zookeeper->createIfNotExists(joinPath(replica_group_root, "format_version"), String(FORMAT_VERSION));
    zookeeper->createIfNotExists(joinPath(replica_group_root, "replicas"), "");
    zookeeper->createIfNotExists(metadataRoot(), "");
    zookeeper->createIfNotExists(endpointsRoot(), "");
    zookeeper->createIfNotExists(shardsRoot(), "");
    zookeeper->createIfNotExists(clustersRoot(), "");
    zookeeper->createIfNotExists(refsRoot(), "");
    zookeeper->createIfNotExists(endpointRefsRoot(), "");
    zookeeper->createIfNotExists(shardRefsRoot(), "");
    zookeeper->createIfNotExists(snapshotDigestPath(), encodeData(""));
    rebuildReferenceIndex();
}

bool ClusterMetadataStorage::metadataInitialized() const
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataStorage::metadataInitialized");
    return zookeeper->exists(metadataRoot());
}

ClusterMetadataStorage::Snapshot ClusterMetadataStorage::readSnapshot() const
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataStorage::readSnapshot");
    Snapshot snapshot;
    for (const auto & name : listEndpointNames())
        snapshot.endpoints.emplace(name, readEndpoint(name));
    for (const auto & name : listShardNames())
        snapshot.shards.emplace(name, readShard(name));
    for (const auto & name : listClusterNames())
        snapshot.clusters.emplace(name, readCluster(name));
    snapshot.digest = calculateDigest(snapshot);
    return snapshot;
}

String ClusterMetadataStorage::calculateDigest(const Snapshot & snapshot) const
{
    SipHash hash;
    updateHashWithMap(hash, "endpoints", snapshot.endpoints, [](const auto & value) { return value.serialize(); });
    updateHashWithMap(hash, "shards", snapshot.shards, [](const auto & value) { return value.serialize(); });
    updateHashWithMap(hash, "clusters", snapshot.clusters, [](const auto & value) { return value.serialize(); });
    return getSipHash128AsHexString(hash);
}

String ClusterMetadataStorage::readSnapshotDigest() const
{
    return readData(snapshotDigestPath());
}

void ClusterMetadataStorage::writeSnapshotDigest(const String & digest)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataStorage::writeSnapshotDigest");
    createOrUpdateData(snapshotDigestPath(), digest);
}

void ClusterMetadataStorage::appendWriteSnapshotDigestOps(Coordination::Requests & ops, const String & digest) const
{
    ops.emplace_back(zkutil::makeSetRequest(snapshotDigestPath(), encodeData(digest), -1));
}

void ClusterMetadataStorage::appendCreateEndpointOps(
    Coordination::Requests & ops,
    const String & name,
    const EndpointCatalogDefinition & definition) const
{
    validateEntityName(name, "Endpoint");
    ops.emplace_back(zkutil::makeCreateRequest(endpointPath(name), encodeData(definition.serialize()), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(endpointRefsPath(name), "", zkutil::CreateMode::Persistent));
}

void ClusterMetadataStorage::appendDropEndpointOps(Coordination::Requests & ops, const String & name) const
{
    validateEntityName(name, "Endpoint");
    ops.emplace_back(zkutil::makeRemoveRequest(endpointPath(name), -1));
    /// Removing the refs directory is the atomic "no shards reference this endpoint" check:
    /// Keeper rejects non-empty znode removal.
    ops.emplace_back(zkutil::makeRemoveRequest(endpointRefsPath(name), -1));
}

void ClusterMetadataStorage::appendUpsertEndpointOps(
    Coordination::Requests & ops,
    const String & name,
    const EndpointCatalogDefinition & definition) const
{
    validateEntityName(name, "Endpoint");
    ops.emplace_back(zkutil::makeCheckRequest(endpointPath(name), -1));
    ops.emplace_back(zkutil::makeSetRequest(endpointPath(name), encodeData(definition.serialize()), -1));
}

EndpointCatalogDefinition ClusterMetadataStorage::readEndpoint(const String & name) const
{
    validateEntityName(name, "Endpoint");
    return EndpointCatalogDefinition::deserialize(readData(endpointPath(name)));
}

bool ClusterMetadataStorage::endpointExists(const String & name) const
{
    validateEntityName(name, "Endpoint");
    return zookeeper->exists(endpointPath(name));
}

std::vector<String> ClusterMetadataStorage::listEndpointNames() const
{
    return zookeeper->getChildren(endpointsRoot());
}

void ClusterMetadataStorage::appendCreateShardOps(
    Coordination::Requests & ops,
    const String & name,
    const ShardCatalogDefinition & definition) const
{
    validateEntityName(name, "Shard");
    for (const auto & endpoint_name : definition.endpoint_names)
    {
        validateEntityName(endpoint_name, "Endpoint");
        ops.emplace_back(zkutil::makeCheckRequest(endpointPath(endpoint_name), -1));
    }
    ops.emplace_back(zkutil::makeCreateRequest(shardPath(name), encodeData(definition.serialize()), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(shardRefsPath(name), "", zkutil::CreateMode::Persistent));
    for (const auto & endpoint_name : definition.endpoint_names)
        ops.emplace_back(zkutil::makeCreateRequest(endpointShardRefPath(endpoint_name, name), "", zkutil::CreateMode::Persistent));
}

void ClusterMetadataStorage::appendUpsertShardOps(
    Coordination::Requests & ops,
    const String & name,
    const ShardCatalogDefinition & definition) const
{
    validateEntityName(name, "Shard");
    Coordination::Stat current_stat;
    const auto current = readShard(name, &current_stat);

    std::unordered_set<String> current_endpoints(current.endpoint_names.begin(), current.endpoint_names.end());
    std::unordered_set<String> new_endpoints(definition.endpoint_names.begin(), definition.endpoint_names.end());
    for (const auto & endpoint_name : definition.endpoint_names)
    {
        validateEntityName(endpoint_name, "Endpoint");
        ops.emplace_back(zkutil::makeCheckRequest(endpointPath(endpoint_name), -1));
    }

    ops.emplace_back(zkutil::makeCheckRequest(shardPath(name), current_stat.version));
    ops.emplace_back(zkutil::makeSetRequest(shardPath(name), encodeData(definition.serialize()), -1));
    for (const auto & endpoint_name : current.endpoint_names)
    {
        if (!new_endpoints.contains(endpoint_name))
            ops.emplace_back(zkutil::makeRemoveRequest(endpointShardRefPath(endpoint_name, name), -1));
    }
    for (const auto & endpoint_name : definition.endpoint_names)
    {
        if (!current_endpoints.contains(endpoint_name))
            ops.emplace_back(zkutil::makeCreateRequest(endpointShardRefPath(endpoint_name, name), "", zkutil::CreateMode::Persistent));
    }
}

void ClusterMetadataStorage::appendDropShardOps(Coordination::Requests & ops, const String & name) const
{
    validateEntityName(name, "Shard");
    Coordination::Stat current_stat;
    const auto current = readShard(name, &current_stat);
    ops.emplace_back(zkutil::makeCheckRequest(shardPath(name), current_stat.version));
    ops.emplace_back(zkutil::makeRemoveRequest(shardPath(name), -1));
    /// Removing the refs directory is the atomic "no clusters reference this shard" check.
    ops.emplace_back(zkutil::makeRemoveRequest(shardRefsPath(name), -1));
    for (const auto & endpoint_name : current.endpoint_names)
        ops.emplace_back(zkutil::makeRemoveRequest(endpointShardRefPath(endpoint_name, name), -1));
}

ShardCatalogDefinition ClusterMetadataStorage::readShard(const String & name) const
{
    return readShard(name, nullptr);
}

ShardCatalogDefinition ClusterMetadataStorage::readShard(const String & name, Coordination::Stat * stat) const
{
    validateEntityName(name, "Shard");
    auto shard = ShardCatalogDefinition::deserialize(decodeData(zookeeper->get(shardPath(name), stat)));
    if (shard.name.empty())
        shard.name = name;
    return shard;
}

bool ClusterMetadataStorage::shardExists(const String & name) const
{
    validateEntityName(name, "Shard");
    return zookeeper->exists(shardPath(name));
}

std::vector<String> ClusterMetadataStorage::listShardNames() const
{
    return zookeeper->getChildren(shardsRoot());
}

void ClusterMetadataStorage::appendCreateClusterOps(
    Coordination::Requests & ops,
    const String & name,
    const ClusterCatalogDefinition & definition) const
{
    validateEntityName(name, "Cluster");
    for (const auto & shard_name : definition.members)
    {
        validateEntityName(shard_name, "Shard");
        ops.emplace_back(zkutil::makeCheckRequest(shardPath(shard_name), -1));
    }
    ops.emplace_back(zkutil::makeCreateRequest(clusterPath(name), encodeData(definition.serialize()), zkutil::CreateMode::Persistent));
    for (const auto & shard_name : definition.members)
        ops.emplace_back(zkutil::makeCreateRequest(shardClusterRefPath(shard_name, name), "", zkutil::CreateMode::Persistent));
}

void ClusterMetadataStorage::appendUpsertClusterOps(
    Coordination::Requests & ops,
    const String & name,
    const ClusterCatalogDefinition & definition) const
{
    validateEntityName(name, "Cluster");
    Coordination::Stat current_stat;
    const auto current = readCluster(name, &current_stat);

    std::unordered_set<String> current_members(current.members.begin(), current.members.end());
    std::unordered_set<String> new_members(definition.members.begin(), definition.members.end());
    for (const auto & shard_name : definition.members)
    {
        validateEntityName(shard_name, "Shard");
        ops.emplace_back(zkutil::makeCheckRequest(shardPath(shard_name), -1));
    }

    ops.emplace_back(zkutil::makeCheckRequest(clusterPath(name), current_stat.version));
    ops.emplace_back(zkutil::makeSetRequest(clusterPath(name), encodeData(definition.serialize()), -1));
    for (const auto & shard_name : current.members)
    {
        if (!new_members.contains(shard_name))
            ops.emplace_back(zkutil::makeRemoveRequest(shardClusterRefPath(shard_name, name), -1));
    }
    for (const auto & shard_name : definition.members)
    {
        if (!current_members.contains(shard_name))
            ops.emplace_back(zkutil::makeCreateRequest(shardClusterRefPath(shard_name, name), "", zkutil::CreateMode::Persistent));
    }
}

void ClusterMetadataStorage::appendDropClusterOps(Coordination::Requests & ops, const String & name) const
{
    validateEntityName(name, "Cluster");
    Coordination::Stat current_stat;
    const auto current = readCluster(name, &current_stat);
    ops.emplace_back(zkutil::makeCheckRequest(clusterPath(name), current_stat.version));
    ops.emplace_back(zkutil::makeRemoveRequest(clusterPath(name), -1));
    for (const auto & shard_name : current.members)
        ops.emplace_back(zkutil::makeRemoveRequest(shardClusterRefPath(shard_name, name), -1));
}

ClusterCatalogDefinition ClusterMetadataStorage::readCluster(const String & name) const
{
    return readCluster(name, nullptr);
}

ClusterCatalogDefinition ClusterMetadataStorage::readCluster(const String & name, Coordination::Stat * stat) const
{
    validateEntityName(name, "Cluster");
    return ClusterCatalogDefinition::deserialize(decodeData(zookeeper->get(clusterPath(name), stat)));
}

bool ClusterMetadataStorage::clusterExists(const String & name) const
{
    validateEntityName(name, "Cluster");
    return zookeeper->exists(clusterPath(name));
}

std::vector<String> ClusterMetadataStorage::listClusterNames() const
{
    return zookeeper->getChildren(clustersRoot());
}

void ClusterMetadataStorage::validateEndpointsExist(const std::vector<String> & endpoint_names) const
{
    std::vector<String> paths;
    paths.reserve(endpoint_names.size());
    for (const auto & name : endpoint_names)
    {
        validateEntityName(name, "Endpoint");
        paths.push_back(endpointPath(name));
    }

    auto responses = zookeeper->exists(paths);
    for (size_t i = 0; i < endpoint_names.size(); ++i)
    {
        if (responses[i].error != Coordination::Error::ZOK)
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", endpoint_names[i]);
    }
}

void ClusterMetadataStorage::validateShardsExist(const std::vector<String> & shard_names) const
{
    std::vector<String> paths;
    paths.reserve(shard_names.size());
    for (const auto & name : shard_names)
    {
        validateEntityName(name, "Shard");
        paths.push_back(shardPath(name));
    }

    auto responses = zookeeper->exists(paths);
    for (size_t i = 0; i < shard_names.size(); ++i)
    {
        if (responses[i].error != Coordination::Error::ZOK)
            throw Exception(ErrorCodes::SHARD_DOESNT_EXIST, "Cluster metadata shard `{}` does not exist", shard_names[i]);
    }
}

String ClusterMetadataStorage::metadataRoot() const
{
    return joinPath(replica_group_root, "metadata");
}

String ClusterMetadataStorage::endpointsRoot() const
{
    return joinPath(metadataRoot(), "endpoints");
}

String ClusterMetadataStorage::shardsRoot() const
{
    return joinPath(metadataRoot(), "shards");
}

String ClusterMetadataStorage::clustersRoot() const
{
    return joinPath(metadataRoot(), "clusters");
}

String ClusterMetadataStorage::refsRoot() const
{
    return joinPath(metadataRoot(), "refs");
}

String ClusterMetadataStorage::endpointRefsRoot() const
{
    return joinPath(refsRoot(), "endpoints");
}

String ClusterMetadataStorage::shardRefsRoot() const
{
    return joinPath(refsRoot(), "shards");
}

String ClusterMetadataStorage::snapshotDigestPath() const
{
    return joinPath(metadataRoot(), "snapshot_digest");
}

String ClusterMetadataStorage::endpointPath(const String & name) const
{
    return joinPath(endpointsRoot(), name);
}

String ClusterMetadataStorage::shardPath(const String & name) const
{
    return joinPath(shardsRoot(), name);
}

String ClusterMetadataStorage::clusterPath(const String & name) const
{
    return joinPath(clustersRoot(), name);
}

String ClusterMetadataStorage::endpointRefsPath(const String & endpoint_name) const
{
    return joinPath(endpointRefsRoot(), endpoint_name);
}

String ClusterMetadataStorage::endpointShardRefPath(const String & endpoint_name, const String & shard_name) const
{
    return joinPath(endpointRefsPath(endpoint_name), shard_name);
}

String ClusterMetadataStorage::shardRefsPath(const String & shard_name) const
{
    return joinPath(shardRefsRoot(), shard_name);
}

String ClusterMetadataStorage::shardClusterRefPath(const String & shard_name, const String & cluster_name) const
{
    return joinPath(shardRefsPath(shard_name), cluster_name);
}

String ClusterMetadataStorage::readData(const String & path) const
{
    return decodeData(zookeeper->get(path));
}

void ClusterMetadataStorage::createOrUpdateData(const String & path, const String & data)
{
    zookeeper->createOrUpdate(path, encodeData(data), zkutil::CreateMode::Persistent);
}

void ClusterMetadataStorage::rebuildReferenceIndex() const
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataStorage::rebuildReferenceIndex");

    for (const auto & endpoint_name : listEndpointNames())
    {
        validateEntityName(endpoint_name, "Endpoint");
        zookeeper->createIfNotExists(endpointRefsPath(endpoint_name), "");
    }

    for (const auto & shard_name : listShardNames())
    {
        validateEntityName(shard_name, "Shard");
        const auto shard = readShard(shard_name);
        zookeeper->createIfNotExists(shardRefsPath(shard_name), "");
        for (const auto & endpoint_name : shard.endpoint_names)
        {
            validateEntityName(endpoint_name, "Endpoint");
            zookeeper->createIfNotExists(endpointShardRefPath(endpoint_name, shard_name), "");
        }
    }

    for (const auto & cluster_name : listClusterNames())
    {
        validateEntityName(cluster_name, "Cluster");
        const auto cluster = readCluster(cluster_name);
        for (const auto & shard_name : cluster.members)
        {
            validateEntityName(shard_name, "Shard");
            zookeeper->createIfNotExists(shardClusterRefPath(shard_name, cluster_name), "");
        }
    }
}

String ClusterMetadataStorage::encodeData(const String & data) const
{
    if (!encrypted)
        return data;

#if USE_SSL
    FileEncryption::Header header{
        .algorithm = FileEncryption::parseAlgorithmFromString(encryption_algorithm.empty() ? "aes_128_ctr" : encryption_algorithm),
        .key_fingerprint = encryption_key_fingerprint,
        .init_vector = FileEncryption::InitVector::random()
    };

    FileEncryption::Encryptor encryptor(header.algorithm, encryption_key, header.init_vector);
    WriteBufferFromOwnString out;
    header.write(out);
    encryptor.encrypt(data.data(), data.size(), out);
    return out.str();
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cluster metadata encryption requires building with SSL support");
#endif
}

String ClusterMetadataStorage::decodeData(const String & data) const
{
    if (!encrypted || data.empty())
        return data;

#if USE_SSL
    ReadBufferFromString in(data);

    FileEncryption::Header header;
    try
    {
        header.read(in);
    }
    catch (Exception & e)
    {
        e.addMessage("While reading the header of encrypted cluster metadata");
        throw;
    }

    String encrypted_buffer;
    encrypted_buffer.resize(in.available());
    size_t bytes_read = 0;
    while (bytes_read < encrypted_buffer.size() && !in.eof())
        bytes_read += in.read(encrypted_buffer.data() + bytes_read, encrypted_buffer.size() - bytes_read);

    String decrypted_buffer;
    decrypted_buffer.resize(bytes_read);
    FileEncryption::Encryptor encryptor(header.algorithm, encryption_key, header.init_vector);
    encryptor.decrypt(encrypted_buffer.data(), bytes_read, decrypted_buffer.data());

    return decrypted_buffer;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cluster metadata encryption requires building with SSL support");
#endif
}

}
