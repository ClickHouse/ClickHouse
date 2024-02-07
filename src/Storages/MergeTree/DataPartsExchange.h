#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/IStorage_fwd.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/copyData.h>
#include <IO/ConnectionTimeouts.h>
#include <Common/Throttler.h>
#include <Disks/RemoteDiskFeature.h>


namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

class StorageReplicatedMergeTree;
class PooledReadWriteBufferFromHTTP;

namespace DataPartsExchange
{

/** Service for sending parts from the table *ReplicatedMergeTree.
  */
class Service final : public InterserverIOEndpoint
{
public:
    explicit Service(StorageReplicatedMergeTree & data_);

    Service(const Service &) = delete;
    Service & operator=(const Service &) = delete;

    std::string getId(const std::string & node_id) const override;
    void processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response) override;

private:
    MergeTreeData::DataPartPtr findPart(const String & name);
    void sendPartFromMemory(
        const MergeTreeData::DataPartPtr & part,
        WriteBuffer & out,
        bool send_projections);

    MergeTreeData::DataPart::Checksums sendPartFromDisk(
        const MergeTreeData::DataPartPtr & part,
        WriteBuffer & out,
        int client_protocol_version,
        RemoteDiskFeature feature,
        bool send_projections);

    /// StorageReplicatedMergeTree::shutdown() waits for all parts exchange handlers to finish,
    /// so Service will never access dangling reference to storage
    StorageReplicatedMergeTree & data;
    LoggerPtr log;
};

/** Client for getting the parts from the table *MergeTree.
  */
class Fetcher final : private boost::noncopyable
{
public:
    explicit Fetcher(StorageReplicatedMergeTree & data_);

    /// Downloads a part to tmp_directory. If to_detached - downloads to the `detached` directory.
    std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> fetchSelectedPart(
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        const String & part_name,
        const String & zookeeper_name,
        const String & replica_path,
        const String & host,
        int port,
        const ConnectionTimeouts & timeouts,
        const String & user,
        const String & password,
        const String & interserver_scheme,
        ThrottlerPtr throttler,
        bool to_detached = false,
        const String & tmp_prefix_ = "",
        std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr = nullptr,
        // Why not RemoteDiskFeature? Throughout other code, features are distinct --
        // VFS is incompatible with 0copy. However, here we want to check both features
        // so we either need to transform RemoteDiskFeature into a bitset (which is bad
        // as in other places it's not a bitset logically) or use explicit bool flags
        bool try_use_zero_copy = true,
        bool try_use_vfs = true,
        DiskPtr dest_disk = nullptr);

    /// You need to stop the data transfer.
    ActionBlocker blocker;

private:
    using OutputBufferGetter = std::function<std::unique_ptr<WriteBufferFromFileBase>(IDataPartStorage &, const String &, size_t)>;

    void downloadBaseOrProjectionPartToDisk(
        const String & replica_path,
        const MutableDataPartStoragePtr & data_part_storage,
        PooledReadWriteBufferFromHTTP & in,
        OutputBufferGetter output_buffer_getter,
        MergeTreeData::DataPart::Checksums & checksums,
        ThrottlerPtr throttler,
        bool sync) const;

    MergeTreeData::MutableDataPartPtr downloadPartToDisk(
        const String & part_name,
        const String & replica_path,
        bool to_detached,
        const String & tmp_prefix_,
        DiskPtr disk,
        RemoteDiskFeature feature,
        PooledReadWriteBufferFromHTTP & in,
        OutputBufferGetter output_buffer_getter,
        size_t projections,
        ThrottlerPtr throttler,
        bool sync);

    MergeTreeData::MutableDataPartPtr downloadPartToMemory(
       MutableDataPartStoragePtr data_part_storage,
       const String & part_name,
       const MergeTreePartInfo & part_info,
       const UUID & part_uuid,
       const StorageMetadataPtr & metadata_snapshot,
       ContextPtr context,
       PooledReadWriteBufferFromHTTP & in,
       size_t projections,
       bool is_projection,
       ThrottlerPtr throttler);

    MergeTreeData::MutableDataPartPtr downloadPartToDiskRemoteMeta(
       const String & part_name,
       const String & replica_path,
       bool to_detached,
       const String & tmp_prefix_,
       DiskPtr disk,
       PooledReadWriteBufferFromHTTP & in,
       size_t projections,
       MergeTreeData::DataPart::Checksums & checksums,
       ThrottlerPtr throttler);

    StorageReplicatedMergeTree & data;
    LoggerPtr log;
};

}

}
