#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/IStorage_fwd.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/copyData.h>
#include <IO/ConnectionTimeouts.h>
#include <Common/Throttler.h>
#include <Common/ActionBlocker.h>


namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

class StorageReplicatedMergeTree;
class ReadWriteBufferFromHTTP;

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

    MergeTreeData::DataPart::Checksums sendPartFromDisk(
        const MergeTreeData::DataPartPtr & part,
        WriteBuffer & out,
        int client_protocol_version,
        bool from_remote_disk,
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
        bool try_zero_copy = true,
        DiskPtr dest_disk = nullptr);

    /// You need to stop the data transfer.
    ActionBlocker blocker;

private:
    using OutputBufferGetter = std::function<std::unique_ptr<WriteBufferFromFileBase>(IDataPartStorage &, const String &, size_t)>;

    void downloadBaseOrProjectionPartToDisk(
        const String & replica_path,
        const MutableDataPartStoragePtr & data_part_storage,
        ReadWriteBufferFromHTTP & in,
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
        bool to_remote_disk,
        ReadWriteBufferFromHTTP & in,
        OutputBufferGetter output_buffer_getter,
        size_t projections,
        ThrottlerPtr throttler,
        bool sync);

    MergeTreeData::MutableDataPartPtr downloadPartToDiskRemoteMeta(
       const String & part_name,
       const String & replica_path,
       bool to_detached,
       const String & tmp_prefix_,
       DiskPtr disk,
       ReadWriteBufferFromHTTP & in,
       size_t projections,
       MergeTreeData::DataPart::Checksums & checksums,
       ThrottlerPtr throttler);

    StorageReplicatedMergeTree & data;
    LoggerPtr log;
};

}

}
