#pragma once

#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/IStorage_fwd.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/copyData.h>
#include <IO/ConnectionTimeouts.h>
#include <Common/Throttler.h>
#include <Common/ActionBlocker.h>
#include <IO/ReadBuffer.h>


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
    void processQuery(const HTMLForm & params, ReadBufferPtr body, WriteBuffer & out, HTTPServerResponse & response) override;

private:
    MergeTreeData::DataPartPtr findPart(const String & name);

    MergeTreeData::DataPart::Checksums sendPartFromDisk(
        const MergeTreeData::DataPartPtr & part,
        const HTMLForm & params,
        WriteBuffer & out,
        HTTPServerResponse & response,
        int client_protocol_version,
        bool send_projections);

    bool checkRemoteFsMetadataCapabilities(
        const HTMLForm & params,
        HTTPServerResponse & response,
        MergeTreeData::DataPartPtr part,
        int client_protocol_version);

    /// Ephemeral zero-copy lock may be lost for PreActive parts
    /// do not expose PreActive parts for zero-copy
    void waitZeroCopyPreActivePart(MergeTreeData::DataPartPtr part);

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
        ReadWriteBufferFromHTTP & in,
        OutputBufferGetter output_buffer_getter,
        size_t projections,
        ThrottlerPtr throttler,
        bool sync);

    MergeTreeData::MutableDataPartPtr downloadPartToDiskImpl(
        const String & part_name,
        const String & replica_path,
        bool to_detached,
        const String & tmp_prefix,
        MergeTreeData::DataPart::Checksums & data_checksums,
        DiskPtr disk,
        ReadWriteBufferFromHTTP & in,
        OutputBufferGetter output_buffer_getter,
        size_t projections,
        ThrottlerPtr throttler,
        bool sync,
        std::function<void (DataPartStorageOnDiskBase &)> clear_storage_if_exists);

    Strings getZeroCopyDiskCapability(const String & part_name, Poco::URI & uri, DiskPtr disk, bool try_zero_copy);

    MergeTreeData::MutableDataPartPtr tryFetchZeroCopy(
        const Strings & capability,
        const String & remote_fs_metadata,
        const String & part_name,
        const String & replica_path,
        bool to_detached,
        const String & tmp_prefix,
        DiskPtr disk,
        ReadWriteBufferFromHTTP & in,
        size_t projections,
        ThrottlerPtr throttler,
        int server_protocol_version,
        bool sync);

    MergeTreeData::MutableDataPartPtr downloadPartToRemoteDisk(
        const String & part_name,
        const String & replica_path,
        bool to_detached,
        const String & tmp_prefix_,
        DiskPtr disk,
        ReadWriteBufferFromHTTP & in,
        OutputBufferGetter output_buffer_getter,
        size_t projections,
        ThrottlerPtr throttler,
        bool sync);

    StorageReplicatedMergeTree & data;
    LoggerPtr log;
};

}

}
