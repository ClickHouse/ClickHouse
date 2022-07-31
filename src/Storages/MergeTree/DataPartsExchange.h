#pragma once

#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/IStorage_fwd.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/copyData.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Common/Throttler.h>


namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

class StorageReplicatedMergeTree;

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
        const std::map<String, std::shared_ptr<IMergeTreeDataPart>> & projections = {});

    MergeTreeData::DataPart::Checksums sendPartFromDisk(
        const MergeTreeData::DataPartPtr & part,
        WriteBuffer & out,
        int client_protocol_version,
        const std::map<String, std::shared_ptr<IMergeTreeDataPart>> & projections = {});

    void sendPartFromDiskRemoteMeta(const MergeTreeData::DataPartPtr & part, WriteBuffer & out);

    /// StorageReplicatedMergeTree::shutdown() waits for all parts exchange handlers to finish,
    /// so Service will never access dangling reference to storage
    StorageReplicatedMergeTree & data;
    Poco::Logger * log;
};

/** Client for getting the parts from the table *MergeTree.
  */
class Fetcher final : private boost::noncopyable
{
public:
    explicit Fetcher(StorageReplicatedMergeTree & data_) : data(data_), log(&Poco::Logger::get("Fetcher")) {}

    /// Downloads a part to tmp_directory. If to_detached - downloads to the `detached` directory.
    MergeTreeData::MutableDataPartPtr fetchPart(
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        const String & part_name,
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
    void downloadBaseOrProjectionPartToDisk(
            const String & replica_path,
            const String & part_download_path,
            bool sync,
            DiskPtr disk,
            PooledReadWriteBufferFromHTTP & in,
            MergeTreeData::DataPart::Checksums & checksums,
            ThrottlerPtr throttler) const;


    MergeTreeData::MutableDataPartPtr downloadPartToDisk(
            const String & part_name,
            const String & replica_path,
            bool to_detached,
            const String & tmp_prefix_,
            bool sync,
            DiskPtr disk,
            PooledReadWriteBufferFromHTTP & in,
            size_t projections,
            MergeTreeData::DataPart::Checksums & checksums,
            ThrottlerPtr throttler);

    MergeTreeData::MutableDataPartPtr downloadPartToMemory(
            const String & part_name,
            const UUID & part_uuid,
            const StorageMetadataPtr & metadata_snapshot,
            ContextPtr context,
            DiskPtr disk,
            PooledReadWriteBufferFromHTTP & in,
            size_t projections,
            ThrottlerPtr throttler);

    MergeTreeData::MutableDataPartPtr downloadPartToDiskRemoteMeta(
            const String & part_name,
            const String & replica_path,
            bool to_detached,
            const String & tmp_prefix_,
            DiskPtr disk,
            PooledReadWriteBufferFromHTTP & in,
            ThrottlerPtr throttler);

    StorageReplicatedMergeTree & data;
    Poco::Logger * log;
};

}

}
