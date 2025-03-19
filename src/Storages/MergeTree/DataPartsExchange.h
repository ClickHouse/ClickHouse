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
class Service : public InterserverIOEndpoint
{
public:
    explicit Service(StorageReplicatedMergeTree & data_);

    Service(const Service &) = delete;
    Service & operator=(const Service &) = delete;

    std::string getId(const std::string & node_id) const override;
    void processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response) override;

protected:
    MergeTreeData::DataPartPtr findPart(const String & name);

    virtual MergeTreeData::DataPart::Checksums sendPartFromDisk(
        const MergeTreeData::DataPartPtr & part,
        WriteBuffer & out,
        int client_protocol_version,
        bool send_projections);

    /// StorageReplicatedMergeTree::shutdown() waits for all parts exchange handlers to finish,
    /// so Service will never access dangling reference to storage
    StorageReplicatedMergeTree & data;
    LoggerPtr log;
};

class ServiceZeroCopy : public Service
{
public:
    explicit ServiceZeroCopy(StorageReplicatedMergeTree & data_);

    ServiceZeroCopy(const Service &) = delete;
    ServiceZeroCopy & operator=(const Service &) = delete;

    void processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response) override;

protected:
    MergeTreeData::DataPartPtr findPart(const String & name);

    MergeTreeData::DataPart::Checksums sendPartFromDisk(
        const MergeTreeData::DataPartPtr & part,
        WriteBuffer & out,
        int client_protocol_version,
        bool send_projections) override;
};

/** Client for getting the parts from the table *MergeTree.
  */
class Fetcher : private boost::noncopyable
{
public:
    explicit Fetcher(StorageReplicatedMergeTree & data_);

    /// Downloads a part to tmp_directory. If to_detached - downloads to the `detached` directory.
    virtual std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> fetchSelectedPart(
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
        bool to_detached,
        const String & tmp_prefix_,
        std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
        DiskPtr dest_disk);

    /// You need to stop the data transfer.
    ActionBlocker blocker;

    virtual ~Fetcher() = default;

protected:
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

    StorageReplicatedMergeTree & data;
    LoggerPtr log;

};

class FetcherZeroCopy : Fetcher
{
public:
    explicit FetcherZeroCopy(StorageReplicatedMergeTree & data_);

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
        bool to_detached,
        const String & tmp_prefix_,
        std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
        DiskPtr dest_disk) override;

protected:
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

};

}

}
