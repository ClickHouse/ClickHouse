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
class Service : private boost::noncopyable, public InterserverIOEndpoint
{
public:
    explicit Service(StorageReplicatedMergeTree & data_);

    std::string getId(const std::string & node_id) const override;
    void processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response) override;

protected:
    MergeTreeData::DataPartPtr processQueryImpl(
        const HTMLForm & params, ReadBuffer & body, WriteBuffer & out,
        HTTPServerResponse & response, int client_protocol_version, bool send_projections);

    virtual MergeTreeData::DataPartPtr findPart(const String & name);

    virtual void writeUniqueIdIfNeed(
        const MergeTreeData::DataPartPtr & /* part */,
        const IDataPartStorage::ReplicatedFilesDescription & /* replicated_description */,
        WriteBuffer & /* out */) {}

    virtual void compareChecksumsIfNeed(
        const MergeTreeData::DataPartPtr & /* part */,
        const MergeTreeData::DataPart::Checksums & /* data_checksums */);

    MergeTreeData::DataPart::Checksums sendPartFromDisk(
        const MergeTreeData::DataPartPtr & part,
        WriteBuffer & out,
        int client_protocol_version,
        bool send_projections);

    MergeTreeData::DataPart::Checksums sendPartFromDiskImpl(
        const MergeTreeData::DataPartPtr & part,
        IDataPartStorage::ReplicatedFilesDescription replicated_description,
        WriteBuffer & out,
        int client_protocol_version,
        bool send_projections);

    NameSet getFilesToReplicate(const MergeTreeData::DataPartPtr & part, int client_protocol_version);

    virtual bool processRemoteFsMetadataCookie(
        const MergeTreeData::DataPartPtr & /* part */,
        const HTMLForm & /* params */,
        WriteBuffer & /* out */,
        HTTPServerResponse & /* response */,
        int /* client_protocol_version */,
        bool /* send_projections */) { return false; }

    void report_broken_part(MergeTreeData::DataPartPtr part);

    /// StorageReplicatedMergeTree::shutdown() waits for all parts exchange handlers to finish,
    /// so Service will never access dangling reference to storage
    StorageReplicatedMergeTree & data;
    LoggerPtr log;
};

class ServiceZeroCopy : public Service
{
public:
    explicit ServiceZeroCopy(StorageReplicatedMergeTree & data_);

protected:
    MergeTreeData::DataPartPtr findPart(const String & name) override;

    void writeUniqueIdIfNeed(
        const MergeTreeData::DataPartPtr & part,
        const IDataPartStorage::ReplicatedFilesDescription & replicated_description,
        WriteBuffer & out) override;

    void compareChecksumsIfNeed(
        const MergeTreeData::DataPartPtr & /* part */,
        const MergeTreeData::DataPart::Checksums & /* data_checksums */) override {}

    bool processRemoteFsMetadataCookie(
        const MergeTreeData::DataPartPtr & part,
        const HTMLForm & params,
        WriteBuffer & out,
        HTTPServerResponse & response,
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

    virtual MergeTreeData::MutableDataPartPtr downloadPartToDisk(
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
        bool preserve_blobs = false);

    Poco::URI getURI(
        const String & part_name,
        const String & zookeeper_name,
        const String & replica_path,
        const String & host,
        const String & interserver_scheme,
        int port,
        DiskPtr disk) const;

    void readPartInfo(
        ReadWriteBufferFromHTTP & in,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreePartInfo & part_info,
        const String & part_name,
        std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
        String & remote_fs_metadata,
        MergeTreeDataPartType & part_type,
        UUID & part_uuid,
        int & server_protocol_version,
        size_t & projections,
        size_t & sum_files_size,
        DiskPtr & disk) const;

    virtual void addRemoteFsMetadataIfNeed(
        const String & /* part_name */,
        const DiskPtr & /* disk */,
        Poco::URI & /* uri */) const {}

    virtual void processRemoteFsMetadataCookie(const String & remote_fs_metadata, int server_protocol_version);
    virtual void setReplicatedFetchReadCallback(
        const String & part_name,
        const String & replica_path,
        const MergeTreePartInfo & part_info,
        const DiskPtr & disk,
        const Poco::URI & uri,
        bool to_detached,
        size_t sum_files_size,
        ReadWriteBufferFromHTTP & in);

    StorageReplicatedMergeTree & data;
    LoggerPtr log;

};

class FetcherZeroCopy : public Fetcher
{
public:
    explicit FetcherZeroCopy(StorageReplicatedMergeTree & data_);

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
        bool sync) override;

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

    void addRemoteFsMetadataIfNeed(
        const String & part_name,
        const DiskPtr & disk,
        Poco::URI & uri) const override;

    void processRemoteFsMetadataCookie(const String & remote_fs_metadata, int server_protocol_version) override;

    void setReplicatedFetchReadCallback(
        const String & /* part_name */,
        const String & /* replica_path */,
        const MergeTreePartInfo & /* part_info */,
        const DiskPtr & /* disk */,
        const Poco::URI & /* uri */,
        bool /* to_detached */,
        size_t /* sum_files_size */,
        ReadWriteBufferFromHTTP & /* in */) override {}
};

using ServicePtr = std::shared_ptr<DB::DataPartsExchange::Service>;
using FetcherPtr = std::shared_ptr<DB::DataPartsExchange::Fetcher>;

class DataPartsExchangeFactory
{
public:
    static ServicePtr getService(StorageReplicatedMergeTree & data);
    static FetcherPtr getFetcher(StorageReplicatedMergeTree & data);
    static FetcherPtr getFetcher(StorageReplicatedMergeTree & data, bool try_zero_copy);
};

}

}
