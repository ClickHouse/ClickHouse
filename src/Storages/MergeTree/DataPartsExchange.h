#pragma once

#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/IStorage_fwd.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/copyData.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadWriteBufferFromHTTP.h>


namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

namespace DataPartsExchange
{

/** Service for sending parts from the table *MergeTree.
  */
class Service final : public InterserverIOEndpoint
{
public:
    explicit Service(MergeTreeData & data_) : data(data_), log(&Poco::Logger::get(data.getLogName() + " (Replicated PartsService)")) {}

    Service(const Service &) = delete;
    Service & operator=(const Service &) = delete;

    std::string getId(const std::string & node_id) const override;
    void processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response) override;

private:
    MergeTreeData::DataPartPtr findPart(const String & name);
    void sendPartFromMemory(const MergeTreeData::DataPartPtr & part, WriteBuffer & out);
    void sendPartFromDisk(const MergeTreeData::DataPartPtr & part, WriteBuffer & out, int client_protocol_version);
    void sendPartS3Metadata(const MergeTreeData::DataPartPtr & part, WriteBuffer & out);

    /// StorageReplicatedMergeTree::shutdown() waits for all parts exchange handlers to finish,
    /// so Service will never access dangling reference to storage
    MergeTreeData & data;
    Poco::Logger * log;
};

/** Client for getting the parts from the table *MergeTree.
  */
class Fetcher final : private boost::noncopyable
{
public:
    explicit Fetcher(MergeTreeData & data_) : data(data_), log(&Poco::Logger::get("Fetcher")) {}

    /// Downloads a part to tmp_directory. If to_detached - downloads to the `detached` directory.
    MergeTreeData::MutableDataPartPtr fetchPart(
        const StorageMetadataPtr & metadata_snapshot,
        const String & part_name,
        const String & replica_path,
        const String & host,
        int port,
        const ConnectionTimeouts & timeouts,
        const String & user,
        const String & password,
        const String & interserver_scheme,
        bool to_detached = false,
        const String & tmp_prefix_ = "",
        std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr = nullptr,
        bool try_use_s3_copy = true,
        const DiskPtr disk_s3 = nullptr);

    /// You need to stop the data transfer.
    ActionBlocker blocker;

private:
    MergeTreeData::MutableDataPartPtr downloadPartToDisk(
            const String & part_name,
            const String & replica_path,
            bool to_detached,
            const String & tmp_prefix_,
            bool sync,
            ReservationPtr reservation,
            PooledReadWriteBufferFromHTTP & in);

    MergeTreeData::MutableDataPartPtr downloadPartToMemory(
            const String & part_name,
            const UUID & part_uuid,
            const StorageMetadataPtr & metadata_snapshot,
            ReservationPtr reservation,
            PooledReadWriteBufferFromHTTP & in);

    MergeTreeData::MutableDataPartPtr downloadPartToS3(
            const String & part_name,
            const String & replica_path,
            bool to_detached,
            const String & tmp_prefix_,
            const Disks & disks_s3,
            PooledReadWriteBufferFromHTTP & in);

    MergeTreeData & data;
    Poco::Logger * log;
};

}

}
