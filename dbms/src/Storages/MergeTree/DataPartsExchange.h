#pragma once

#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/copyData.h>


namespace DB
{

namespace DataPartsExchange
{

/** Service for sending parts from the table *MergeTree.
  */
class Service final : public InterserverIOEndpoint
{
public:
    Service(MergeTreeData & data_, StoragePtr & storage_) : data(data_),
        owned_storage(storage_), log(&Logger::get(data.getLogName() + " (Replicated PartsService)")) {}

    Service(const Service &) = delete;
    Service & operator=(const Service &) = delete;

    std::string getId(const std::string & node_id) const override;
    void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out, Poco::Net::HTTPServerResponse & response) override;

private:
    MergeTreeData::DataPartPtr findPart(const String & name);
    MergeTreeData::DataPartPtr findShardedPart(const String & name, size_t shard_no);

private:
    MergeTreeData & data;
    StoragePtr owned_storage;
    Logger * log;
};

/** Client for getting the parts from the table *MergeTree.
  */
class Fetcher final
{
public:
    Fetcher(MergeTreeData & data_) : data(data_), log(&Logger::get("Fetcher")) {}

    Fetcher(const Fetcher &) = delete;
    Fetcher & operator=(const Fetcher &) = delete;

    /// Downloads a part to tmp_directory. If to_detached - downloads to the `detached` directory.
    MergeTreeData::MutableDataPartPtr fetchPart(
        const String & part_name,
        const String & replica_path,
        const String & host,
        int port,
        bool to_detached = false);

    /// Method for resharding. Downloads a sharded part
    /// from the specified shard to the `to_detached` folder.
    MergeTreeData::MutableDataPartPtr fetchShardedPart(
        const InterserverIOEndpointLocation & location,
        const String & part_name,
        size_t shard_no);

    void cancel() { is_cancelled = true; }

private:
    MergeTreeData::MutableDataPartPtr fetchPartImpl(
        const String & part_name,
        const String & replica_path,
        const String & host,
        int port,
        const String & shard_no,
        bool to_detached);

private:
    MergeTreeData & data;
    /// You need to stop the data transfer.
    std::atomic<bool> is_cancelled {false};
    Logger * log;
};

}

}
