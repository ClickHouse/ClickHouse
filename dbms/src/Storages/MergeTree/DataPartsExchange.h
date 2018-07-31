#pragma once

#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/IStorage.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/copyData.h>
#include <IO/ConnectionTimeouts.h>


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
        storage(storage_), log(&Logger::get(data.getLogName() + " (Replicated PartsService)")) {}

    Service(const Service &) = delete;
    Service & operator=(const Service &) = delete;

    std::string getId(const std::string & node_id) const override;
    void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out, Poco::Net::HTTPServerResponse & response) override;

private:
    MergeTreeData::DataPartPtr findPart(const String & name);

private:
    MergeTreeData & data;
    StorageWeakPtr storage;
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
        const ConnectionTimeouts & timeouts,
        const String & user,
        const String & password,
        bool to_detached = false,
        const String & tmp_prefix_ = "");

    /// You need to stop the data transfer.
    ActionBlocker blocker;

private:
    MergeTreeData & data;
    Logger * log;
};

}

}
