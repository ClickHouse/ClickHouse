#pragma once

#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <IO/WriteBuffer.h>
#include <Core/Types.h>
#include <common/logger_useful.h>

namespace DB
{

namespace RemotePartChecker
{

enum class Status : UInt8
{
    OK = 0,
    NOT_FOUND,
    INCONSISTENT,
    ERROR
};

class Service final : public InterserverIOEndpoint
{
public:
    Service(StoragePtr & storage_);
    Service(const Service &) = delete;
    Service & operator=(const Service &) = delete;
    std::string getId(const std::string & node_id) const override;
    void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out, Poco::Net::HTTPServerResponse & response) override;

private:
    StoragePtr owned_storage;
    MergeTreeData & data;
    Logger * log = &Logger::get("RemotePartChecker::Service");
};

class Client final
{
public:
    Client() = default;
    Client(const Client &) = delete;
    Client & operator=(const Client &) = delete;

    Status check(const std::string & part_name, const std::string & hash,
        const InterserverIOEndpointLocation & to_location);

private:
//    Logger * log = &Logger::get("RemotePartChecker::Client");
};

}

}
