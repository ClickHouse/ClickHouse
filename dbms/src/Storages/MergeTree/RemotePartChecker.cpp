#include <Storages/MergeTree/RemotePartChecker.h>
#include <Storages/MergeTree/ReshardingWorker.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPRequest.h>


namespace DB
{

namespace RemotePartChecker
{

namespace
{

std::string getEndpointId(const std::string & node_id)
{
    return "RemotePartChecker:" + node_id;
}

}

Service::Service(StoragePtr & storage_)
    : owned_storage{storage_}, data{static_cast<StorageReplicatedMergeTree &>(*storage_).getData()}
{
}

std::string Service::getId(const std::string & node_id) const
{
    return getEndpointId(node_id);
}

void Service::processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out, Poco::Net::HTTPServerResponse & response)
{
    auto part_name = params.get("part");
    auto hash = params.get("hash");

    Status status = Status::OK;

    try
    {
        auto part_path = data.getFullPath() + "detached/" + part_name;
        if (!Poco::File{part_path}.exists())
            status = Status::NOT_FOUND;
        else
        {
            auto computed_hash = ReshardingWorker::computeHashFromPart(part_path);
            if (computed_hash != hash)
                status = Status::INCONSISTENT;
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        status = Status::ERROR;
    }

    writeBinary(static_cast<UInt8>(status), out);
    out.next();
}

Status Client::check(const std::string & part_name, const std::string & hash,
    const InterserverIOEndpointLocation & to_location)
{
    Poco::URI uri;
    uri.setScheme("http");
    uri.setHost(to_location.host);
    uri.setPort(to_location.port);
    uri.setQueryParameters(
    {
        {"endpoint", getEndpointId(to_location.name) },
        {"compress", "false"},
        {"part", part_name},
        {"hash", hash}
    });

    ReadWriteBufferFromHTTP in{uri, Poco::Net::HTTPRequest::HTTP_POST};

    UInt8 val;
    readBinary(val, in);
    assertEOF(in);
    return static_cast<Status>(val);
}

}

}
