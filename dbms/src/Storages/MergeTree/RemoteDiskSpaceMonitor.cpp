#include <Storages/MergeTree/RemoteDiskSpaceMonitor.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Interpreters/Context.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/HTTPRequest.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

namespace RemoteDiskSpaceMonitor
{

namespace
{

std::string getEndpointId(const std::string & node_id)
{
    return "RemoteDiskSpaceMonitor:" + node_id;
}

}

Service::Service(const Context & context_)
    : context{context_}
{
}

std::string Service::getId(const std::string & node_id) const
{
    return getEndpointId(node_id);
}

void Service::processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out, Poco::Net::HTTPServerResponse & response)
{
    if (is_cancelled)
        throw Exception{"RemoteDiskSpaceMonitor service terminated", ErrorCodes::ABORTED};

    size_t free_space = DiskSpaceMonitor::getUnreservedFreeSpace(context.getPath());
    writeBinary(free_space, out);
    out.next();
}

size_t Client::getFreeSpace(const InterserverIOEndpointLocation & location) const
{
    Poco::URI uri;
    uri.setScheme("http");
    uri.setHost(location.host);
    uri.setPort(location.port);
    uri.setQueryParameters(
    {
        {"endpoint", getEndpointId(location.name) },
        {"compress", "false"}
    }
    );

    ReadWriteBufferFromHTTP in{uri, Poco::Net::HTTPRequest::HTTP_POST};

    size_t free_disk_space;
    readBinary(free_disk_space, in);
    assertEOF(in);

    return free_disk_space;
}

}

}
