#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/ActionBlocker.h>
#include <Core/Types.h>
#include <map>
#include <atomic>
#include <utility>
#include <Poco/Net/HTMLForm.h>

namespace Poco { namespace Net { class HTTPServerResponse; } }

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_INTERSERVER_IO_ENDPOINT;
    extern const int NO_SUCH_INTERSERVER_IO_ENDPOINT;
}

/** Location of the service.
  */
struct InterserverIOEndpointLocation
{
public:
    InterserverIOEndpointLocation(const std::string & name_, const std::string & host_, UInt16 port_)
        : name(name_), host(host_), port(port_)
    {
    }

    /// Creates a location based on its serialized representation.
    InterserverIOEndpointLocation(const std::string & serialized_location)
    {
        ReadBufferFromString buf(serialized_location);
        readBinary(name, buf);
        readBinary(host, buf);
        readBinary(port, buf);
        assertEOF(buf);
    }

    /// Serializes the location.
    std::string toString() const
    {
        WriteBufferFromOwnString buf;
        writeBinary(name, buf);
        writeBinary(host, buf);
        writeBinary(port, buf);
        return buf.str();
    }

public:
    std::string name;
    std::string host;
    UInt16 port;
};

/** Query processor from other servers.
  */
class InterserverIOEndpoint
{
public:
    virtual std::string getId(const std::string & path) const = 0;
    virtual void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out, Poco::Net::HTTPServerResponse & response) = 0;
    virtual ~InterserverIOEndpoint() {}

    /// You need to stop the data transfer if blocker is activated.
    ActionBlocker blocker;
};

using InterserverIOEndpointPtr = std::shared_ptr<InterserverIOEndpoint>;


/** Here you can register a service that processes requests from other servers.
  * Used to transfer chunks in ReplicatedMergeTree.
  */
class InterserverIOHandler
{
public:
    void addEndpoint(const String & name, InterserverIOEndpointPtr endpoint)
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (endpoint_map.count(name))
            throw Exception("Duplicate interserver IO endpoint: " + name, ErrorCodes::DUPLICATE_INTERSERVER_IO_ENDPOINT);
        endpoint_map[name] = std::move(endpoint);
    }

    void removeEndpoint(const String & name)
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!endpoint_map.count(name))
            throw Exception("No interserver IO endpoint named " + name, ErrorCodes::NO_SUCH_INTERSERVER_IO_ENDPOINT);
        endpoint_map.erase(name);
    }

    InterserverIOEndpointPtr getEndpoint(const String & name)
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!endpoint_map.count(name))
            throw Exception("No interserver IO endpoint named " + name, ErrorCodes::NO_SUCH_INTERSERVER_IO_ENDPOINT);
        return endpoint_map[name];
    }

private:
    using EndpointMap = std::map<String, InterserverIOEndpointPtr>;

    EndpointMap endpoint_map;
    std::mutex mutex;
};

/// In the constructor calls `addEndpoint`, in the destructor - `removeEndpoint`.
class InterserverIOEndpointHolder
{
public:
    InterserverIOEndpointHolder(const String & name_, InterserverIOEndpointPtr endpoint_, InterserverIOHandler & handler_)
        : name(name_), endpoint(std::move(endpoint_)), handler(handler_)
    {
        handler.addEndpoint(name, endpoint);
    }

    InterserverIOEndpointPtr getEndpoint()
    {
        return endpoint;
    }

    ~InterserverIOEndpointHolder()
    {
        try
        {
            handler.removeEndpoint(name);
            /// After destroying the object, `endpoint` can still live, since its ownership is acquired during the processing of the request,
            /// see InterserverIOHTTPHandler.cpp
        }
        catch (...)
        {
            tryLogCurrentException("~InterserverIOEndpointHolder");
        }
    }

    ActionBlocker & getBlocker() { return endpoint->blocker; }
    void cancelForever() { getBlocker().cancelForever(); }
    ActionBlocker::BlockHolder cancel() { return getBlocker().cancel(); }

private:
    String name;
    InterserverIOEndpointPtr endpoint;
    InterserverIOHandler & handler;
};

using InterserverIOEndpointHolderPtr = std::shared_ptr<InterserverIOEndpointHolder>;

}
