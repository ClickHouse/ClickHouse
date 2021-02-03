#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/ActionBlocker.h>
#include <common/types.h>
#include <map>
#include <atomic>
#include <utility>
#include <shared_mutex>
#include <Poco/Net/HTMLForm.h>

namespace Poco { namespace Net { class HTTPServerResponse; } }

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_INTERSERVER_IO_ENDPOINT;
    extern const int NO_SUCH_INTERSERVER_IO_ENDPOINT;
}

/** Query processor from other servers.
  */
class InterserverIOEndpoint
{
public:
    virtual std::string getId(const std::string & path) const = 0;
    virtual void processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out, Poco::Net::HTTPServerResponse & response) = 0;
    virtual ~InterserverIOEndpoint() = default;

    /// You need to stop the data transfer if blocker is activated.
    ActionBlocker blocker;
    std::shared_mutex rwlock;
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
        std::lock_guard lock(mutex);
        bool inserted = endpoint_map.try_emplace(name, std::move(endpoint)).second;
        if (!inserted)
            throw Exception("Duplicate interserver IO endpoint: " + name, ErrorCodes::DUPLICATE_INTERSERVER_IO_ENDPOINT);
    }

    bool removeEndpointIfExists(const String & name)
    {
        std::lock_guard lock(mutex);
        return endpoint_map.erase(name);
    }

    InterserverIOEndpointPtr getEndpoint(const String & name)
    try
    {
        std::lock_guard lock(mutex);
        return endpoint_map.at(name);
    }
    catch (...)
    {
        throw Exception("No interserver IO endpoint named " + name, ErrorCodes::NO_SUCH_INTERSERVER_IO_ENDPOINT);
    }

private:
    using EndpointMap = std::map<String, InterserverIOEndpointPtr>;

    EndpointMap endpoint_map;
    std::mutex mutex;
};

}
