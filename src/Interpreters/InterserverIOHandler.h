#pragma once

#include <Common/ActionBlocker.h>
#include <Common/Exception.h>
#include <Common/SharedMutex.h>
#include <IO/ReadBuffer.h>
#include <base/types.h>

#include <map>
#include <mutex>

namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_INTERSERVER_IO_ENDPOINT;
    extern const int NO_SUCH_INTERSERVER_IO_ENDPOINT;
}

class HTMLForm;
class HTTPServerRequest;
class HTTPServerResponse;
class ReadBuffer;
class WriteBuffer;

/** Query processor from other servers.
  */
class InterserverIOEndpoint
{
public:
    virtual std::string getId(const std::string & path) const = 0;
    virtual void processQuery(const HTMLForm & params, ReadBufferPtr body, WriteBuffer & out, HTTPServerResponse & response) = 0;

    /// Whether this endpoint authenticates a per-request `Bearer` credential. The default is
    /// false: the interserver HTTP handler then rejects a `Bearer` request with the same generic
    /// authentication failure as any other unsupported scheme, without resolving the endpoint, so
    /// presenting a bearer token discloses neither endpoint existence nor endpoint-specific
    /// authentication results. Endpoints that accept bearer tokens override this to return true
    /// and validate the token in `authenticate`.
    virtual bool acceptsBearerAuth() const { return false; }

    /// Optional per-endpoint authentication, invoked by the interserver HTTP handler before
    /// `processQuery` when the shared credential check defers a request to the endpoint (a
    /// `Bearer` credential whose target endpoint's `acceptsBearerAuth` returns true). The default
    /// rejects a `Bearer` credential and accepts Basic / no-credential requests (already validated
    /// by the shared check); it is a safety net for endpoints that do not implement bearer
    /// authentication. Endpoints that authenticate bearer tokens override this. Throw to reject.
    virtual void authenticate(const HTTPServerRequest & request) const;

    virtual ~InterserverIOEndpoint() = default;

    /// You need to stop the data transfer if blocker is activated.
    ActionBlocker blocker;
    SharedMutex rwlock;
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
            throw Exception(ErrorCodes::DUPLICATE_INTERSERVER_IO_ENDPOINT, "Duplicate interserver IO endpoint: {}", name);
    }

    bool removeEndpointIfExists(const String & name)
    {
        std::lock_guard lock(mutex);
        return endpoint_map.erase(name);
    }

    InterserverIOEndpointPtr getEndpoint(const String & name) const
    try
    {
        std::lock_guard lock(mutex);
        return endpoint_map.at(name);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::NO_SUCH_INTERSERVER_IO_ENDPOINT, "No interserver IO endpoint named {}", name);
    }

    /// Non-throwing counterpart of `getEndpoint`, used by the interserver HTTP handler to decide,
    /// before authentication, whether a `Bearer` credential should be deferred to the target
    /// endpoint. Returns nullptr when no endpoint with this name is registered.
    InterserverIOEndpointPtr tryGetEndpoint(const String & name) const
    {
        std::lock_guard lock(mutex);
        auto it = endpoint_map.find(name);
        return it == endpoint_map.end() ? nullptr : it->second;
    }

private:
    using EndpointMap = std::map<String, InterserverIOEndpointPtr>;

    EndpointMap endpoint_map;
    mutable std::mutex mutex;
};

}
