#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <boost/core/noncopyable.hpp>
#include <Server/StatelessWorker/StatelessWorkerAllocation_fwd.h>
#include <Server/StatelessWorker/IStatelessWorkersProvider.h>

namespace DB
{

class DiscoveryStatelessWorkersProvider;
class StaticStatelessWorkersProvider;

/// The host and port pair used to reach one stateless worker. `stateless_worker_port`
/// is the port the query initiator dials for task dispatch; `streaming_exchange_port` is the port
/// used for inter-worker exchange streams. Zero means "use the server default" (resolved
/// via `resolveWorkerAddress` in the executor).
struct StatelessWorkerAddress
{
    std::string host;
    uint16_t stateless_worker_port = 0;
    uint16_t streaming_exchange_port = 0;
};

/// Lightweight, per-query RAII handle that keeps workers alive for the
/// lifetime of a query. Holds refcounts in the provider; the destructor
/// decrements them so the provider knows which workers are still in use.
///
/// No threads, no timers, no network calls — just a handle.
/// Stores worker_ids internally (returned to the provider on release) and the worker
/// addresses externally for the caller to dial.
class StatelessWorkerAllocation : private boost::noncopyable
{
public:
    ~StatelessWorkerAllocation();

    const std::vector<StatelessWorkerAddress> & getAddresses() const { return addresses; }

    /// The token to present when dialing this allocation's workers, read from the owning provider on
    /// every call so a rotated token is picked up. Empty for a static allocation.
    std::string getAuthToken() const;

private:
    friend class DiscoveryStatelessWorkersProvider;
    friend class StaticStatelessWorkersProvider;
    StatelessWorkerAllocation(
        StatelessWorkersProviderPtr provider_,
        std::vector<std::string> worker_ids_,
        std::vector<StatelessWorkerAddress> addresses_);

    StatelessWorkersProviderPtr provider;
    std::vector<std::string> worker_ids;     /// For release() -> provider refcounts
    std::vector<StatelessWorkerAddress> addresses; /// For getAddresses() -> caller
};

}
