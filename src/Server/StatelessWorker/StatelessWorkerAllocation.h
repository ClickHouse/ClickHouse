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

/// How to reach one stateless worker: `stateless_worker_port` for task dispatch and
/// `streaming_exchange_port` for exchange streams; zero means the server default.
struct StatelessWorkerAddress
{
    std::string host;
    uint16_t stateless_worker_port = 0;
    uint16_t streaming_exchange_port = 0;
};

/// Per-query RAII handle over the workers a provider handed out; the destructor returns them to
/// the provider. Holds the worker addresses for the caller to dial.
class StatelessWorkerAllocation : private boost::noncopyable
{
public:
    ~StatelessWorkerAllocation();

    const std::vector<StatelessWorkerAddress> & getAddresses() const { return addresses; }

    /// The token to present when dialing the workers, read from the provider on every call so a rotated
    /// token is picked up. Empty for a static allocation.
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
