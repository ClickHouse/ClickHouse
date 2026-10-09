#include <Server/StatelessWorker/StatelessWorkerAllocation.h>
#include <Server/StatelessWorker/IStatelessWorkersProvider.h>
#include <Common/ProfileEvents.h>
#include <base/defines.h>

namespace ProfileEvents
{
    extern const Event StatelessWorkerProvided;
}

namespace DB
{

StatelessWorkerAllocation::StatelessWorkerAllocation(
    StatelessWorkersProviderPtr provider_,
    std::vector<std::string> worker_ids_,
    std::vector<StatelessWorkerAddress> addresses_)
    : provider(std::move(provider_))
    , worker_ids(std::move(worker_ids_))
    , addresses(std::move(addresses_))
{
    chassert(worker_ids.empty() || worker_ids.size() == addresses.size());

    /// Per-query count of stateless workers provided to the query (a query
    /// executed over N stateless workers adds N). Constructed on the query
    /// thread, so this is attributed to the query's ProfileEvents. Counts the
    /// provided addresses: a static allocation carries addresses but no
    /// `worker_ids`, so counting ids would report zero for static clusters.
    ProfileEvents::increment(ProfileEvents::StatelessWorkerProvided, addresses.size());
}

StatelessWorkerAllocation::~StatelessWorkerAllocation()
{
    if (provider)
        provider->release(worker_ids);
}

std::string StatelessWorkerAllocation::getAuthToken() const
{
    /// A static allocation has no provider and no token.
    return provider ? provider->getAuthToken() : std::string{};
}

}
