#include <Common/HostResolvePool.h>

#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/ErrorCodes.h>
#include <Common/thread_local_rng.h>
#include <Common/MemoryTrackerSwitcher.h>

#include <mutex>

namespace ProfileEvents
{
    extern const Event AddressesDiscovered;
    extern const Event AddressesExpired;
    extern const Event AddressesMarkedAsFailed;
}

namespace CurrentMetrics
{
    extern const Metric AddressesActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int DNS_ERROR;
}

HostResolverMetrics HostResolver::getMetrics()
{
    return HostResolverMetrics{
        .discovered = ProfileEvents::AddressesDiscovered,
        .expired = ProfileEvents::AddressesExpired,
        .failed = ProfileEvents::AddressesMarkedAsFailed,
        .active_count = CurrentMetrics::AddressesActive,
    };
}

HostResolver::WeakPtr HostResolver::getWeakFromThis()
{
    return weak_from_this();
}

HostResolver::HostResolver(String host_, Poco::Timespan history_)
    : host(std::move(host_))
    , history(history_)
    , resolve_function([](const String & host_to_resolve) { return DNSResolver::instance().resolveHostAll(host_to_resolve); })
{
    update();
}

HostResolver::HostResolver(
    ResolveFunction && resolve_function_, String host_, Poco::Timespan history_)
    : host(std::move(host_)), history(history_), resolve_function(std::move(resolve_function_))
{
    update();
}

HostResolver::~HostResolver()
{
    std::lock_guard lock(mutex);
    CurrentMetrics::sub(metrics.active_count, records.size());
    records.clear();
}

void HostResolver::Entry::setFail()
{
    fail = true;

    if (auto lock = pool.lock())
        lock->setFail(address);
}

HostResolver::Entry::~Entry()
{
    if (!fail)
    {
        if (auto lock = pool.lock())
            lock->setSuccess(address);
    }
}

void HostResolver::update()
{
    MemoryTrackerSwitcher switcher{&total_memory_tracker};

    auto next_gen = resolve_function(host);
    if (next_gen.empty())
        throw NetException(ErrorCodes::DNS_ERROR, "no endpoints resolved for host {}", host);

    std::sort(next_gen.begin(), next_gen.end());

    Poco::Timestamp now;

    std::lock_guard lock(mutex);
    updateImpl(now, next_gen);
}

void HostResolver::reset()
{
    std::lock_guard lock(mutex);

    CurrentMetrics::sub(metrics.active_count, records.size());
    records.clear();
}

void HostResolver::updateWeights()
{
    updateWeightsImpl();

    if (getTotalWeight() == 0 && !records.empty())
    {
        for (auto & rec : records)
            rec.failed = false;

        updateWeightsImpl();
    }

    chassert((getTotalWeight() > 0 && !records.empty()) || records.empty());
}

HostResolver::Entry HostResolver::resolve()
{
    if (isUpdateNeeded())
        update();

    std::lock_guard lock(mutex);
    return Entry(*this, selectBest());
}

void HostResolver::setSuccess(const Poco::Net::IPAddress & address)
{
    std::lock_guard lock(mutex);

    auto it = find(address);
    if (it == records.end())
        return;

    auto old_weight = it->getWeight();
    ++it->usage;
    auto new_weight = it->getWeight();

    if (old_weight != new_weight)
        updateWeights();
}

void HostResolver::setFail(const Poco::Net::IPAddress & address)
{
    Poco::Timestamp now;

    {
        std::lock_guard lock(mutex);

        auto it = find(address);
        if (it == records.end())
            return;

        it->failed = true;
        it->fail_time = now;
    }

    ProfileEvents::increment(metrics.failed);
    update();
}

Poco::Net::IPAddress HostResolver::selectBest()
{
    chassert(!records.empty());
    auto random_weight_picker = std::uniform_int_distribution<size_t>(0, getTotalWeight() - 1);
    size_t weight = random_weight_picker(thread_local_rng);
    auto it = std::partition_point(records.begin(), records.end(), [&](const Record & rec) { return rec.weight_prefix_sum <= weight; });
    chassert(it != records.end());
    return it->address;
}

HostResolver::Records::iterator HostResolver::find(const Poco::Net::IPAddress & addr) TSA_REQUIRES(mutex)
{
    auto it = std::lower_bound(
        records.begin(), records.end(), addr, [](const Record & rec, const Poco::Net::IPAddress & value) { return rec.address < value; });

    if (it != records.end() && it->address != addr)
        return records.end();

    return it;
}

bool HostResolver::isUpdateNeeded()
{
    Poco::Timestamp now;

    std::lock_guard lock(mutex);
    return last_resolve_time + history < now || records.empty();
}

void HostResolver::updateImpl(Poco::Timestamp now, std::vector<Poco::Net::IPAddress> & next_gen)
    TSA_REQUIRES(mutex)
{
    const auto last_effective_resolve = now - history;

    Records merged;
    merged.reserve(records.size() + next_gen.size());

    auto it_before = records.begin();
    auto it_next = next_gen.begin();

    while (it_before != records.end() || it_next != next_gen.end())
    {
        if (it_next == next_gen.end() || (it_before != records.end() && it_before->address < *it_next))
        {
            if (it_before->resolve_time >= last_effective_resolve)
                merged.push_back(*it_before);
            else
            {
                CurrentMetrics::sub(metrics.active_count, 1);
                ProfileEvents::increment(metrics.expired, 1);
            }
            ++it_before;
        }
        else if (it_before == records.end() || (it_next != next_gen.end() && *it_next < it_before->address))
        {
            CurrentMetrics::add(metrics.active_count, 1);
            ProfileEvents::increment(metrics.discovered, 1);
            merged.push_back(Record(*it_next, now));
            ++it_next;
        }
        else
        {
            merged.push_back(*it_before);
            merged.back().resolve_time = now;

            ++it_before;
            ++it_next;
        }
    }

    for (auto & rec : merged)
        if (rec.failed && rec.fail_time < last_effective_resolve)
            rec.failed = false;

    chassert(std::is_sorted(merged.begin(), merged.end()));

    last_resolve_time = now;
    records.swap(merged);

    if (records.empty())
        throw NetException(ErrorCodes::DNS_ERROR, "no endpoints resolved for host {}", host);

    updateWeights();
}

size_t HostResolver::getTotalWeight() const
{
    if (records.empty())
        return 0;
    return records.back().weight_prefix_sum;
}


void HostResolver::updateWeightsImpl()
{
    size_t total_weight_next = 0;

    for (auto & rec: records)
    {
        total_weight_next += rec.getWeight();
        rec.weight_prefix_sum = total_weight_next;
    }
}

HostResolversPool & HostResolversPool::instance()
{
    static HostResolversPool instance;
    return instance;
}

void HostResolversPool::dropCache()
{
    std::lock_guard lock(mutex);
    host_pools.clear();
}

HostResolver::Ptr HostResolversPool::getResolver(const String & host)
{
    std::lock_guard lock(mutex);

    auto it = host_pools.find(host);
    if (it != host_pools.end())
        return it->second;

    it = host_pools.emplace(host, HostResolver::create(host)).first;

    return it->second;
}

}
