#pragma once
#include <Core/Types.h>

#include <atomic>
#include <memory>

namespace DB
{

/// How much one query may still write into one filesystem cache.
/// Created only for a query which sets `filesystem_cache_query_limit_bytes`, and only for the
/// caches it reads through, so a query without the limit never allocates one. Counts the bytes the
/// query reserves itself: data another query cached and this one only reads is not charged, and
/// eviction of what it wrote does not give the budget back.
class FileCacheQueryBudget
{
public:
    explicit FileCacheQueryBudget(size_t size_limit_) : size_limit(size_limit_) {}

    /// Takes `bytes` from what is left, or leaves the budget untouched and returns false. Checking
    /// and taking in one step, so that threads of one query cannot pass the check together and
    /// write more than the limit between them.
    bool tryChargeBytes(size_t bytes)
    {
        size_t charged = charged_bytes.load(std::memory_order_relaxed);
        do
        {
            if (charged + bytes > size_limit)
                return false;
        }
        while (!charged_bytes.compare_exchange_weak(charged, charged + bytes, std::memory_order_relaxed));
        return true;
    }

    /// Gives back what a reservation took after it turned out not to happen.
    void unchargeBytes(size_t bytes) { charged_bytes.fetch_sub(bytes, std::memory_order_relaxed); }

    /// Space which this query reserved is charged as soon as it is reserved and is given back only
    /// if the reservation fails, so a query can be charged for a little more than it writes.
    size_t getChargedBytes() const { return charged_bytes.load(std::memory_order_relaxed); }
    size_t getSizeLimit() const { return size_limit; }

private:
    const size_t size_limit;
    /// A plain counter which publishes nothing else, so relaxed ordering is enough everywhere.
    std::atomic<size_t> charged_bytes = 0;
};

using FileCacheQueryBudgetPtr = std::shared_ptr<FileCacheQueryBudget>;

}
