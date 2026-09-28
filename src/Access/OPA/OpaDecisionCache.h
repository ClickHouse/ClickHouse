#pragma once

#include <Access/OPA/OpaExpressions.h>
#include <Access/OPA/OpaRequest.h>
#include <Core/Types.h>
#include <base/defines.h>

#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>


namespace DB
{

/** Memoizes OPA decisions for the duration of one query.
  *
  * A single query asks the same question repeatedly: the planner checks a table, the analyzer checks
  * it again while resolving columns, and a wrapper storage checks it once more for each of its
  * children. Without memoization each of those costs a request, and a policy server sees traffic
  * proportional to the shape of the query plan rather than to the objects the query touches.
  *
  * The cache lives for one query on purpose. Decisions must not outlive it, because a policy can
  * change between queries and a stale allow is exactly the answer that must never be reused.
  */
class OpaDecisionCache
{
public:
    struct Key
    {
        Names operations;
        OpaResource resource;

        auto toTuple() const { return std::tie(operations, resource); }
        friend bool operator==(const Key & left, const Key & right) { return left.toTuple() == right.toTuple(); }
    };

    struct Hash
    {
        size_t operator()(const Key & key) const;
    };

    std::optional<bool> get(const Key & key) const;
    void set(const Key & key, bool decision);

    /// Row filters are memoized separately: they answer a different question about the same resource,
    /// and a query asks for them as often as it asks whether the table may be read at all.
    std::optional<Strings> getRowFilters(const Key & key) const;
    void setRowFilters(const Key & key, const Strings & filters);

    std::optional<std::vector<OpaColumnMask>> getColumnMasks(const Key & key) const;
    void setColumnMasks(const Key & key, const std::vector<OpaColumnMask> & masks);

private:
    mutable std::mutex mutex;
    std::unordered_map<Key, bool, Hash> decisions TSA_GUARDED_BY(mutex);
    std::unordered_map<Key, Strings, Hash> row_filters TSA_GUARDED_BY(mutex);
    std::unordered_map<Key, std::vector<OpaColumnMask>, Hash> column_masks TSA_GUARDED_BY(mutex);
};

using OpaDecisionCachePtr = std::shared_ptr<OpaDecisionCache>;

}
