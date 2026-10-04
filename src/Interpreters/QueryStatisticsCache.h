#pragma once

#include <mutex>
#include <optional>
#include <Core/Names.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

namespace DB
{

/// Merged part statistics reused within one query. A part name carries partition and block info but
/// no table identity, so the key holds the storage too, and `required_columns` is canonical because
/// the loader it keys reads the names as a set. Entries live until the query ends, hence the cap.
class QueryStatisticsCache
{
public:
    struct Key
    {
        const IStorage * storage = nullptr;
        Names part_names;
        /// Sorted and deduplicated - build it with `makeRequiredColumns`.
        Names required_columns;

        bool operator==(const Key & other) const = default;

        static Names makeRequiredColumns(const Names & columns);
    };

    struct KeyHash
    {
        size_t operator()(const Key & key) const;
    };

    /// Absent key -> `std::nullopt`. A stored null payload means the parts carry no statistics, so that
    /// outcome is reached once too.
    std::optional<ConditionSelectivityPayloadPtr> get(const Key & key) const;

    /// Ignored once `max_entries` entries are held. Nothing is evicted: a caller whose entry
    /// disappeared mid-query would load again, which is what this cache exists to avoid.
    void set(Key key, ConditionSelectivityPayloadPtr payload, size_t max_entries);

private:
    using Cache = std::unordered_map<Key, ConditionSelectivityPayloadPtr, KeyHash>;

    mutable Cache cache;
    mutable std::mutex cache_mutex;
};

using QueryStatisticsCachePtr = std::shared_ptr<QueryStatisticsCache>;

}
