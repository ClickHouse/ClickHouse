#include "CacheDictionary.h"

#include <Columns/ColumnsNumber.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/typeid_cast.h>
#include <DataStreams/IBlockInputStream.h>
#include <ext/chrono_io.h>
#include <ext/map.h>
#include <ext/range.h>
#include <ext/size.h>

namespace ProfileEvents
{
extern const Event DictCacheKeysRequested;
extern const Event DictCacheKeysRequestedMiss;
extern const Event DictCacheKeysRequestedFound;
extern const Event DictCacheKeysExpired;
extern const Event DictCacheKeysNotFound;
extern const Event DictCacheKeysHit;
extern const Event DictCacheRequestTimeNs;
extern const Event DictCacheRequests;
extern const Event DictCacheLockWriteNs;
extern const Event DictCacheLockReadNs;
}

namespace CurrentMetrics
{
extern const Metric DictCacheRequests;
}

namespace DB
{
namespace ErrorCodes
{
}

template <typename AttributeType, typename OutputType, typename DefaultGetter>
void CacheDictionary::getItemsNumberImpl(
    Attribute & attribute, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const
{
    /// First fill everything with default values
    const auto rows = ext::size(ids);
    for (const auto row : ext::range(0, rows))
        out[row] = get_default(row);

    /// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
    std::unordered_map<Key, std::vector<size_t>> cache_expired_ids;
    std::unordered_map<Key, std::vector<size_t>> cache_not_found_ids;

    auto & attribute_array = std::get<ContainerPtrType<AttributeType>>(attribute.arrays);

    size_t cache_hit = 0;

    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();
        /// fetch up-to-date values, decide which ones require update
        for (const auto row : ext::range(0, rows))
        {
            const auto id = ids[row];

            /** cell should be updated if either:
                *    1. ids do not match,
                *    2. cell has expired,
                *    3. explicit defaults were specified and cell was set default. */

            const auto find_result = findCellIdx(id, now);

            auto update_routine = [&]()
            {
                const auto & cell_idx = find_result.cell_idx;
                const auto & cell = cells[cell_idx];
                if (!cell.isDefault())
                    out[row] = static_cast<OutputType>(attribute_array[cell_idx]);
            };

            if (!find_result.valid)
            {

                if (find_result.outdated)
                {
                    /// Protection of reading very expired keys.
                    if (now > cells[find_result.cell_idx].strict_max)
                    {
                        cache_not_found_ids[id].push_back(row);
                        continue;
                    }

                    cache_expired_ids[id].push_back(row);
                    if (allow_read_expired_keys)
                        update_routine();
                }
                else
                {
                    cache_not_found_ids[id].push_back(row);
                }
            }
            else
            {
                ++cache_hit;
                update_routine();
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired_ids.size());
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found_ids.size());
    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

    query_count.fetch_add(rows, std::memory_order_relaxed);
    hit_count.fetch_add(rows - cache_expired_ids.size() - cache_not_found_ids.size(), std::memory_order_release);

    if (cache_not_found_ids.empty())
    {
        /// Nothing to update - return
        if (cache_expired_ids.empty())
            return;

        /// Update async only if allow_read_expired_keys_is_enabledadd condvar usage and better code
        if (allow_read_expired_keys)
        {
            std::vector<Key> required_expired_ids;
            required_expired_ids.reserve(cache_expired_ids.size());
            std::transform(std::begin(cache_expired_ids), std::end(cache_expired_ids), std::back_inserter(required_expired_ids),
                           [](auto & pair) { return pair.first; });

            /// request new values
            auto update_unit_ptr = std::make_shared<UpdateUnit>(required_expired_ids);

            tryPushToUpdateQueueOrThrow(update_unit_ptr);

            /// Nothing to do - return
            return;
        }
    }

    /// From this point we have to update all keys sync.
    /// Maybe allow_read_expired_keys_from_cache_dictionary is disabled
    /// and there no cache_not_found_ids but some cache_expired.

    std::vector<Key> required_ids;
    required_ids.reserve(cache_not_found_ids.size() + cache_expired_ids.size());
    std::transform(
            std::begin(cache_not_found_ids), std::end(cache_not_found_ids),
            std::back_inserter(required_ids), [](auto & pair) { return pair.first; });
    std::transform(
            std::begin(cache_expired_ids), std::end(cache_expired_ids),
            std::back_inserter(required_ids), [](auto & pair) { return pair.first; });

    auto on_cell_updated = [&] (const auto id, const auto cell_idx)
    {
        const auto attribute_value = attribute_array[cell_idx];

        for (const size_t row : cache_not_found_ids[id])
            out[row] = static_cast<OutputType>(attribute_value);

        for (const size_t row : cache_expired_ids[id])
            out[row] = static_cast<OutputType>(attribute_value);
    };

    auto on_id_not_found = [&] (auto, auto) {};

    /// Request new values
    auto update_unit_ptr = std::make_shared<UpdateUnit>(required_ids, on_cell_updated, on_id_not_found);

    tryPushToUpdateQueueOrThrow(update_unit_ptr);
    waitForCurrentUpdateFinish(update_unit_ptr);
}

template <typename DefaultGetter>
void CacheDictionary::getItemsString(
    Attribute & attribute, const PaddedPODArray<Key> & ids, ColumnString * out, DefaultGetter && get_default) const
{
    const auto rows = ext::size(ids);

    /// save on some allocations
    out->getOffsets().reserve(rows);

    auto & attribute_array = std::get<ContainerPtrType<StringRef>>(attribute.arrays);

    auto found_outdated_values = false;

    /// perform optimistic version, fallback to pessimistic if failed
    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();
        /// fetch up-to-date values, discard on fail
        for (const auto row : ext::range(0, rows))
        {
            const auto id = ids[row];

            const auto find_result = findCellIdx(id, now);
            if (!find_result.valid)
            {
                found_outdated_values = true;
                break;
            }
            else
            {
                const auto & cell_idx = find_result.cell_idx;
                const auto & cell = cells[cell_idx];
                const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];
                out->insertData(string_ref.data, string_ref.size);
            }
        }
    }

    /// optimistic code completed successfully
    if (!found_outdated_values)
    {
        query_count.fetch_add(rows, std::memory_order_relaxed);
        hit_count.fetch_add(rows, std::memory_order_release);
        return;
    }

    /// now onto the pessimistic one, discard possible partial results from the optimistic path
    out->getChars().resize_assume_reserved(0);
    out->getOffsets().resize_assume_reserved(0);

    /// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
    std::unordered_map<Key, std::vector<size_t>> cache_expired_ids;
    std::unordered_map<Key, std::vector<size_t>> cache_not_found_ids;
    /// we are going to store every string separately
    std::unordered_map<Key, String> map;

    size_t total_length = 0;
    size_t cache_hit = 0;
    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();
        for (const auto row : ext::range(0, ids.size()))
        {
            const auto id = ids[row];

            const auto find_result = findCellIdx(id, now);


            auto insert_value_routine = [&]()
            {
                const auto & cell_idx = find_result.cell_idx;
                const auto & cell = cells[cell_idx];
                const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];

                if (!cell.isDefault())
                    map[id] = String{string_ref};

                total_length += string_ref.size + 1;
            };

            if (!find_result.valid)
            {
                if (find_result.outdated)
                {
                    /// Protection of reading very expired keys.
                    if (now > cells[find_result.cell_idx].strict_max)
                    {
                        cache_not_found_ids[id].push_back(row);
                        continue;
                    }

                    cache_expired_ids[id].push_back(row);

                    if (allow_read_expired_keys)
                        insert_value_routine();
                } else
                    cache_not_found_ids[id].push_back(row);
            } else
            {
                ++cache_hit;
                insert_value_routine();
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired_ids.size());
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found_ids.size());
    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

    query_count.fetch_add(rows, std::memory_order_relaxed);
    hit_count.fetch_add(rows - cache_expired_ids.size() - cache_not_found_ids.size(), std::memory_order_release);

    /// Async update of expired keys.
    if (cache_not_found_ids.empty())
    {
        if (allow_read_expired_keys && !cache_expired_ids.empty())
        {
            std::vector<Key> required_expired_ids;
            required_expired_ids.reserve(cache_not_found_ids.size());
            std::transform(std::begin(cache_expired_ids), std::end(cache_expired_ids),
                           std::back_inserter(required_expired_ids), [](auto & pair) { return pair.first; });

            auto update_unit_ptr = std::make_shared<UpdateUnit>(required_expired_ids);

            tryPushToUpdateQueueOrThrow(update_unit_ptr);

            /// Do not return at this point, because there some extra stuff to do at the end of this method.
        }
    }

    /// Request new values sync.
    /// We have request both cache_not_found_ids and cache_expired_ids.
    std::vector<Key> required_ids;
    required_ids.reserve(cache_not_found_ids.size() + cache_expired_ids.size());
    std::transform(
        std::begin(cache_not_found_ids), std::end(cache_not_found_ids),
        std::back_inserter(required_ids), [](auto & pair) { return pair.first; });
    std::transform(
        std::begin(cache_expired_ids), std::end(cache_expired_ids),
        std::back_inserter(required_ids), [](auto & pair) { return pair.first; });

    auto on_cell_updated = [&] (const auto id, const auto cell_idx)
    {
        const auto attribute_value = attribute_array[cell_idx];

        map[id] = String{attribute_value};
        total_length += (attribute_value.size + 1) * cache_not_found_ids[id].size();
    };

    auto on_id_not_found = [&] (const auto id, const auto)
    {
        for (const auto row : cache_not_found_ids[id])
            total_length += get_default(row).size + 1;
    };

    auto update_unit_ptr = std::make_shared<UpdateUnit>(required_ids, on_cell_updated, on_id_not_found);

    tryPushToUpdateQueueOrThrow(update_unit_ptr);
    waitForCurrentUpdateFinish(update_unit_ptr);
    out->getChars().reserve(total_length);

    for (const auto row : ext::range(0, ext::size(ids)))
    {
        const auto id = ids[row];
        const auto it = map.find(id);

        const auto string_ref = it != std::end(map) ? StringRef{it->second} : get_default(row);
        out->insertData(string_ref.data, string_ref.size);
    }
}

}
