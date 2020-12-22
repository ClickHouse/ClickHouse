#pragma once

#include <stdexcept>

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

    /// Maybe there are duplicate keys, so we remember their indices.
    std::unordered_map<Key, std::vector<size_t>> cache_expired_or_not_found_ids;

    auto & attribute_array = std::get<ContainerPtrType<AttributeType>>(attribute.arrays);

    size_t cache_hit = 0;
    size_t cache_not_found_count = 0;
    size_t cache_expired_cound = 0;

    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();

        auto insert_to_answer_routine = [&](size_t row, size_t idx)
        {
            auto & cell = cells[idx];
            if (!cell.isDefault())
                out[row] = static_cast<OutputType>(attribute_array[idx]);
        };

        /// fetch up-to-date values, decide which ones require update
        for (const auto row : ext::range(0, rows))
        {
            const auto id = ids[row];

            /** cell should be updated if either:
                *    1. ids do not match,
                *    2. cell has expired,
                *    3. explicit defaults were specified and cell was set default. */

            const auto [cell_idx, state] = findCellIdxForGet(id, now);

            if (state == ResultState::FoundAndValid)
            {
                ++cache_hit;
                insert_to_answer_routine(row, cell_idx);
            }
            else if (state == ResultState::NotFound || state == ResultState::FoundButExpiredPermanently)
            {
                ++cache_not_found_count;
                cache_expired_or_not_found_ids[id].push_back(row);
            }
            else if (state == ResultState::FoundButExpired)
            {
                cache_expired_cound++;
                cache_expired_or_not_found_ids[id].push_back(row);

                if (allow_read_expired_keys)
                    insert_to_answer_routine(row, cell_idx);
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired_cound);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found_count);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

    query_count.fetch_add(rows, std::memory_order_relaxed);
    hit_count.fetch_add(rows - cache_not_found_count - cache_expired_cound, std::memory_order_release);

    if (!cache_not_found_count)
    {
        /// Nothing to update - return
        if (!cache_expired_cound)
            return;

        /// Update async only if allow_read_expired_keys_is_enabledadd condvar usage and better code
        if (allow_read_expired_keys)
        {
            std::vector<Key> required_expired_ids;
            required_expired_ids.reserve(cache_expired_cound);
            std::transform(std::begin(cache_expired_or_not_found_ids), std::end(cache_expired_or_not_found_ids),
                           std::back_inserter(required_expired_ids), [](auto & pair) { return pair.first; });

            /// request new values
            auto update_unit_ptr = std::make_shared<UpdateUnit>(std::move(required_expired_ids));

            tryPushToUpdateQueueOrThrow(update_unit_ptr);

            /// Nothing to do - return
            return;
        }
    }

    /// From this point we have to update all keys sync.
    /// Maybe allow_read_expired_keys_from_cache_dictionary is disabled
    /// and there no cache_not_found_ids but some cache_expired.

    std::vector<Key> required_ids;
    required_ids.reserve(cache_not_found_count + cache_expired_cound);
    std::transform(std::begin(cache_expired_or_not_found_ids), std::end(cache_expired_or_not_found_ids),
                   std::back_inserter(required_ids), [](auto & pair) { return pair.first; });

    /// Request new values
    auto update_unit_ptr = std::make_shared<UpdateUnit>(std::move(required_ids));

    tryPushToUpdateQueueOrThrow(update_unit_ptr);
    waitForCurrentUpdateFinish(update_unit_ptr);

    /// Add updated keys to asnwer.

    const size_t attribute_index = getAttributeIndex(attribute.name);

    for (auto & [key, value] : update_unit_ptr->found_ids)
    {
        if (value.found)
        {
            for (const size_t row : cache_expired_or_not_found_ids[key])
                out[row] = std::get<OutputType>(value.values[attribute_index]);
        }
    }
}

template <typename DefaultGetter>
void CacheDictionary::getItemsString(
    Attribute & attribute, const PaddedPODArray<Key> & ids, ColumnString * out, DefaultGetter && get_default) const
{
    const auto rows = ext::size(ids);

    /// Save on some allocations.
    out->getOffsets().reserve(rows);

    auto & attribute_array = std::get<ContainerPtrType<StringRef>>(attribute.arrays);

    auto found_outdated_values = false;

    /// Perform optimistic version, fallback to pessimistic if failed.
    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();

        /// Fetch up-to-date values, discard on fail.
        for (const auto row : ext::range(0, rows))
        {
            const auto id = ids[row];
            const auto [cell_idx, state] = findCellIdxForGet(id, now);

            if (state == ResultState::FoundAndValid)
            {
                auto & cell = cells[cell_idx];
                const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];
                out->insertData(string_ref.data, string_ref.size);
            }
            else
            {
                found_outdated_values = true;
                break;
            }
        }
    }

    /// Optimistic code completed successfully.
    if (!found_outdated_values)
    {
        query_count.fetch_add(rows, std::memory_order_relaxed);
        hit_count.fetch_add(rows, std::memory_order_release);
        ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, ids.size());
        return;
    }

    /// Now onto the pessimistic one, discard possible partial results from the optimistic path.
    out->getChars().resize_assume_reserved(0);
    out->getOffsets().resize_assume_reserved(0);

    /// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
    std::unordered_map<Key, std::vector<size_t>> cache_expired_or_not_found_ids;
    /// we are going to store every string separately
    std::unordered_map<Key, String> local_cache;

    size_t cache_not_found_count = 0;
    size_t cache_expired_count = 0;

    size_t total_length = 0;
    size_t cache_hit = 0;
    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();

        auto insert_value_routine = [&](size_t row, size_t id, size_t cell_idx)
        {
            const auto & cell = cells[cell_idx];
            const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];

            /// Do not store default, but count it in total length.
            if (!cell.isDefault())
                local_cache[id] = String{string_ref};

            total_length += string_ref.size + 1;
        };

        for (const auto row : ext::range(0, ids.size()))
        {
            const auto id = ids[row];
            const auto [cell_idx, state] = findCellIdxForGet(id, now);

            if (state == ResultState::FoundAndValid)
            {
                ++cache_hit;
                insert_value_routine(row, id, cell_idx);
            }
            else if (state == ResultState::NotFound || state == ResultState::FoundButExpiredPermanently)
            {
                ++cache_not_found_count;
                cache_expired_or_not_found_ids[id].push_back(row);
            }
            else if (state == ResultState::FoundButExpired)
            {
                ++cache_expired_count;
                cache_expired_or_not_found_ids[id].push_back(row);

                if (allow_read_expired_keys)
                    insert_value_routine(row, id, cell_idx);
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired_count);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found_count);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

    query_count.fetch_add(rows, std::memory_order_relaxed);
    hit_count.fetch_add(rows - cache_expired_count - cache_not_found_count, std::memory_order_release);

    /// Async update of expired keys.
    if (!cache_not_found_count)
    {
        if (allow_read_expired_keys && cache_expired_count)
        {
            std::vector<Key> required_expired_ids;
            required_expired_ids.reserve(cache_expired_count);
            std::transform(std::begin(cache_expired_or_not_found_ids), std::end(cache_expired_or_not_found_ids),
                           std::back_inserter(required_expired_ids), [](auto & pair) { return pair.first; });

            auto update_unit_ptr = std::make_shared<UpdateUnit>(std::move(required_expired_ids));

            tryPushToUpdateQueueOrThrow(update_unit_ptr);

            /// Insert all found keys and defaults to output array.
            out->getChars().reserve(total_length);

            for (const auto row : ext::range(0, ext::size(ids)))
            {
                const auto id = ids[row];
                StringRef value;

                /// Previously we stored found keys in map.
                const auto it = local_cache.find(id);
                if (it != local_cache.end())
                    value = StringRef(it->second);
                else
                    value = get_default(row);

                out->insertData(value.data, value.size);
            }

            /// Nothing to do else.
            return;
        }
    }

    /// We will request both cache_not_found_ids and cache_expired_ids sync.
    std::vector<Key> required_ids;
    required_ids.reserve(cache_not_found_count + cache_expired_count);
    std::transform(
        std::begin(cache_expired_or_not_found_ids), std::end(cache_expired_or_not_found_ids),
        std::back_inserter(required_ids), [](auto & pair) { return pair.first; });

    auto update_unit_ptr = std::make_shared<UpdateUnit>(std::move(required_ids));

    tryPushToUpdateQueueOrThrow(update_unit_ptr);
    waitForCurrentUpdateFinish(update_unit_ptr);

    const size_t attribute_index = getAttributeIndex(attribute.name);

    /// Only calculate the total length.
    for (auto & [key, value] : update_unit_ptr->found_ids)
    {
        if (value.found)
        {
            const auto found_value_ref = std::get<String>(value.values[attribute_index]);
            total_length += (found_value_ref.size() + 1) * cache_expired_or_not_found_ids[key].size();
        }
        else
        {
            for (const auto row : cache_expired_or_not_found_ids[key])
                total_length += get_default(row).size + 1;
        }
    }

    out->getChars().reserve(total_length);

    for (const auto row : ext::range(0, ext::size(ids)))
    {
        const auto id = ids[row];
        StringRef value;

        /// We have two maps: found in cache and found in source.
        const auto local_it = local_cache.find(id);
        if (local_it != local_cache.end())
            value = StringRef(local_it->second);
        else
        {
            const auto found_it = update_unit_ptr->found_ids.find(id);

            /// Previously we didn't store defaults in local cache.
            if (found_it != update_unit_ptr->found_ids.end() && found_it->second.found)
                value = std::get<String>(found_it->second.values[attribute_index]);
            else
                value = get_default(row);
        }

        out->insertData(value.data, value.size);
    }
}

}
