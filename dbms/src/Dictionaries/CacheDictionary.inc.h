#include "CacheDictionary.h"

#include <ext/size.h>
#include <ext/map.h>
#include <ext/range.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnsNumber.h>

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
    extern const int TYPE_MISMATCH;
}

template <typename OutputType, typename DefaultGetter>
void CacheDictionary::getItemsNumber(
    Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    ResultArrayType<OutputType> & out,
    DefaultGetter && get_default) const
{
    if (false) {}
#define DISPATCH(TYPE) \
    else if (attribute.type == AttributeUnderlyingType::TYPE) \
        getItemsNumberImpl<TYPE, OutputType>(attribute, ids, out, std::forward<DefaultGetter>(get_default));
    DISPATCH(UInt8)
    DISPATCH(UInt16)
    DISPATCH(UInt32)
    DISPATCH(UInt64)
    DISPATCH(UInt128)
    DISPATCH(Int8)
    DISPATCH(Int16)
    DISPATCH(Int32)
    DISPATCH(Int64)
    DISPATCH(Float32)
    DISPATCH(Float64)
    DISPATCH(Decimal32)
    DISPATCH(Decimal64)
    DISPATCH(Decimal128)
#undef DISPATCH
    else
        throw Exception("Unexpected type of attribute: " + toString(attribute.type), ErrorCodes::LOGICAL_ERROR);
}

template <typename AttributeType, typename OutputType, typename DefaultGetter>
void CacheDictionary::getItemsNumberImpl(
    Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    ResultArrayType<OutputType> & out,
    DefaultGetter && get_default) const
{
    /// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
    std::unordered_map<Key, std::vector<size_t>> outdated_ids;
    auto & attribute_array = std::get<ContainerPtrType<AttributeType>>(attribute.arrays);
    const auto rows = ext::size(ids);

    size_t cache_expired = 0, cache_not_found = 0, cache_hit = 0;

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
            if (!find_result.valid)
            {
                outdated_ids[id].push_back(row);
                if (find_result.outdated)
                    ++cache_expired;
                else
                    ++cache_not_found;
            }
            else
            {
                ++cache_hit;
                const auto & cell_idx = find_result.cell_idx;
                const auto & cell = cells[cell_idx];
                out[row] = cell.isDefault() ? get_default(row) : static_cast<OutputType>(attribute_array[cell_idx]);
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

    query_count.fetch_add(rows, std::memory_order_relaxed);
    hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

    if (outdated_ids.empty())
        return;

    std::vector<Key> required_ids(outdated_ids.size());
    std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
        [] (auto & pair) { return pair.first; });

    /// request new values
    update(required_ids,
    [&] (const auto id, const auto cell_idx)
    {
        const auto attribute_value = attribute_array[cell_idx];

        for (const size_t row : outdated_ids[id])
            out[row] = static_cast<OutputType>(attribute_value);
    },
    [&] (const auto id, const auto)
    {
        for (const size_t row : outdated_ids[id])
            out[row] = get_default(row);
    });
}

template <typename DefaultGetter>
void CacheDictionary::getItemsString(
    Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    ColumnString * out,
    DefaultGetter && get_default) const
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
    std::unordered_map<Key, std::vector<size_t>> outdated_ids;
    /// we are going to store every string separately
    std::unordered_map<Key, String> map;

    size_t total_length = 0;
    size_t cache_expired = 0, cache_not_found = 0, cache_hit = 0;
    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();
        for (const auto row : ext::range(0, ids.size()))
        {
            const auto id = ids[row];

            const auto find_result = findCellIdx(id, now);
            if (!find_result.valid)
            {
                outdated_ids[id].push_back(row);
                if (find_result.outdated)
                    ++cache_expired;
                else
                    ++cache_not_found;
            }
            else
            {
                ++cache_hit;
                const auto & cell_idx = find_result.cell_idx;
                const auto & cell = cells[cell_idx];
                const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];

                if (!cell.isDefault())
                    map[id] = String{string_ref};

                total_length += string_ref.size + 1;
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

    query_count.fetch_add(rows, std::memory_order_relaxed);
    hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

    /// request new values
    if (!outdated_ids.empty())
    {
        std::vector<Key> required_ids(outdated_ids.size());
        std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
            [] (auto & pair) { return pair.first; });

        update(required_ids,
        [&] (const auto id, const auto cell_idx)
        {
            const auto attribute_value = attribute_array[cell_idx];

            map[id] = String{attribute_value};
            total_length += (attribute_value.size + 1) * outdated_ids[id].size();
        },
        [&] (const auto id, const auto)
        {
            for (const auto row : outdated_ids[id])
                total_length += get_default(row).size + 1;
        });
    }

    out->getChars().reserve(total_length);

    for (const auto row : ext::range(0, ext::size(ids)))
    {
        const auto id = ids[row];
        const auto it = map.find(id);

        const auto string_ref = it != std::end(map) ? StringRef{it->second} : get_default(row);
        out->insertData(string_ref.data, string_ref.size);
    }
}

template <typename PresentIdHandler, typename AbsentIdHandler>
void CacheDictionary::update(
    const std::vector<Key> & requested_ids,
    PresentIdHandler && on_cell_updated,
    AbsentIdHandler && on_id_not_found) const
{
    std::unordered_map<Key, UInt8> remaining_ids{requested_ids.size()};
    for (const auto id : requested_ids)
        remaining_ids.insert({ id, 0 });

    std::uniform_int_distribution<UInt64> distribution
    {
        dict_lifetime.min_sec,
        dict_lifetime.max_sec
    };

    const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::DictCacheRequests};
        Stopwatch watch;
        auto stream = source_ptr->loadIds(requested_ids);
        stream->readPrefix();

        const auto now = std::chrono::system_clock::now();

        while (const auto block = stream->read())
        {
            const auto id_column = typeid_cast<const ColumnUInt64 *>(block.safeGetByPosition(0).column.get());
            if (!id_column)
                throw Exception{name + ": id column has type different from UInt64.", ErrorCodes::TYPE_MISMATCH};

            const auto & ids = id_column->getData();

            /// cache column pointers
            const auto column_ptrs = ext::map<std::vector>(ext::range(0, attributes.size()), [&block] (size_t i)
            {
                return block.safeGetByPosition(i + 1).column.get();
            });

            for (const auto i : ext::range(0, ids.size()))
            {
                const auto id = ids[i];

                const auto find_result = findCellIdx(id, now);
                const auto & cell_idx = find_result.cell_idx;

                auto & cell = cells[cell_idx];

                for (const auto attribute_idx : ext::range(0, attributes.size()))
                {
                    const auto & attribute_column = *column_ptrs[attribute_idx];
                    auto & attribute = attributes[attribute_idx];

                    setAttributeValue(attribute, cell_idx, attribute_column[i]);
                }

                /// if cell id is zero and zero does not map to this cell, then the cell is unused
                if (cell.id == 0 && cell_idx != zero_cell_idx)
                    element_count.fetch_add(1, std::memory_order_relaxed);

                cell.id = id;
                if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
                    cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
                else
                    cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

                /// inform caller
                on_cell_updated(id, cell_idx);
                /// mark corresponding id as found
                remaining_ids[id] = 1;
            }
        }

        stream->readSuffix();

        ProfileEvents::increment(ProfileEvents::DictCacheKeysRequested, requested_ids.size());
        ProfileEvents::increment(ProfileEvents::DictCacheRequestTimeNs, watch.elapsed());
    }

    size_t not_found_num = 0, found_num = 0;

    const auto now = std::chrono::system_clock::now();
    /// Check which ids have not been found and require setting null_value
    for (const auto & id_found_pair : remaining_ids)
    {
        if (id_found_pair.second)
        {
            ++found_num;
            continue;
        }
        ++not_found_num;

        const auto id = id_found_pair.first;

        const auto find_result = findCellIdx(id, now);
        const auto & cell_idx = find_result.cell_idx;

        auto & cell = cells[cell_idx];

        /// Set null_value for each attribute
        for (auto & attribute : attributes)
            setDefaultAttributeValue(attribute, cell_idx);

        /// Check if cell had not been occupied before and increment element counter if it hadn't
        if (cell.id == 0 && cell_idx != zero_cell_idx)
            element_count.fetch_add(1, std::memory_order_relaxed);

        cell.id = id;
        if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
            cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
        else
            cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

        cell.setDefault();

        /// inform caller that the cell has not been found
        on_id_not_found(id, cell_idx);
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedMiss, not_found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedFound, found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheRequests);
}

}
