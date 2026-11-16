#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <thread>

#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/HashTable/BucketPartitionedTable.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>

namespace ProfileEvents
{
extern const Event JoinProbePreAggregationRowsChecked;
extern const Event JoinProbePreAggregationGroups;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

/// The number of rows of each key of a block, for a probe stream of `INTERSECT ALL` and `EXCEPT ALL`.
template <typename Key>
struct SetOperationPreAggregationTable final : public SetOperationProbeState::PreAggregationTable
{
    /// When the table is full, it is emptied.
    static constexpr size_t max_keys = 4096;
    /// Pre-aggregation pays off only when the rows have at most this many distinct keys per row.
    static constexpr double max_keys_per_row = 2.0 / 3.0;
    /// When it does not pay off, it is skipped for this many rows per row it checked.
    static constexpr UInt64 rows_to_skip_per_checked_row = 32;

    using Map = ClearableHashMap<Key, UInt32, DefaultHash<Key>, ClearableHashMapCell<Key, UInt32, DefaultHash<Key>>,
        HashTableGrowerWithPrecalculation<std::bit_width(2 * max_keys - 1)>>;

    Map map;
    UInt64 clears = 0;

    void reset()
    {
        /// Emptying bumps a 32-bit version, so the table is made anew before the version wraps.
        if (++clears == std::numeric_limits<UInt32>::max() - 1)
        {
            map = Map();
            clears = 0;
        }
        else
            map.clear();
    }
};

/// The probe of `INTERSECT ALL` and `EXCEPT ALL`: each row of the block takes a copy of its key from the counts of the map,
/// if one is left. `keep_matched` keeps the rows that took a copy, `keep_unmatched` the rows that did not.
template <bool keep_matched, bool keep_unmatched, typename KeyGetter, typename Map, typename AddedColumns>
void joinRightColumnsSetOperation(KeyGetter & key_getter, const Map & map, AddedColumns & added_columns, const ScatteredBlock::Selector::Range & selector)
{
    using Key = typename Map::key_type;
    using Table = SetOperationPreAggregationTable<Key>;

    const size_t block_row_begin = selector.first;
    const size_t num_rows = ScatteredBlock::Selector::size(selector);

    if ((keep_matched || keep_unmatched) && !added_columns.columns.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "The probe of a count map adds {} right-side columns, expected none", added_columns.columns.size());
    if (num_rows >= std::numeric_limits<UInt32>::max()) [[unlikely]]
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in left table block for HashJoin: {}", num_rows);

    if constexpr (PreparedKeyGetter<KeyGetter>)
        key_getter.prepareRows(selector, key_getter.skipOrAllSelected(nullptr));

    Arena pool;

    /// The rows of the block with one key, which take their copies from the map with a single lookup.
    struct KeyGroup
    {
        Key key;
        size_t hash;
        /// Before the lookup, how many rows of the block have the key. After it, how many of them are in the result.
        UInt32 num_rows;
        /// One row of the group stands for all of them in the result.
        UInt32 representative_row;
    };

    PODArray<KeyGroup> groups(num_rows);
    size_t num_groups = 0;

    static constexpr size_t num_buckets = NUM_HASH_TABLE_BUCKETS;
    /// The groups of a bucket, as the range [begin, end) of `groups_by_bucket`.
    struct BucketGroups
    {
        UInt32 begin = 0;
        UInt32 end = 0;
    };
    /// Until the groups are sorted, `end` counts the groups of the bucket.
    std::array<BucketGroups, num_buckets> buckets{};

    auto bucket_of = [](const Key & key, size_t hash) ALWAYS_INLINE -> size_t
    {
        /// A one-level map has no buckets, so its keys are split by their hash.
        if constexpr (Map::NUM_BUCKETS > 1)
            return getBucketOfKey<Map>(key, hash);
        else
            return (hash >> 24) & (num_buckets - 1);
    };

    auto add_group = [&](const Key & key, size_t hash, UInt32 group_num_rows, UInt32 representative_row) ALWAYS_INLINE
    {
        groups[num_groups] = KeyGroup{.key = key, .hash = hash, .num_rows = group_num_rows, .representative_row = representative_row};
        ++buckets[bucket_of(key, hash)].end;
        ++num_groups;
    };

    /// The rows [0, pre_aggregated_end) are grouped by key, the remaining rows are groups of size one.
    size_t pre_aggregated_end = 0;
    auto * probe_state = added_columns.set_operation_probe_state;
    auto & pre_aggregation_skip_budget = *added_columns.set_operation_pre_aggregation_skip_budget;
    const bool sample = probe_state && !pre_aggregation_skip_budget.consume(num_rows);
    if (sample)
    {
        auto & table_ptr = probe_state->pre_aggregation_table;
        if (!table_ptr)
            table_ptr = std::make_unique<Table>();
        auto & table = assert_cast<Table &>(*table_ptr);
        table.reset();

        /// Gives the groups of the keys in the table their number of rows, and empties the table.
        size_t first_group_in_table = num_groups;
        auto evict_table_groups = [&]() ALWAYS_INLINE
        {
            for (size_t g = first_group_in_table; g < num_groups; ++g)
                groups[g].num_rows = table.map.find(groups[g].key, groups[g].hash)->getMapped();
            first_group_in_table = num_groups;
            table.reset();
        };

        auto pre_aggregate_rows = [&](size_t begin, size_t end) ALWAYS_INLINE
        {
            for (size_t i = begin; i < end;)
            {
                /// A run of rows that still fits in the table.
                const size_t run_end = i + std::min(end - i, Table::max_keys - (num_groups - first_group_in_table));
                for (; i < run_end; ++i)
                {
                    auto key_holder = key_getter.getKeyHolder(block_row_begin + i, pool);
                    const auto & key = keyHolderGetKey(key_holder);
                    const size_t hash = map.hash(key);

                    typename Table::Map::LookupResult cell = nullptr;
                    bool inserted = false;
                    table.map.emplace(key, cell, inserted, hash);
                    if (inserted)
                    {
                        cell->getMapped() = 1;
                        add_group(key, hash, 0, static_cast<UInt32>(i));
                    }
                    else
                        ++cell->getMapped();
                }
                if (num_groups - first_group_in_table == Table::max_keys)
                    evict_table_groups();
            }
        };

        auto pays_off = [](size_t num_keys, size_t num_checked_rows)
        {
            return static_cast<double>(num_keys) <= Table::max_keys_per_row * static_cast<double>(num_checked_rows);
        };

        const size_t sample_end = std::min(num_rows, Table::max_keys);
        pre_aggregate_rows(0, sample_end);
        const bool pays_off_on_sample = pays_off(num_groups, sample_end);

        pre_aggregated_end = pays_off_on_sample ? num_rows : sample_end;
        if (pays_off_on_sample)
            pre_aggregate_rows(sample_end, num_rows);
        evict_table_groups();

        if (!pays_off(num_groups, pre_aggregated_end))
            pre_aggregation_skip_budget.replenish(pre_aggregated_end, Table::rows_to_skip_per_checked_row);

        ProfileEvents::increment(ProfileEvents::JoinProbePreAggregationRowsChecked, pre_aggregated_end);
        ProfileEvents::increment(ProfileEvents::JoinProbePreAggregationGroups, num_groups);
    }

    for (size_t i = pre_aggregated_end; i < num_rows; ++i)
    {
        auto key_holder = key_getter.getKeyHolder(block_row_begin + i, pool);
        const auto & key = keyHolderGetKey(key_holder);
        add_group(key, map.hash(key), 1, static_cast<UInt32>(i));
    }

    size_t num_buckets_to_probe = 0;
    UInt32 position = 0;
    for (auto & bucket : buckets)
    {
        num_buckets_to_probe += bucket.end != 0;
        bucket.begin = position;
        position += bucket.end;
        bucket.end = bucket.begin;
    }

    /// Each group goes to the end of its bucket, which moves `end` from `begin` to the end of the range.
    PODArray<UInt32> groups_by_bucket(num_groups);
    for (size_t g = 0; g < num_groups; ++g)
        groups_by_bucket[buckets[bucket_of(groups[g].key, groups[g].hash)].end++] = static_cast<UInt32>(g);

    auto * bucket_locks = added_columns.set_operation_bucket_locks;
    while (num_buckets_to_probe > 0)
    {
        bool probed_any = false;
        for (size_t bucket = 0; bucket < num_buckets; ++bucket)
        {
            if (buckets[bucket].begin == buckets[bucket].end)
                continue;

            std::unique_lock lock(bucket_locks[bucket].mutex, std::try_to_lock);
            if (!lock.owns_lock())
                continue;

            for (size_t k = buckets[bucket].begin; k < buckets[bucket].end; ++k)
            {
                const UInt32 g = groups_by_bucket[k];
                auto & group = groups[g];
                UInt32 matches = 0;
                if (const auto * cell = map.find(group.key, group.hash))
                {
                    auto & remaining = cell->getMapped().remaining;
                    matches = static_cast<UInt32>(std::min<Int64>(remaining, group.num_rows));
                    remaining -= matches;
                }
                group.num_rows = keep_matched ? matches : group.num_rows - matches;
            }

            /// Probed, so its range is emptied.
            buckets[bucket].begin = buckets[bucket].end;
            --num_buckets_to_probe;
            probed_any = true;
        }

        if (!probed_any)
            std::this_thread::yield();
    }

    added_columns.filter = IColumn::Filter(num_rows, 0);
    if constexpr (keep_matched || keep_unmatched)
    {
        /// The groups follow their representative rows, so the result rows stay in ascending order.
        auto * filter = added_columns.filter.data();
        added_columns.matched_rows.resize(num_rows);
        auto * result_rows = added_columns.matched_rows.data();
        size_t num_result_rows = 0;
        bool has_repeats = false;
        for (size_t g = 0; g < num_groups; ++g)
        {
            const UInt32 row = groups[g].representative_row;
            const UInt32 copies = groups[g].num_rows;
            filter[row] = copies != 0;
            has_repeats |= copies > 1;
            for (UInt32 c = 0; c < copies; ++c)
                result_rows[num_result_rows++] = row;
        }
        added_columns.matched_rows.resize(num_result_rows);
        added_columns.matched_rows_have_repeats = has_repeats;
    }
}

}
