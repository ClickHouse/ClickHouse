#include <Storages/TimeSeries/TimeSeriesActiveSeriesCache.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/UUID.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <chrono>
#include <cstring>


namespace DB
{

TimeSeriesActiveSeriesCache::TimeSeriesActiveSeriesCache(size_t max_entries, UInt32 ttl_seconds_)
    : shards(NUM_SHARDS)
    , ttl_seconds(ttl_seconds_)
{
    bool unlimited = (max_entries == 0);
    size_t base = unlimited ? 0 : (max_entries / NUM_SHARDS);
    size_t rem = unlimited ? 0 : (max_entries % NUM_SHARDS);
    for (size_t i = 0; i < NUM_SHARDS; ++i)
    {
        shards[i].unlimited = unlimited;
        shards[i].max_shard_entries = base + (i < rem ? 1 : 0);
    }
}

void TimeSeriesActiveSeriesCache::updateSettings(size_t max_entries, UInt32 ttl_seconds_) const
{
    ttl_seconds.store(ttl_seconds_, std::memory_order_relaxed);
    bool unlimited = (max_entries == 0);
    size_t base = unlimited ? 0 : (max_entries / NUM_SHARDS);
    size_t rem = unlimited ? 0 : (max_entries % NUM_SHARDS);
    for (size_t i = 0; i < NUM_SHARDS; ++i)
    {
        auto & shard = shards[i];
        std::lock_guard lock(shard.mutex);
        shard.unlimited = unlimited;
        shard.max_shard_entries = base + (i < rem ? 1 : 0);
        while (!shard.unlimited && shard.map.size() > shard.max_shard_entries)
        {
            auto it = shard.map.begin();
            if (it != shard.map.end())
                shard.map.erase(it->getKey());
            else
                break;
        }
    }
}

UInt128 TimeSeriesActiveSeriesCache::extractId(const IColumn & id_column, size_t row)
{
    if (const auto * col_uuid = typeid_cast<const ColumnUUID *>(&id_column))
        return col_uuid->getElement(row).toUnderType();

    if (const auto * col_u128 = typeid_cast<const ColumnVector<UInt128> *>(&id_column))
        return col_u128->getElement(row);

    if (const auto * col_fixed = typeid_cast<const ColumnFixedString *>(&id_column))
    {
        if (col_fixed->getN() == 16)
        {
            UInt128 val;
            std::memcpy(&val, col_fixed->getChars().data() + row * 16, 16);
            return val;
        }
    }

    if (const auto * col_u64 = typeid_cast<const ColumnVector<UInt64> *>(&id_column))
        /// The converting constructor zero-extends into the low word. Do not reach for a two-element
        /// brace list instead: it fills the words in argument order, so it would mean the value << 64.
        return col_u64->getElement(row);

    if (const auto * col_lc = typeid_cast<const ColumnLowCardinality *>(&id_column))
    {
        size_t dict_idx = col_lc->getIndexes().getUInt(row);
        return extractId(*col_lc->getDictionary().getNestedColumn(), dict_idx);
    }

    SipHash sip_hash;
    id_column.updateHashWithValue(row, sip_hash);
    return sip_hash.get128();
}

void TimeSeriesActiveSeriesCache::checkBulk(
    const ColumnPtr & id_column, UInt32 current_time, IColumn::Filter & out_filter, size_t & out_written_count) const
{
    size_t num_rows = id_column->size();
    out_filter.resize_fill(num_rows, 0);
    out_written_count = 0;

    if (num_rows == 0)
        return;

    UInt32 ttl = ttl_seconds.load(std::memory_order_relaxed);

    std::vector<std::vector<size_t>> shard_rows(NUM_SHARDS);
    std::vector<UInt128> ids(num_rows);

    for (size_t i = 0; i < num_rows; ++i)
    {
        UInt128 id = extractId(*id_column, i);
        ids[i] = id;
        size_t shard_idx = getShardIndex(id);
        shard_rows[shard_idx].push_back(i);
    }

    for (size_t shard_idx = 0; shard_idx < NUM_SHARDS; ++shard_idx)
    {
        const auto & rows = shard_rows[shard_idx];
        if (rows.empty())
            continue;

        auto & shard = shards[shard_idx];
        std::lock_guard lock(shard.mutex);

        for (size_t row_idx : rows)
        {
            const auto & id = ids[row_idx];
            auto * it = shard.map.find(id);

            bool needs_write = false;
            if (!it)
            {
                needs_write = true;
            }
            else if (ttl > 0 && current_time > it->getMapped() && (current_time - it->getMapped() >= ttl))
            {
                needs_write = true;
            }

            if (needs_write)
            {
                out_filter[row_idx] = 1;
                ++out_written_count;
            }
        }
    }
}

void TimeSeriesActiveSeriesCache::commit(const std::vector<UInt128> & ids, UInt32 commit_time) const
{
    if (ids.empty())
        return;

    if (commit_time == 0)
    {
        commit_time = static_cast<UInt32>(
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    }

    std::vector<std::vector<UInt128>> shard_ids(NUM_SHARDS);
    for (const auto & id : ids)
    {
        size_t shard_idx = getShardIndex(id);
        shard_ids[shard_idx].push_back(id);
    }

    for (size_t shard_idx = 0; shard_idx < NUM_SHARDS; ++shard_idx)
    {
        const auto & ids_in_shard = shard_ids[shard_idx];
        if (ids_in_shard.empty())
            continue;

        auto & shard = shards[shard_idx];
        std::lock_guard lock(shard.mutex);

        if (!shard.unlimited && shard.max_shard_entries == 0)
            continue;

        for (const auto & id : ids_in_shard)
        {
            auto * it = shard.map.find(id);
            if (it)
            {
                it->getMapped() = commit_time;
            }
            else
            {
                if (!shard.unlimited && shard.map.size() >= shard.max_shard_entries)
                {
                    auto first = shard.map.begin();
                    if (first != shard.map.end())
                        shard.map.erase(first->getKey());
                }
                shard.map[id] = commit_time;
            }
        }
    }
}

}
