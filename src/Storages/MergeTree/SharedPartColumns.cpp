#include <Storages/MergeTree/SharedPartColumns.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/ColumnsDescription.h>
#include <Common/CurrentMetrics.h>
#include <Common/SharedLockGuard.h>
#include <Common/SipHash.h>

#include <xxhash.h>

namespace CurrentMetrics
{
    extern const Metric SharedPartSerializationsCacheSize;
    extern const Metric SharedPartColumnsSubstreamsCacheSize;
}

namespace DB
{

namespace
{

SharedPartColumns::NameToNumber buildColumnPositions(const NamesAndTypesList & columns)
{
    SharedPartColumns::NameToNumber positions;
    positions.reserve(columns.size());
    size_t pos = 0;
    for (const auto & column : columns)
        positions.emplace(column.name, pos++);
    return positions;
}

/// Amortized cleanup of expired entries: they are erased only when the map has doubled since the
/// last sweep, so the cost stays O(1) per insertion and the map size stays within a constant
/// factor of the number of live entries.
template <typename Cache>
void sweepExpiredEntries(Cache & cache, size_t & size_after_sweep, AggregatedMetrics::GlobalSum & metric)
{
    if (cache.size() < std::max<size_t>(16, size_after_sweep * 2))
        return;

    size_t erased = std::erase_if(cache, [](const auto & entry) { return entry.second.expired(); });
    metric.sub(erased);
    size_after_sweep = cache.size();
}

}

SharedPartColumns::SharedPartColumns(
    NamesAndTypesList columns_,
    std::shared_ptr<const ColumnsDescription> columns_description_,
    std::shared_ptr<const ColumnsDescription> columns_description_with_collected_nested_)
    : columns(std::move(columns_))
    , column_name_to_position(buildColumnPositions(columns))
    , columns_description(std::move(columns_description_))
    , columns_description_with_collected_nested(std::move(columns_description_with_collected_nested_))
    , serializations_cache_metric_handle(CurrentMetrics::SharedPartSerializationsCacheSize)
    , substreams_cache_metric_handle(CurrentMetrics::SharedPartColumnsSubstreamsCacheSize)
{
}

SerializationByName SharedPartColumns::buildSerializations(const SerializationInfoByName & infos) const
{
    SerializationByName serializations;

    for (const auto & column : columns)
    {
        auto it = infos.find(column.name);
        auto serialization = it == infos.end()
            ? IDataType::getSerialization(column, infos.getSettings())
            : IDataType::getSerialization(column, *it->second);

        serializations.emplace(column.name, serialization);

        IDataType::forEachSubcolumn([&](const auto &, const auto & subname, const auto & subdata)
        {
            auto full_name = Nested::concatenateName(column.name, subname);
            /// Don't override the column serialization with subcolumn serialization if column with the same name exists.
            if (!column_name_to_position.contains(full_name))
                serializations.emplace(full_name, subdata.serialization);
        }, ISerialization::SubstreamData(serialization));
    }

    return serializations;
}

SharedPartColumns::SerializationsCacheKey SharedPartColumns::buildSerializationsCacheKey(const SerializationInfoByName & infos)
{
    SerializationsCacheKey key{infos.getSettings(), {}};
    key.per_column_kinds.reserve(infos.size());

    /// The infos map is ordered by column name, so the key is deterministic.
    for (const auto & [column_name, info] : infos)
    {
        WriteBufferFromOwnString kinds;
        info->serialializeKindStackBinary(kinds);
        key.per_column_kinds.emplace_back(column_name, kinds.str(), info->getSettings());
    }

    return key;
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"

namespace
{

/// Feed a string into the hash state, length-prefixed so concatenations are unambiguous.
void updateHashWithString(XXH3_state_t & state, std::string_view s)
{
    UInt64 size = s.size();
    XXH_INLINE_XXH3_64bits_update(&state, &size, sizeof(size));
    XXH_INLINE_XXH3_64bits_update(&state, s.data(), s.size());
}

}

size_t SharedPartColumns::SerializationsCacheKeyHash::operator()(const SerializationsCacheKey & key) const noexcept
{
    /// XXH3 instead of the more usual SipHash: this runs on the part loading path over one entry
    /// per column with a serialization info, and XXH3 is several times faster (the hash only keys
    /// an in-memory cache, so it does not need to be cryptographic or stable across versions).
    XXH3_state_t state;
    XXH_INLINE_XXH3_64bits_reset(&state);

    SipHash settings_hash;
    key.settings.updateHash(settings_hash);
    UInt64 settings_hash_value = settings_hash.get64();
    XXH_INLINE_XXH3_64bits_update(&state, &settings_hash_value, sizeof(settings_hash_value));

    UInt64 entries = key.per_column_kinds.size();
    XXH_INLINE_XXH3_64bits_update(&state, &entries, sizeof(entries));

    for (const auto & [name, kinds, settings] : key.per_column_kinds)
    {
        updateHashWithString(state, name);
        updateHashWithString(state, kinds);

        SipHash entry_settings_hash;
        settings.updateHash(entry_settings_hash);
        UInt64 entry_settings_hash_value = entry_settings_hash.get64();
        XXH_INLINE_XXH3_64bits_update(&state, &entry_settings_hash_value, sizeof(entry_settings_hash_value));
    }

    return XXH_INLINE_XXH3_64bits_digest(&state);
}

#pragma clang diagnostic pop

std::shared_ptr<const SerializationByName> SharedPartColumns::getSerializations(const SerializationInfoByName & infos) const
{
    auto key = buildSerializationsCacheKey(infos);

    {
        SharedLockGuard lock(serializations_cache_mutex);
        if (auto it = serializations_cache.find(key); it != serializations_cache.end())
            if (auto shared = it->second.lock())
                return shared;
    }

    /// Build outside the lock: when the cache does not help (each part has a unique combination of
    /// serialization kinds), concurrent part loads still build their maps in parallel, exactly as
    /// they did when the map was built per-part.
    auto built = std::make_shared<const SerializationByName>(buildSerializations(infos));

    std::lock_guard lock(serializations_cache_mutex);
    auto [it, inserted] = serializations_cache.try_emplace(std::move(key));
    if (!inserted)
    {
        /// A concurrent load interned the same key first: reuse its result.
        if (auto shared = it->second.lock())
            return shared;

        /// The previous holder of this key is gone: replace the expired entry in place.
        it->second = built;
        return built;
    }

    it->second = built;
    serializations_cache_metric_handle.add(1);
    sweepExpiredEntries(serializations_cache, serializations_cache_size_after_sweep, serializations_cache_metric_handle);
    return built;
}

std::shared_ptr<const ColumnsSubstreams> SharedPartColumns::internColumnsSubstreams(ColumnsSubstreams substreams) const
{
    UInt128 content_hash = substreams.getHash();

    {
        SharedLockGuard lock(substreams_cache_mutex);
        if (auto it = substreams_cache.find(content_hash); it != substreams_cache.end())
            if (auto shared = it->second.lock(); shared && *shared == substreams)
                return shared;
    }

    auto built = std::make_shared<const ColumnsSubstreams>(std::move(substreams));

    std::lock_guard lock(substreams_cache_mutex);
    auto [it, inserted] = substreams_cache.try_emplace(content_hash);
    if (!inserted)
    {
        if (auto shared = it->second.lock())
        {
            if (*shared == *built)
                return shared;

            /// Full 128-bit hash collision between different contents: keep the existing entry and
            /// return the non-shared copy. Correctness never depends on the hash.
            return built;
        }

        it->second = built;
        return built;
    }

    it->second = built;
    substreams_cache_metric_handle.add(1);
    sweepExpiredEntries(substreams_cache, substreams_cache_size_after_sweep, substreams_cache_metric_handle);
    return built;
}

const SharedPartColumnsPtr & SharedPartColumns::getEmpty()
{
    static const SharedPartColumnsPtr empty = []
    {
        auto description = std::make_shared<const ColumnsDescription>();
        return std::make_shared<const SharedPartColumns>(NamesAndTypesList{}, description, description);
    }();
    return empty;
}

const std::shared_ptr<const SerializationByName> & SharedPartColumns::getEmptySerializations()
{
    static const auto empty = std::make_shared<const SerializationByName>();
    return empty;
}

const std::shared_ptr<const ColumnsSubstreams> & SharedPartColumns::getEmptyColumnsSubstreams()
{
    static const auto empty = std::make_shared<const ColumnsSubstreams>();
    return empty;
}

}
