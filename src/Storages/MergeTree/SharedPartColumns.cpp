#include <Storages/MergeTree/SharedPartColumns.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/ColumnsDescription.h>
#include <Common/CurrentMetrics.h>
#include <Common/Jemalloc.h>
#include <Common/JemallocMergeTreeArena.h>
#include <Common/SharedLockGuard.h>
#include <Common/SipHash.h>

#include <xxhash.h>

/// With XXH_INLINE_ALL (from contrib/xxHash) every XXH function is marked as unused,
/// so any actual use triggers this warning.
#pragma clang diagnostic ignored "-Wused-but-marked-unused"

namespace CurrentMetrics
{
    extern const Metric SharedPartSerializationsCacheSize;
    extern const Metric SharedPartSerializationGroupsCacheSize;
    extern const Metric SharedPartColumnsSubstreamsCacheSize;
    extern const Metric SharedPartColumnSubstreamsEntriesCacheSize;
}

namespace DB
{

namespace
{

/// The keys view into the names of `columns`, which belongs to the same bundle and outlives the map.
SharedPartColumns::NameToNumber buildColumnPositions(const NamesAndTypesList & columns)
{
    SharedPartColumns::NameToNumber positions;
    positions.reserve(columns.size());
    size_t pos = 0;
    for (const auto & column : columns)
        positions.emplace(std::string_view(column.name), pos++);
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
    , serialization_groups_metric_handle(CurrentMetrics::SharedPartSerializationGroupsCacheSize)
    , substreams_cache_metric_handle(CurrentMetrics::SharedPartColumnsSubstreamsCacheSize)
    , substream_entries_metric_handle(CurrentMetrics::SharedPartColumnSubstreamsEntriesCacheSize)
{
}

SerializationByName PartSerializations::toSerializationByName() const
{
    SerializationByName result;
    result.reserve(name_to_slot->size());
    for (const auto & [name, slot] : *name_to_slot)
        result.emplace(name, groups[slot.first]->serializations[slot.second]);
    return result;
}

const PartSerializations::NameToSlotPtr & PartSerializations::getEmptyNameToSlot()
{
    static const auto empty = std::make_shared<const NameToSlot>();
    return empty;
}

SharedPartColumns::SerializationsBuild SharedPartColumns::buildSerializations(const SerializationInfoByName & infos) const
{
    SerializationsBuild build;
    build.groups.reserve(columns.size());
    build.group_keys.reserve(columns.size());
    build.name_to_slot = std::make_shared<PartSerializations::NameToSlot>();
    /// A lower bound: subcolumns add entries on top, but this avoids the first rehashes.
    build.name_to_slot->reserve(columns.size());

    /// The lookup map is a pure function of its deterministic build sequence, so hashing the
    /// sequence gives a valid interning key for it (verified by full content comparison on a hit).
    XXH3_state_t name_sequence_hash;
    XXH_INLINE_XXH3_128bits_reset(&name_sequence_hash);

    auto add_name = [&](const String & name, UInt32 column_position, UInt32 index_in_group)
    {
        /// emplace: the first occurrence of a name wins, as in the flat map this replaces.
        build.name_to_slot->emplace(name, std::make_pair(column_position, index_in_group));
        UInt64 size = name.size();
        XXH_INLINE_XXH3_128bits_update(&name_sequence_hash, &size, sizeof(size));
        XXH_INLINE_XXH3_128bits_update(&name_sequence_hash, name.data(), name.size());
    };

    UInt32 position = 0;
    for (const auto & column : columns)
    {
        auto it = infos.find(column.name);

        SerializationGroupKey key;
        key.column_position = position;
        if (it == infos.end())
        {
            key.settings = infos.getSettings();
        }
        else
        {
            key.has_info = true;
            key.settings = it->second->getSettings();
            WriteBufferFromOwnString kinds;
            it->second->serialializeKindStackBinary(kinds);
            key.kinds = std::move(kinds.str());
        }

        auto serialization = it == infos.end()
            ? IDataType::getSerialization(column, infos.getSettings())
            : IDataType::getSerialization(column, *it->second);

        auto group = std::make_shared<PartSerializations::ColumnGroup>();
        group->serializations.push_back(serialization);
        add_name(column.name, position, 0);

        IDataType::forEachSubcolumn([&](const auto &, const auto & subname, const auto & subdata)
        {
            auto full_name = Nested::concatenateName(column.name, subname);
            /// Don't override the column serialization with subcolumn serialization if column with the same name exists.
            if (!column_name_to_position.contains(full_name))
            {
                add_name(full_name, position, static_cast<UInt32>(group->serializations.size()));
                group->serializations.push_back(subdata.serialization);
            }
        }, ISerialization::SubstreamData(serialization));

        build.groups.push_back(std::move(group));
        build.group_keys.push_back(std::move(key));
        ++position;
    }

    auto hash = XXH_INLINE_XXH3_128bits_digest(&name_sequence_hash);
    build.name_to_slot_hash = {hash.low64, hash.high64};
    return build;
}

SharedPartColumns::SerializationsCacheKey SharedPartColumns::buildSerializationsCacheKey(const SerializationInfoByName & infos)
{
    SerializationsCacheKey key{infos.getSettings(), {}, {}};
    key.per_entry_settings.reserve(infos.size());

    size_t names_size = 0;
    for (const auto & [column_name, _] : infos)
        names_size += column_name.size();

    /// The kind stacks are one byte per column in the common case; the estimate avoids most of
    /// the buffer growth reallocations without over-reserving much.
    key.names_and_kinds.reserve(names_size + infos.size() * 8);

    {
        WriteBufferFromString out(key.names_and_kinds, AppendModeTag{});

        /// The infos map is ordered by column name, so the key is deterministic. The name is
        /// length-prefixed and the kind stacks (whose serialized length is unknown upfront) are
        /// length-suffixed, which keeps the encoding injective without a per-column buffer.
        for (const auto & [column_name, info] : infos)
        {
            writeStringBinary(column_name, out);
            size_t offset = out.count();
            info->serialializeKindStackBinary(out);
            writeVarUInt(out.count() - offset, out);
            key.per_entry_settings.push_back(info->getSettings());
        }

        out.finalize();
    }

    return key;
}

size_t SharedPartColumns::SerializationGroupKeyHash::operator()(const SerializationGroupKey & key) const noexcept
{
    XXH3_state_t state;
    XXH_INLINE_XXH3_64bits_reset(&state);

    XXH_INLINE_XXH3_64bits_update(&state, &key.column_position, sizeof(key.column_position));
    XXH_INLINE_XXH3_64bits_update(&state, &key.has_info, sizeof(key.has_info));
    XXH_INLINE_XXH3_64bits_update(&state, key.kinds.data(), key.kinds.size());

    SipHash settings_hash;
    key.settings.updateHash(settings_hash);
    UInt64 settings_hash_value = settings_hash.get64();
    XXH_INLINE_XXH3_64bits_update(&state, &settings_hash_value, sizeof(settings_hash_value));

    return XXH_INLINE_XXH3_64bits_digest(&state);
}

size_t SharedPartColumns::SerializationsCacheKeyHash::operator()(const SerializationsCacheKey & key) const noexcept
{
    /// XXH3 instead of the more usual SipHash: this runs on the part loading path over one entry
    /// per column with a serialization info, and XXH3 is several times faster (the hash only keys
    /// an in-memory cache, so it does not need to be cryptographic or stable across versions).
    /// The settings go through their `updateHash` so that new fields are picked up automatically;
    /// a stale hash could only miss sharing between equal keys, never share between unequal ones
    /// (equality compares the full key).
    XXH3_state_t state;
    XXH_INLINE_XXH3_64bits_reset(&state);

    SipHash settings_hash;
    key.settings.updateHash(settings_hash);
    for (const auto & entry_settings : key.per_entry_settings)
        entry_settings.updateHash(settings_hash);
    UInt64 settings_hash_value = settings_hash.get64();
    XXH_INLINE_XXH3_64bits_update(&state, &settings_hash_value, sizeof(settings_hash_value));

    XXH_INLINE_XXH3_64bits_update(&state, key.names_and_kinds.data(), key.names_and_kinds.size());

    return XXH_INLINE_XXH3_64bits_digest(&state);
}

PartSerializationsPtr SharedPartColumns::getSerializations(const SerializationInfoByName & infos) const
{
    auto key = buildSerializationsCacheKey(infos);

    {
        SharedLockGuard lock(serializations_cache_mutex);
        if (auto it = serializations_cache.find(key); it != serializations_cache.end())
            if (auto shared = it->second.lock())
                return shared;
    }

    /// Build outside the lock: when the whole-object cache does not help (each part has a unique
    /// combination of serialization kinds), concurrent part loads still build in parallel, exactly
    /// as they did when the serializations were built per-part.
    /// Everything built here lives as long as some part of the table needs it: route it to the
    /// dedicated parts arena (no-op when the caller already did).
    ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());
    auto build = buildSerializations(infos);

    std::lock_guard lock(serializations_cache_mutex);

    /// Intern the per-column groups and the name lookup map, so that parts whose serialization
    /// kinds differ in some columns still share the groups of every column they agree on (and the
    /// lookup map, which does not depend on the kinds at all).
    for (size_t i = 0; i != build.groups.size(); ++i)
    {
        auto [it, inserted] = serialization_groups_cache.try_emplace(std::move(build.group_keys[i]));
        if (!inserted)
        {
            if (auto shared = it->second.lock())
            {
                build.groups[i] = std::move(shared);
                continue;
            }
            it->second = build.groups[i];
            continue;
        }
        it->second = build.groups[i];
        serialization_groups_metric_handle.add(1);
    }
    sweepExpiredEntries(serialization_groups_cache, serialization_groups_size_after_sweep, serialization_groups_metric_handle);

    PartSerializations::NameToSlotPtr name_to_slot = std::move(build.name_to_slot);
    {
        auto [it, inserted] = serialization_name_slots_cache.try_emplace(build.name_to_slot_hash);
        if (!inserted)
        {
            /// The sequence hash is verified by full content comparison: on a mismatch (a hash
            /// collision) the map is simply left unshared. Correctness never depends on the hash.
            if (auto shared = it->second.lock())
            {
                if (*shared == *name_to_slot)
                    name_to_slot = std::move(shared);
            }
            else
                it->second = name_to_slot;
        }
        else
            it->second = name_to_slot;

        /// This cache holds one entry per distinct name set (almost always exactly one), so
        /// expired entries are swept unconditionally.
        std::erase_if(serialization_name_slots_cache, [](const auto & entry) { return entry.second.expired(); });
    }

    auto built = std::make_shared<const PartSerializations>(std::move(name_to_slot), std::move(build.groups));

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

std::shared_ptr<const ColumnsSubstreams> SharedPartColumns::internColumnsSubstreams(const ColumnsSubstreams & substreams) const
{
    UInt128 content_hash = substreams.getHash();

    {
        SharedLockGuard lock(substreams_cache_mutex);
        if (auto it = substreams_cache.find(content_hash); it != substreams_cache.end())
            if (auto shared = it->second.lock(); shared && *shared == substreams)
                return shared;
    }

    /// The first part with this content pays for the copies; they are shared from then on.
    /// Route them to the dedicated parts arena (no-op when the caller already did).
    ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());

    /// Intern the per-column entries first, so that parts whose substream sets differ in some
    /// columns (and therefore never match the whole-object cache) still share the entries of
    /// every column they agree on. The copy below is then only a vector of shared pointers.
    ColumnsSubstreams interned = substreams;

    std::vector<UInt128> entry_hashes;
    interned.internColumnEntries([&](const ColumnsSubstreams::ColumnEntryPtr & entry)
    {
        entry_hashes.push_back(ColumnsSubstreams::getColumnEntryHash(*entry));
        return entry;
    });

    {
        std::lock_guard lock(substreams_cache_mutex);
        size_t entry_index = 0;
        interned.internColumnEntries([&](const ColumnsSubstreams::ColumnEntryPtr & entry) TSA_NO_THREAD_SAFETY_ANALYSIS
        {
            UInt128 entry_hash = entry_hashes[entry_index++];
            auto [it, inserted] = substream_entries_cache.try_emplace(entry_hash);
            if (!inserted)
            {
                if (auto shared = it->second.lock())
                {
                    if (shared->column == entry->column && shared->substreams == entry->substreams)
                        return shared;
                    /// Full 128-bit hash collision between different contents: keep the existing
                    /// entry and leave this one unshared. Correctness never depends on the hash.
                    return entry;
                }
                it->second = entry;
                return entry;
            }
            it->second = entry;
            substream_entries_metric_handle.add(1);
            return entry;
        });
        sweepExpiredEntries(substream_entries_cache, substream_entries_size_after_sweep, substream_entries_metric_handle);
    }

    auto built = std::make_shared<const ColumnsSubstreams>(std::move(interned));

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

const PartSerializationsPtr & SharedPartColumns::getEmptySerializations()
{
    static const auto empty = std::make_shared<const PartSerializations>();
    return empty;
}

const std::shared_ptr<const ColumnsSubstreams> & SharedPartColumns::getEmptyColumnsSubstreams()
{
    static const auto empty = std::make_shared<const ColumnsSubstreams>();
    return empty;
}

}
