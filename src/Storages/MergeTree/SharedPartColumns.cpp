#include <Storages/MergeTree/SharedPartColumns.h>

#include <base/scope_guard.h>
#include <DataTypes/DataTypeCustom.h>
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

/// The recursive kind encoding of an all-default serialization info of each column's type.
/// It depends only on the type structure (the settings do not affect the encoded bytes).
std::vector<String> buildDefaultKindEncodings(const NamesAndTypesList & columns)
{
    std::vector<String> encodings;
    encodings.reserve(columns.size());
    for (const auto & column : columns)
    {
        WriteBufferFromOwnString out;
        column.type->createSerializationInfo({})->serialializeKindStackBinary(out);
        encodings.push_back(std::move(out.str()));
    }
    return encodings;
}

/// `ratio_of_defaults_for_sparse`, `choose_kind` and `compute_exact_num_defaults` only steer how
/// serialization kinds are chosen and how statistics are collected at write time; with the kind
/// stacks fixed by the key they do not affect the serializations that are built (only the version
/// fields do, see `IDataType::getSerialization`). Normalize them in the cache keys to the values
/// `SerializationInfoByName::readJSON` reconstructs, so that parts loaded from disk (which lose
/// the write-time values) share with freshly written parts that carry the live table settings.
SerializationInfoSettings normalizeSettingsForKey(SerializationInfoSettings settings)
{
    settings.ratio_of_defaults_for_sparse = 1.0;
    settings.choose_kind = false;
    settings.compute_exact_num_defaults = false;
    return settings;
}

template <typename Cache>
size_t eraseExpiredEntries(Cache & cache, AggregatedMetrics::GlobalSum & metric)
{
    size_t erased = std::erase_if(cache, [](const auto & entry) { return entry.second.expired(); });
    metric.sub(erased);
    return cache.size();
}

/// Amortized cleanup of expired entries on insertion: they are erased only when the map has
/// doubled since the last sweep, so the cost stays O(1) per insertion and the map size stays
/// within a constant factor of the number of live entries. Expiration without insertion is
/// handled by `onPartRelease`.
template <typename Cache>
void sweepExpiredEntries(Cache & cache, size_t & size_after_sweep, AggregatedMetrics::GlobalSum & metric)
{
    if (cache.size() < std::max<size_t>(16, size_after_sweep * 2))
        return;

    size_after_sweep = eraseExpiredEntries(cache, metric);
}

}

SharedPartColumns::SharedPartColumns(
    NamesAndTypesList columns_,
    std::shared_ptr<const ColumnsDescription> columns_description_,
    std::shared_ptr<const ColumnsDescription> columns_description_with_collected_nested_,
    bool collect_nested_,
    String interning_key_)
    : columns(std::move(columns_))
    , column_name_to_position(buildColumnPositions(columns))
    , columns_description(std::move(columns_description_))
    , columns_description_with_collected_nested(std::move(columns_description_with_collected_nested_))
    , collect_nested(collect_nested_)
    , interning_key(std::move(interning_key_))
    , serializations_cache_metric_handle(CurrentMetrics::SharedPartSerializationsCacheSize)
    , serialization_groups_metric_handle(CurrentMetrics::SharedPartSerializationGroupsCacheSize)
    , substreams_cache_metric_handle(CurrentMetrics::SharedPartColumnsSubstreamsCacheSize)
    , substream_entries_metric_handle(CurrentMetrics::SharedPartColumnSubstreamsEntriesCacheSize)
    , default_kind_encodings(buildDefaultKindEncodings(columns))
{
}

String SharedPartColumns::describeColumns(const NamesAndTypesList & columns)
{
    /// Everything is length-framed, so no description can be read as another one.
    WriteBufferFromOwnString out;
    for (const auto & column : columns)
    {
        writeStringBinary(column.name, out);
        writeStringBinary(column.type->getName(), out);

        /// One entry per type node, empty when it has no custom serialization, so that the identity of a
        /// node cannot be read as the identity of another one at a different position.
        auto describe_custom_serialization = [&](const IDataType & type)
        {
            const auto * custom = type.getCustomSerialization();
            writeStringBinary(custom ? custom->getCustomSerializationIdentity() : "", out);
        };
        describe_custom_serialization(*column.type);
        column.type->forEachChild(describe_custom_serialization);
    }
    return out.str();
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

std::vector<SharedPartColumns::SerializationGroupKey> SharedPartColumns::buildSerializationGroupKeys(const SerializationInfoByName & infos) const
{
    std::vector<SerializationGroupKey> keys;
    keys.reserve(columns.size());

    UInt32 position = 0;
    for (const auto & column : columns)
    {
        auto it = infos.find(column.name);

        SerializationGroupKey key;
        key.column_position = position;
        key.map_settings = normalizeSettingsForKey(infos.getSettings());
        if (it == infos.end())
        {
            key.kinds = default_kind_encodings[position];
            key.settings = normalizeSettingsForKey(infos.getSettings());
        }
        else
        {
            key.settings = normalizeSettingsForKey(it->second->getSettings());
            WriteBufferFromOwnString kinds;
            it->second->serialializeKindStackBinary(kinds);
            key.kinds = std::move(kinds.str());
        }

        keys.push_back(std::move(key));
        ++position;
    }

    return keys;
}

PartSerializations::ColumnGroupPtr SharedPartColumns::buildSerializationGroup(const NameAndTypePair & column, const SerializationInfoByName & infos) const
{
    auto it = infos.find(column.name);
    auto serialization = it == infos.end()
        ? IDataType::getSerialization(column, infos.getSettings())
        : IDataType::getSerialization(column, *it->second);

    auto group = std::make_shared<PartSerializations::ColumnGroup>();
    group->serializations.push_back(serialization);
    group->names.push_back(column.name);

    IDataType::forEachSubcolumn([&](const auto &, const auto & subname, const auto & subdata)
    {
        auto full_name = Nested::concatenateName(column.name, subname);
        /// Don't override the column serialization with subcolumn serialization if column with the same name exists.
        if (!column_name_to_position.contains(full_name))
        {
            group->names.push_back(std::move(full_name));
            group->serializations.push_back(subdata.serialization);
        }
    }, ISerialization::SubstreamData(serialization));

    /// The group is shared and long-lived: don't keep the growth overshoot of the vectors.
    group->serializations.shrink_to_fit();
    group->names.shrink_to_fit();
    return group;
}

SharedPartColumns::SerializationsCacheKey SharedPartColumns::buildSerializationsCacheKey(const SerializationInfoByName & infos) const
{
    SerializationsCacheKey key{normalizeSettingsForKey(infos.getSettings()), {}, {}};

    /// The kind encodings are one byte per column in the common case; the estimate avoids most of
    /// the buffer growth reallocations without over-reserving much.
    key.kinds.reserve(columns.size() * 4);

    {
        WriteBufferFromString out(key.kinds, AppendModeTag{});

        /// One effective encoding per column, in bundle order (see SerializationsCacheKey).
        /// The kind stacks (whose serialized length is unknown upfront) are length-suffixed,
        /// which keeps the framing injective without a per-column buffer.
        UInt32 position = 0;
        for (const auto & column : columns)
        {
            size_t offset = out.count();
            if (auto it = infos.find(column.name); it != infos.end())
            {
                it->second->serialializeKindStackBinary(out);
                auto entry_settings = normalizeSettingsForKey(it->second->getSettings());
                if (!(entry_settings == key.settings))
                    key.settings_overrides.emplace_back(position, std::move(entry_settings));
            }
            else
            {
                out.write(default_kind_encodings[position].data(), default_kind_encodings[position].size());
            }
            writeVarUInt(out.count() - offset, out);
            ++position;
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
    XXH_INLINE_XXH3_64bits_update(&state, key.kinds.data(), key.kinds.size());

    SipHash settings_hash;
    key.settings.updateHash(settings_hash);
    key.map_settings.updateHash(settings_hash);
    UInt64 settings_hash_value = settings_hash.get64();
    XXH_INLINE_XXH3_64bits_update(&state, &settings_hash_value, sizeof(settings_hash_value));

    return XXH_INLINE_XXH3_64bits_digest(&state);
}

size_t SharedPartColumns::SerializationsCacheKeyHash::operator()(const SerializationsCacheKey & key) const noexcept
{
    /// XXH3 instead of the more usual SipHash: this runs on the part loading path over one
    /// encoding per column, and XXH3 is several times faster (the hash only keys an in-memory
    /// cache, so it does not need to be cryptographic or stable across versions).
    /// The settings go through their `updateHash` so that new fields are picked up automatically;
    /// a stale hash could only miss sharing between equal keys, never share between unequal ones
    /// (equality compares the full key).
    XXH3_state_t state;
    XXH_INLINE_XXH3_64bits_reset(&state);

    SipHash settings_hash;
    key.settings.updateHash(settings_hash);
    for (const auto & [position, override_settings] : key.settings_overrides)
    {
        settings_hash.update(position);
        override_settings.updateHash(settings_hash);
    }
    UInt64 settings_hash_value = settings_hash.get64();
    XXH_INLINE_XXH3_64bits_update(&state, &settings_hash_value, sizeof(settings_hash_value));

    XXH_INLINE_XXH3_64bits_update(&state, key.kinds.data(), key.kinds.size());

    return XXH_INLINE_XXH3_64bits_digest(&state);
}

PartSerializationsPtr SharedPartColumns::getSerializations(const SerializationInfoByName & infos) const
{
    SerializationsCacheKey key;
    std::vector<SerializationGroupKey> group_keys;
    std::vector<PartSerializations::ColumnGroupPtr> groups;

    {
        /// Everything built in this block can outlive the call: on a cache miss the key and the
        /// group keys are moved into the cache maps below, and the groups (with the vector that
        /// holds them) into the part's `PartSerializations`, so allocate them in the parts arena
        /// from the start. Only the transient name lookup draft below stays in the default arena
        /// (the interned copy is what the parts keep).
        ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());

        key = buildSerializationsCacheKey(infos);

        {
            SharedLockGuard lock(serializations_cache_mutex);
            if (auto it = serializations_cache.find(key); it != serializations_cache.end())
                if (auto shared = it->second.lock())
                    return shared;
        }

        /// Whole-object miss: a new combination of serialization kinds. Probe the per-column group
        /// cache before building anything: the keys are cheap to compute (the kind encodings fit in
        /// SSO strings), and when only some columns' kinds changed, the groups of all the others are
        /// reused without constructing a single serialization object.
        group_keys = buildSerializationGroupKeys(infos);
        groups.resize(group_keys.size());

        {
            SharedLockGuard lock(serializations_cache_mutex);
            for (size_t i = 0; i != group_keys.size(); ++i)
                if (auto it = serialization_groups_cache.find(group_keys[i]); it != serialization_groups_cache.end())
                    groups[i] = it->second.lock();
        }

        /// Build the missing groups outside the lock (concurrent loads of parts with distinct
        /// kinds do not serialize each other): the groups live as long as some part of the table
        /// needs them.
        size_t i = 0;
        for (const auto & column : columns)
        {
            if (!groups[i])
                groups[i] = buildSerializationGroup(column, infos);
            ++i;
        }
    }

    /// `supportsPooling() == false` marks a serialization that must not be shared: it keeps mutable
    /// state (`SerializationJSON` accumulates caches inside its extraction tree) or depends on the
    /// settings of the query that built it (its parser, on `allow_simdjson`). Those groups are rebuilt
    /// for every part, as they were before they were interned, so they stay out of both caches: a group
    /// that is not in the group cache can only reach a part through a whole object, which is why one
    /// containing such a group is not interned either.
    std::vector<bool> shareable(groups.size(), true);
    for (size_t i = 0; i != groups.size(); ++i)
    {
        for (const auto & serialization : groups[i]->serializations)
        {
            if (!serialization->supportsPooling())
            {
                shareable[i] = false;
                break;
            }
        }
    }
    const bool shareable_as_a_whole = std::all_of(shareable.begin(), shareable.end(), [](bool s) { return s; });

    /// Assemble the name lookup map from the names stored in the groups (no subcolumn
    /// enumeration). The map is a pure function of the name sequence, so the sequence hash is its
    /// interning key, verified by full content comparison on a hit.
    XXH3_state_t name_sequence_hash;
    XXH_INLINE_XXH3_128bits_reset(&name_sequence_hash);
    size_t total_names = 0;
    for (const auto & group : groups)
    {
        total_names += group->names.size();
        for (const auto & name : group->names)
        {
            UInt64 size = name.size();
            XXH_INLINE_XXH3_128bits_update(&name_sequence_hash, &size, sizeof(size));
            XXH_INLINE_XXH3_128bits_update(&name_sequence_hash, name.data(), name.size());
        }
    }
    auto sequence_hash = XXH_INLINE_XXH3_128bits_digest(&name_sequence_hash);
    UInt128 name_to_slot_hash{sequence_hash.low64, sequence_hash.high64};

    PartSerializations::NameToSlot name_to_slot_draft;
    name_to_slot_draft.reserve(total_names);
    for (UInt32 group_index = 0; group_index != groups.size(); ++group_index)
        for (UInt32 name_index = 0; name_index != groups[group_index]->names.size(); ++name_index)
            /// emplace: the first occurrence of a name wins, as in the flat map this replaces.
            name_to_slot_draft.emplace(groups[group_index]->names[name_index], std::make_pair(group_index, name_index));

    /// Route the rest of what ends up owned by the caches or the parts (the cache map nodes, the
    /// interned name lookup map, the `PartSerializations` object) to the dedicated arena.
    ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());

    std::lock_guard lock(serializations_cache_mutex);

    /// Intern the groups this thread built (a concurrent load may have interned the same keys).
    for (size_t i = 0; i != groups.size(); ++i)
    {
        if (!shareable[i])
            continue;

        auto [it, inserted] = serialization_groups_cache.try_emplace(std::move(group_keys[i]));
        if (!inserted)
        {
            if (auto shared = it->second.lock())
            {
                groups[i] = std::move(shared);
                continue;
            }
            it->second = groups[i];
            continue;
        }
        it->second = groups[i];
        serialization_groups_metric_handle.add(1);
    }
    sweepExpiredEntries(serialization_groups_cache, serialization_groups_size_after_sweep, serialization_groups_metric_handle);

    PartSerializations::NameToSlotPtr name_to_slot;
    {
        auto it = serialization_name_slots_cache.find(name_to_slot_hash);
        if (it != serialization_name_slots_cache.end())
        {
            /// On a hash collision (different content) the map is simply left unshared below.
            /// Correctness never depends on the hash.
            if (auto shared = it->second.lock(); shared && *shared == name_to_slot_draft)
                name_to_slot = std::move(shared);
        }

        if (!name_to_slot)
        {
            /// The draft was built in the default arena; the interned copy (exact capacity)
            /// belongs to the parts arena.
            name_to_slot = std::make_shared<const PartSerializations::NameToSlot>(name_to_slot_draft);
            if (it != serialization_name_slots_cache.end())
                it->second = name_to_slot;
            else
                serialization_name_slots_cache.emplace(name_to_slot_hash, name_to_slot);
        }

        /// This cache holds one entry per distinct name set (almost always exactly one), so
        /// expired entries are swept unconditionally.
        std::erase_if(serialization_name_slots_cache, [](const auto & entry) { return entry.second.expired(); });
    }

    auto built = std::make_shared<const PartSerializations>(std::move(name_to_slot), std::move(groups));

    if (!shareable_as_a_whole)
        return built;

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

            /// The entry is shared and long-lived, but it was built incrementally (e.g. by a part
            /// writer) and may carry container growth overshoot, so what gets interned is a compact copy
            /// of it. Not `make_shared<const ColumnEntry>`: `ColumnsSubstreams` mutates a uniquely held
            /// entry through a `const_cast`, which is only defined when the object itself is not const.
            auto compact = [&] { return std::make_shared<ColumnsSubstreams::ColumnEntry>(*entry); };

            if (auto it = substream_entries_cache.find(entry_hash); it != substream_entries_cache.end())
            {
                if (auto shared = it->second.lock())
                {
                    if (shared->column == entry->column && shared->substreams == entry->substreams)
                        return shared;
                    /// Full 128-bit hash collision between different contents: keep the existing
                    /// entry and leave this one unshared. Correctness never depends on the hash.
                    return entry;
                }

                /// The entry expired: replace it in place, the node is already counted.
                ColumnsSubstreams::ColumnEntryPtr replacement = compact();
                it->second = replacement;
                return replacement;
            }

            /// Build before inserting: a node published to the map is counted, and counting it must not
            /// be able to fail afterwards (an allocation here can throw on a memory limit).
            ColumnsSubstreams::ColumnEntryPtr interned_entry = compact();
            substream_entries_cache.emplace(entry_hash, interned_entry);
            substream_entries_metric_handle.add(1);
            return interned_entry;
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

void SharedPartColumns::onPartRelease() const
{
    /// A part just returned its bundle reference, so interned pieces may have expired. Sweeping costs
    /// O(entries), so wait for enough releases to amortize it, but never for more than can still
    /// happen: the threshold is at most half of the remaining holders, so a draining table always
    /// reaches it again instead of pinning the dead entries until the next insertion.
    /// Not inside `chassert`: it does not evaluate its argument in release builds.
    const UInt64 holders_before_release = holders.fetch_sub(1, std::memory_order_relaxed);
    chassert(holders_before_release > 0);

    if (releases_since_sweep.fetch_add(1, std::memory_order_relaxed) + 1 < release_sweep_threshold.load(std::memory_order_relaxed))
        return;

    /// Exactly one sweeper: concurrent part removals all cross the gate above, and scanning the caches
    /// once per removal would serialize them behind the cache locks. The winner's sweep covers the
    /// releases the others just counted.
    if (sweeping.exchange(true, std::memory_order_acquire))
        return;

    SCOPE_EXIT({ sweeping.store(false, std::memory_order_release); });

    releases_since_sweep.store(0, std::memory_order_relaxed);

    size_t live_entries = 0;

    {
        std::lock_guard lock(serializations_cache_mutex);
        live_entries += eraseExpiredEntries(serializations_cache, serializations_cache_metric_handle);
        serializations_cache_size_after_sweep = serializations_cache.size();
        live_entries += eraseExpiredEntries(serialization_groups_cache, serialization_groups_metric_handle);
        serialization_groups_size_after_sweep = serialization_groups_cache.size();
        std::erase_if(serialization_name_slots_cache, [](const auto & entry) { return entry.second.expired(); });
    }

    {
        std::lock_guard lock(substreams_cache_mutex);
        live_entries += eraseExpiredEntries(substreams_cache, substreams_cache_metric_handle);
        substreams_cache_size_after_sweep = substreams_cache.size();
        live_entries += eraseExpiredEntries(substream_entries_cache, substream_entries_metric_handle);
        substream_entries_size_after_sweep = substream_entries_cache.size();
    }

    /// Both scales matter, so a full drain of P parts sweeps O(log P) times. Read the holders now: the
    /// releases that crossed the gate during the sweep are covered by it.
    const UInt64 remaining_holders = holders.load(std::memory_order_relaxed);
    release_sweep_threshold.store(
        std::max<UInt64>(1, std::min<UInt64>(live_entries, remaining_holders / 2)), std::memory_order_relaxed);
}

const SharedPartColumnsPtr & SharedPartColumns::getEmpty()
{
    static const SharedPartColumnsPtr empty = []
    {
        auto description = std::make_shared<const ColumnsDescription>();
        return std::make_shared<const SharedPartColumns>(NamesAndTypesList{}, description, description, false, String{});
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
