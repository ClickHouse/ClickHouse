#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Common/AggregatedMetrics.h>
#include <Common/SharedMutex.h>
#include <base/defines.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

class ColumnsDescription;

/// Schema-derived metadata shared by all data parts of a table that store the same set of columns.
///
/// Each active data part used to carry its own copy of this metadata (`columns`, the name-to-position
/// map, the `ColumnsDescription`s, the `serializations` map and `columns_substreams`), all built in
/// `IMergeTreeDataPart::setColumns` and immutable afterwards. All of it scales with the number of
/// columns and is identical across parts with the same stored schema, so for tables with many parts
/// (and especially wide tables) the per-part copies dominated the server RSS. Instead, parts now hold
/// a `shared_ptr` to one immutable `SharedPartColumns` bundle interned in a per-`MergeTreeData` cache
/// keyed by the stored column list (see `MergeTreeData::getSharedPartColumnsForColumns`).
///
/// The members that are a pure function of the column list (`columns`, `column_name_to_position`,
/// `columns_description{,_with_collected_nested}`) live directly in the bundle. The members that also
/// depend on the per-part serialization kinds (the `serializations` map and `columns_substreams`) are
/// interned in secondary caches nested inside the bundle, so they deduplicate across all parts whose
/// serialization kinds coincide, independently of the per-part row statistics stored in
/// `SerializationInfo::Data`. `serialization_infos` itself is per-part data (it holds the row/default
/// counters of the part) and stays in the part.
///
/// A serialization kind is an `ISerialization::Kind` (`Default`, `Sparse`, `Detached`, `Replicated`),
/// chosen per column when a part is written and stored in its `serialization.json`. Kinds stack
/// (`Detached` over `Sparse` over `Default`) and every subcolumn of a nested type has its own stack, so
/// a column's serialization is identified by a whole recursive kind stack.
///
/// Everything here is immutable after construction; the nested caches only intern immutable values.
/// They hold `weak_ptr`s, so an entry dies with the last part holding its value and nothing can leak.
/// The dead entries are reclaimed by two amortized sweeps: on insertion once a cache has doubled, and
/// on part release (see `onPartRelease`) for tables that only delete. Lifetime of the bundle itself is
/// managed by the owning `MergeTreeData` cache.

/// The serializations of the columns of one data part, assembled from shared pieces:
/// one immutable group per column (the serialization of the column itself followed by the
/// serializations of its subcolumns) and one immutable name -> (column, index) lookup map.
/// The groups depend only on their column and its serialization kinds, so parts whose kinds
/// differ in other columns still share them; the lookup map does not depend on the kinds at all
/// (sparse serialization does not change the set of subcolumn names). All pieces are interned in
/// the owning `SharedPartColumns` bundle.
class PartSerializations
{
public:
    struct ColumnGroup
    {
        /// The serialization of the column itself followed by the serializations of its
        /// subcolumns in enumeration order.
        std::vector<SerializationPtr> serializations;
        /// The lookup name of every serialization above (the column name, then the subcolumn full
        /// names). Stored so that the name lookup map can be assembled from interned groups
        /// without re-enumerating the subcolumns.
        std::vector<String> names;
    };
    using ColumnGroupPtr = std::shared_ptr<const ColumnGroup>;

    /// Column name or subcolumn full name -> (column position, index within the column's group).
    using NameToSlot = std::unordered_map<String, std::pair<UInt32, UInt32>>;
    using NameToSlotPtr = std::shared_ptr<const NameToSlot>;

    PartSerializations() = default;
    PartSerializations(NameToSlotPtr name_to_slot_, std::vector<ColumnGroupPtr> groups_)
        : name_to_slot(std::move(name_to_slot_)), groups(std::move(groups_)) {}

    SerializationPtr tryGet(const String & name) const
    {
        auto it = name_to_slot->find(name);
        if (it == name_to_slot->end())
            return nullptr;
        return groups[it->second.first]->serializations[it->second.second];
    }

    bool empty() const { return groups.empty(); }

    template <typename Callback>
    void forEach(Callback && callback) const
    {
        for (const auto & [name, slot] : *name_to_slot)
            callback(name, groups[slot.first]->serializations[slot.second]);
    }

    /// Materialize a flat map (for consumers that keep their own copy, e.g. part writers).
    SerializationByName toSerializationByName() const;

private:
    NameToSlotPtr name_to_slot = getEmptyNameToSlot();
    std::vector<ColumnGroupPtr> groups;

    static const NameToSlotPtr & getEmptyNameToSlot();
};

using PartSerializationsPtr = std::shared_ptr<const PartSerializations>;

class SharedPartColumns
{
public:
    /// The keys are views into the names of the `columns` member of the owning bundle, which is
    /// immutable and outlives the map, so the names are not duplicated.
    using NameToNumber = std::unordered_map<std::string_view, size_t>;

    SharedPartColumns(
        NamesAndTypesList columns_,
        std::shared_ptr<const ColumnsDescription> columns_description_,
        std::shared_ptr<const ColumnsDescription> columns_description_with_collected_nested_,
        bool collect_nested_,
        String interning_key_);

    /// What makes two column lists interchangeable for a data part, and therefore the interning key of a
    /// bundle: the names of the columns, their full type names and the identities of the custom
    /// serializations attached to them from outside the type. `NamesAndTypesList` equality is not enough:
    /// it compares types with `IDataType::equals`, which ignores custom names at any depth
    /// (`Nullable(Bool)` against `Nullable(UInt8)`) although a part persists the name it was declared
    /// with, and no type name shows a serialization attached by a codec (`Quantized` attaches one).
    static String describeColumns(const NamesAndTypesList & columns);

    const NamesAndTypesList columns;
    const NameToNumber column_name_to_position;
    const std::shared_ptr<const ColumnsDescription> columns_description;
    /// Aliases `columns_description` when `Nested::collect` produces no distinct list
    /// (or when the `share_nested_offsets` setting is disabled).
    const std::shared_ptr<const ColumnsDescription> columns_description_with_collected_nested;
    /// The `share_nested_offsets` value the bundle was built with, which shapes
    /// `columns_description_with_collected_nested`. Read-only, but part of the interning key anyway so
    /// that a bundle can never be shared across two values of it.
    const bool collect_nested;
    /// Stored so that the release lookup rebuilds the key the bundle was interned under.
    const String interning_key;

    /// Returns the serializations for the given serialization infos. The whole object is shared
    /// across parts whose infos produce the same serializations (same kinds and settings,
    /// regardless of the per-part row statistics); when the kind combinations differ, the
    /// per-column groups and the name lookup map inside it are still shared (see
    /// `PartSerializations`). Everything is built outside the cache lock, so concurrent loads of
    /// parts with distinct serialization kinds do not serialize each other.
    PartSerializationsPtr getSerializations(const SerializationInfoByName & infos) const;

    /// Returns a shared copy of the given columns substreams, deduplicated by content across all
    /// parts of the table with this column list. Copies the substreams only when no part holds
    /// equal content yet.
    std::shared_ptr<const ColumnsSubstreams> internColumnsSubstreams(const ColumnsSubstreams & substreams) const;

    /// Reference accounting for the release gate below, called by `SharedPartColumnsHolder`.
    void onPartAcquire() const { holders.fetch_add(1, std::memory_order_relaxed); }

    /// Reclamation hook, called when a part returns its bundle reference (i.e. when interned
    /// pieces may have just died): sweeps the expired entries of the nested caches, gated so the
    /// cost stays amortized. Without it, a table that stops creating parts but keeps deleting them
    /// (TTL, retention) would pin the dead entries until the next insertion.
    void onPartRelease() const;

    /// The bundle installed in parts before `setColumns` is called (and in parts that never store
    /// columns). Not interned in any cache.
    static const std::shared_ptr<const SharedPartColumns> & getEmpty();

    /// Shared empty values installed in parts before their metadata is set.
    static const PartSerializationsPtr & getEmptySerializations();
    static const std::shared_ptr<const ColumnsSubstreams> & getEmptyColumnsSubstreams();

private:
    /// Everything the serializations of one column depend on besides the column itself:
    /// its recursive serialization kind stacks and the effective settings. Exact equality
    /// (not just a hash) because freshly built serialization objects cannot be compared by
    /// content to verify a match.
    struct SerializationGroupKey
    {
        UInt32 column_position = 0;
        /// The effective serialized recursive kind stacks: the entry's when the column has a
        /// serialization info entry, the precomputed all-default encoding of the column's type
        /// otherwise. An entry that only carries default kinds therefore gets the same key as no
        /// entry at all, so old parts written without `serialization.json` share the groups of
        /// newer parts whose columns are simply not sparse.
        String kinds;
        /// The entry settings, or the map settings when there is no entry, with the write-time-only
        /// fields cleared (see `normalizeSettingsForKey`): only the fields that affect the built
        /// serializations participate in the key.
        SerializationInfoSettings settings;
        /// The settings of the whole `SerializationInfoByName`, normalized as above: the effective
        /// settings of every recursive element. `settings` above hides them, because
        /// `SerializationInfoTuple` reports defaults while `DataTypeTuple::getSerialization` builds
        /// each element from that element's info.
        SerializationInfoSettings map_settings;

        bool operator==(const SerializationGroupKey & other) const = default;
    };

    struct SerializationGroupKeyHash
    {
        size_t operator()(const SerializationGroupKey & key) const noexcept;
    };

    /// For caches whose key is already a content hash.
    struct ContentHashKeyHash
    {
        size_t operator()(const UInt128 & key) const noexcept { return static_cast<size_t>(key); }
    };

    /// Builds the serialization group of one column (the group key must be the one built by
    /// `buildSerializationGroupKeys` for that column).
    PartSerializations::ColumnGroupPtr buildSerializationGroup(const NameAndTypePair & column, const SerializationInfoByName & infos) const;

    /// The per-column interning keys for the given infos: everything the group of each column
    /// depends on besides the column itself. Cheap to build (no serialization objects), so the
    /// group cache can be probed before building anything.
    std::vector<SerializationGroupKey> buildSerializationGroupKeys(const SerializationInfoByName & infos) const;

    /// Everything the whole serializations object depends on besides the column list:
    /// the effective serialization kinds and settings of every column, but not the per-part
    /// `SerializationInfo::Data`. The kinds are captured with
    /// `SerializationInfo::serialializeKindStackBinary`, which encodes the whole recursive
    /// structure (e.g. `SerializationInfoTuple` includes the kind stack of every element) — the
    /// top-level kind stack alone is not enough: two parts can have identical top-level kinds
    /// while a tuple element is sparse in one and dense in the other. The kinds are recorded per
    /// column position in the bundle order (default-normalized as in `SerializationGroupKey`), so
    /// the key carries no column names.
    struct SerializationsCacheKey
    {
        /// Normalized as in `SerializationGroupKey`.
        SerializationInfoSettings settings;
        /// Per column in bundle order: `serialializeKindStackBinary` (or the type's all-default encoding when there is
        /// no entry) plus a varuint of its length: `00 01|01 01|00 01 00 03` = dense, sparse, Tuple(sparse, dense).
        String kinds;
        /// The (position, settings) of the entries whose settings differ from the map settings.
        /// Almost always empty.
        std::vector<std::pair<UInt32, SerializationInfoSettings>> settings_overrides;

        bool operator==(const SerializationsCacheKey & other) const = default;
    };

    struct SerializationsCacheKeyHash
    {
        size_t operator()(const SerializationsCacheKey & key) const noexcept;
    };

    SerializationsCacheKey buildSerializationsCacheKey(const SerializationInfoByName & infos) const;

    mutable SharedMutex serializations_cache_mutex;
    mutable std::unordered_map<SerializationsCacheKey, std::weak_ptr<const PartSerializations>, SerializationsCacheKeyHash>
        serializations_cache TSA_GUARDED_BY(serializations_cache_mutex);
    mutable size_t serializations_cache_size_after_sweep TSA_GUARDED_BY(serializations_cache_mutex) = 0;
    mutable AggregatedMetrics::GlobalSum serializations_cache_metric_handle;

    /// Per-column serialization groups and the name lookup maps, interned separately so that parts
    /// whose serialization kinds differ in some columns (and thus miss the whole-object cache
    /// above) still share the groups of every column they agree on and the lookup map (which does
    /// not depend on the kinds at all).
    mutable std::unordered_map<SerializationGroupKey, std::weak_ptr<const PartSerializations::ColumnGroup>, SerializationGroupKeyHash>
        serialization_groups_cache TSA_GUARDED_BY(serializations_cache_mutex);
    mutable size_t serialization_groups_size_after_sweep TSA_GUARDED_BY(serializations_cache_mutex) = 0;
    mutable AggregatedMetrics::GlobalSum serialization_groups_metric_handle;

    mutable std::unordered_map<UInt128, std::weak_ptr<const PartSerializations::NameToSlot>, ContentHashKeyHash>
        serialization_name_slots_cache TSA_GUARDED_BY(serializations_cache_mutex);

    mutable SharedMutex substreams_cache_mutex;
    mutable std::unordered_map<UInt128, std::weak_ptr<const ColumnsSubstreams>, ContentHashKeyHash>
        substreams_cache TSA_GUARDED_BY(substreams_cache_mutex);
    mutable size_t substreams_cache_size_after_sweep TSA_GUARDED_BY(substreams_cache_mutex) = 0;
    mutable AggregatedMetrics::GlobalSum substreams_cache_metric_handle;

    /// Per-column entries of `ColumnsSubstreams`, interned by content: a column's substreams depend
    /// only on that column and its serialization kinds, so parts whose substream sets differ in
    /// other columns (and thus miss the whole-object cache above) still share the entries of every
    /// column they agree on.
    mutable std::unordered_map<UInt128, std::weak_ptr<const ColumnsSubstreams::ColumnEntry>, ContentHashKeyHash>
        substream_entries_cache TSA_GUARDED_BY(substreams_cache_mutex);
    mutable size_t substream_entries_size_after_sweep TSA_GUARDED_BY(substreams_cache_mutex) = 0;
    mutable AggregatedMetrics::GlobalSum substream_entries_metric_handle;

    /// The all-default recursive kind encoding of each column's type, in bundle order: the
    /// effective kind encoding of a column with no serialization info entry (see
    /// `SerializationGroupKey`). Computed once per bundle.
    const std::vector<String> default_kind_encodings;

    /// See `onPartRelease`. `holders` counts the parts holding this bundle, not the cache reference.
    mutable std::atomic<UInt64> holders{0};
    mutable std::atomic<UInt64> releases_since_sweep{0};
    mutable std::atomic<UInt64> release_sweep_threshold{1};
    mutable std::atomic<bool> sweeping{false};
};

using SharedPartColumnsPtr = std::shared_ptr<const SharedPartColumns>;

}
