#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Common/AggregatedMetrics.h>
#include <Common/SharedMutex.h>
#include <base/defines.h>

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
/// depend on the per-part serialization kinds (the `serializations` map and `columns_substreams`,
/// which differ between parts when columns are sparse in one part and dense in another) are interned
/// in secondary caches nested inside the bundle, so they deduplicate across all parts whose
/// serialization kinds coincide, independently of the per-part row statistics stored in
/// `SerializationInfo::Data`. `serialization_infos` itself is per-part data (it holds the row/default
/// counters of the part) and stays in the part.
///
/// Everything here is immutable after construction; the nested caches only intern immutable values.
/// The nested caches hold `weak_ptr`s (entries expire when the last part holding them dies) and
/// expired entries are swept lazily on insertion, so they cannot leak and need no manual eviction
/// thresholds. Lifetime of the bundle itself is managed by the owning `MergeTreeData` cache.
class SharedPartColumns
{
public:
    /// The keys are views into the names of the `columns` member of the owning bundle, which is
    /// immutable and outlives the map, so the names are not duplicated.
    using NameToNumber = std::unordered_map<std::string_view, size_t>;

    SharedPartColumns(
        NamesAndTypesList columns_,
        std::shared_ptr<const ColumnsDescription> columns_description_,
        std::shared_ptr<const ColumnsDescription> columns_description_with_collected_nested_);

    const NamesAndTypesList columns;
    const NameToNumber column_name_to_position;
    const std::shared_ptr<const ColumnsDescription> columns_description;
    /// Aliases `columns_description` when `Nested::collect` produces no distinct list
    /// (or when the `share_nested_offsets` setting is disabled).
    const std::shared_ptr<const ColumnsDescription> columns_description_with_collected_nested;

    /// Returns the serializations map for the given serialization infos, shared across all parts of
    /// the table whose infos produce the same serializations (same kinds and settings, regardless of
    /// the per-part row statistics). The map is built outside the cache lock, so concurrent loads of
    /// parts with distinct serialization kinds do not serialize each other.
    std::shared_ptr<const SerializationByName> getSerializations(const SerializationInfoByName & infos) const;

    /// Returns a shared copy of the given columns substreams, deduplicated by content across all
    /// parts of the table with this column list. Copies the substreams only when no part holds
    /// equal content yet.
    std::shared_ptr<const ColumnsSubstreams> internColumnsSubstreams(const ColumnsSubstreams & substreams) const;

    /// The bundle installed in parts before `setColumns` is called (and in parts that never store
    /// columns). Not interned in any cache.
    static const std::shared_ptr<const SharedPartColumns> & getEmpty();

    /// Shared empty values installed in parts before their metadata is set.
    static const std::shared_ptr<const SerializationByName> & getEmptySerializations();
    static const std::shared_ptr<const ColumnsSubstreams> & getEmptyColumnsSubstreams();

private:
    /// Builds the serializations map for this column list from the given infos.
    SerializationByName buildSerializations(const SerializationInfoByName & infos) const;

    /// Everything `buildSerializations` reads from the infos besides the column list:
    /// the serialization kinds and the settings, but not the per-part `SerializationInfo::Data`.
    /// The kinds are captured with `SerializationInfo::serialializeKindStackBinary`, which encodes
    /// the whole recursive structure (e.g. `SerializationInfoTuple` includes the kind stack of
    /// every element) — the top-level kind stack alone is not enough: two parts can have identical
    /// top-level kinds while a tuple element is sparse in one and dense in the other.
    struct SerializationsCacheKey
    {
        SerializationInfoSettings settings;
        /// The name and the serialized recursive kind stacks of every column with a serialization
        /// info entry, in map order, framed unambiguously into a single string (see
        /// `buildSerializationsCacheKey`) so that building and comparing a key does one string
        /// allocation instead of two per column.
        String names_and_kinds;
        /// The settings of each entry, in the same order.
        std::vector<SerializationInfoSettings> per_entry_settings;

        bool operator==(const SerializationsCacheKey & other) const = default;
    };

    struct SerializationsCacheKeyHash
    {
        size_t operator()(const SerializationsCacheKey & key) const noexcept;
    };

    static SerializationsCacheKey buildSerializationsCacheKey(const SerializationInfoByName & infos);

    mutable SharedMutex serializations_cache_mutex;
    mutable std::unordered_map<SerializationsCacheKey, std::weak_ptr<const SerializationByName>, SerializationsCacheKeyHash>
        serializations_cache TSA_GUARDED_BY(serializations_cache_mutex);
    mutable size_t serializations_cache_size_after_sweep TSA_GUARDED_BY(serializations_cache_mutex) = 0;
    mutable AggregatedMetrics::GlobalSum serializations_cache_metric_handle;

    /// The key is already a hash of the content.
    struct SubstreamsCacheKeyHash
    {
        size_t operator()(const UInt128 & key) const noexcept { return static_cast<size_t>(key); }
    };

    mutable SharedMutex substreams_cache_mutex;
    mutable std::unordered_map<UInt128, std::weak_ptr<const ColumnsSubstreams>, SubstreamsCacheKeyHash>
        substreams_cache TSA_GUARDED_BY(substreams_cache_mutex);
    mutable size_t substreams_cache_size_after_sweep TSA_GUARDED_BY(substreams_cache_mutex) = 0;
    mutable AggregatedMetrics::GlobalSum substreams_cache_metric_handle;
};

using SharedPartColumnsPtr = std::shared_ptr<const SharedPartColumns>;

}
