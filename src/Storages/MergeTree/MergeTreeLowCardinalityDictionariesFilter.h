#pragma once

#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct SelectQueryInfo;
class SerializationLowCardinality;
struct IndexDescription;

/// Prunes parts by the dictionaries of their `LowCardinality` columns, used as skip indexes of type `set` that cover
/// whole parts: a part cannot match the filter if no value of the dictionaries of a column satisfies it.
/// Only `Wide` parts with dictionaries that contain all values of the column are pruned,
/// see `SerializationLowCardinality::deserializeCompleteDictionaries`.
///
/// It is created once per query. Like skip indexes, it is used right before a part is read, by `MergeTreeSkipIndexReader`,
/// if the read supports it (see `ReadFromMergeTree::supportsLowCardinalityDictionariesOnDataRead`), and by index analysis
/// otherwise (see `MergeTreeDataSelectExecutor::filterPartsByLowCardinalityDictionaries`). In both cases it is used after
/// the query condition cache, the primary key and the skip indexes, so the dictionaries are not read for the parts they
/// exclude, and on data read, also not for the parts that a query with `LIMIT` does not reach.
class MergeTreeLowCardinalityDictionariesFilter
{
public:
    /// Returns nullptr if the filter does not use the dictionaries of any column, or the query cannot use the dictionaries.
    static std::shared_ptr<const MergeTreeLowCardinalityDictionariesFilter> create(
        const MergeTreeData & data,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
        const ContextPtr & context);

    MergeTreeLowCardinalityDictionariesFilter(
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
        const ContextPtr & context);

    ~MergeTreeLowCardinalityDictionariesFilter();

    /// Whether the part can match the filter. Reads the dictionaries of the part. Thread-safe.
    bool canMatch(const MergeTreeData::DataPartPtr & part) const;

    /// Reading the dictionaries of a part costs a read per column, which is more than a read of fewer marks could save,
    /// see `min_marks_per_part_to_load_statistics`.
    size_t getMinMarksPerPart() const { return min_marks_per_part; }

    Names getColumnNames() const;

private:
    struct ColumnCondition;
    std::vector<ColumnCondition> conditions;

    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    ContextPtr context;
    UInt64 max_dictionaries_bytes;
    size_t min_marks_per_part;
    LoggerPtr log;
};

using MergeTreeLowCardinalityDictionariesFilterPtr = std::shared_ptr<const MergeTreeLowCardinalityDictionariesFilter>;

}
