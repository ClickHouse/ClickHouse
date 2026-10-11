#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

namespace DB
{

struct MergeTreeSettings;
class ColumnsStatistics;
class MergedColumnOnlyOutputStream;

/// Call after `extractMergingAndGatheringColumns` and before `chooseMergeAlgorithm`.
/// No-op unless vertical merge and `allow_experimental_vertical_merge_tuple_subcolumns` are on.
/// Replaces flattenable gathering parents with leaf pairs and re-keys skip indexes
/// that were stored under the parent name onto the exact leaf they require.
void maybeFlattenGatheringColumnsForVerticalMerge(
    const MergeTreeSettings & settings,
    NamesAndTypesList & gathering_columns,
    const NamesAndTypesList & storage_columns,
    const NamesAndTypesList & virtual_columns,
    const MergeTreeDataPartsVector & parts,
    const std::vector<AlterConversionsPtr> & alter_conversions,
    const std::unordered_map<String, ColumnsStatistics> & statistics_to_build_by_part,
    bool merge_may_reduce_rows,
    const IndicesDescription & text_indexes_to_merge,
    const StorageMetadataPtr & metadata_snapshot,
    std::unordered_map<String, IndicesDescription> & skip_indexes_by_column,
    LoggerPtr log);

/// Add compressed sizes for flattened gathering leaves. Ordinary storage columns
/// are already covered by `accumulateColumnSizes`.
void addVerticalMergeTupleSubcolumnSizes(
    const NamesAndTypesList & gathering_columns,
    const MergeTreeDataPartsVector & parts,
    std::map<String, UInt64> & column_sizes);

/// Accumulates per-leaf serialization data and the substreams each leaf writer
/// actually emitted, then commits both to the parent once all consecutive
/// gathering leaves of that parent have been written.
class VerticalMergeTupleSubcolumnsState
{
public:
    void addLeaf(
        const NameAndTypePair & leaf,
        const SerializationInfoByName & leaf_infos,
        const ColumnsSubstreams & leaf_substreams);

    bool commitIfComplete(
        const NameAndTypePair * next_column,
        const NamesAndTypesList & storage_columns,
        const MergeTreeMutableDataPartPtr & new_data_part,
        size_t gathered_rows,
        ColumnsSubstreams & gathered_columns_substreams,
        Int32 metadata_version);

    void assertComplete() const;

private:
    SerializationInfoByName pending_leaf_infos{{}};
    /// Substream file names recorded by each leaf writer, in gathering order.
    std::vector<std::vector<String>> pending_leaf_substreams;
    String pending_parent;
};

/// Checksums and parent metadata for one Vertical gathering unit.
/// Returns true when `columns_written` should increment (once per storage column).
bool finalizeVerticalGatheredColumn(
    const NameAndTypePair & column,
    const NameAndTypePair * next_column,
    MergedColumnOnlyOutputStream & column_to,
    std::shared_ptr<VerticalMergeTupleSubcolumnsState> & state,
    MergeTreeMutableDataPartPtr & new_data_part,
    const NamesAndTypesList & storage_columns,
    MergeTreeDataPartChecksums & gathered_checksums,
    ColumnsSubstreams & gathered_columns_substreams,
    size_t gathered_rows,
    Int32 metadata_version);

}
