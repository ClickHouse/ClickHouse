#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <unordered_map>
#include <vector>

namespace DB
{

struct MergeTreeSettings;

/// Stream-scheduling task of a storage column.
struct GatherUnit
{
    enum class Kind
    {
        StorageColumn,
        FatLeaf,
        TinyLeafBatch,
    };

    String id;
    String parent;
    NamesAndTypesList columns;
    Kind kind = Kind::StorageColumn;
    UInt64 working_set_bytes = 0;
};

struct TupleSubcolumnsClassifyResult
{
    bool flatten = false;
    String reason;
    std::vector<GatherUnit> units;
};

/// Classify flattenable `Tuple` gathering columns after Vertical has been chosen.
/// Does not replace `gathering_columns`. Logs one line per gathering column.
std::vector<TupleSubcolumnsClassifyResult> classifyVerticalMergeTupleSubcolumns(
    const MergeTreeSettings & settings,
    const NamesAndTypesList & gathering_columns,
    const NamesAndTypesList & merging_columns,
    const NamesAndTypesList & storage_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartsVector & parts,
    const MergeTreeDataPartsVector & patch_parts,
    const NameSet & expired_columns,
    LoggerPtr log);

/// Replace a flattenable gathering parent with its FatLeaf pairs and at most one TinyLeafBatch.
/// Also fills `gathering_units` (one StorageColumn unit per unflattened parent). Re-keys skip
/// indexes that were stored under the parent name onto the exact leaf they require.
void applyVerticalMergeTupleSubcolumns(
    const std::vector<TupleSubcolumnsClassifyResult> & results,
    NamesAndTypesList & gathering_columns,
    std::vector<GatherUnit> & gathering_units,
    std::unordered_map<String, IndicesDescription> & skip_indexes_by_column,
    LoggerPtr log);

/// Number of on-disk streams a whole-parent write of `parent` would open.
/// Used so a FatLeaf / TinyLeafBatch writer applies the adaptive compress-buffer
/// threshold against the group's stream count, not the leaf writer's 1–3 streams.
size_t countFlattenedTupleParentStreams(
    const NameAndTypePair & parent,
    const SerializationPtr & parent_serialization,
    const MergeTreeSettings & settings);

/// Synthesize the parent `t` columns_substreams entry and fold leaf SerializationInfo::Data
/// into the existing parent tree. Does not call `setColumns`.
void commitFlattenedTupleGroupMetadata(
    const NameAndTypePair & parent,
    const SerializationPtr & parent_serialization,
    const SerializationInfoByName & leaf_infos,
    size_t gathered_rows,
    const MergeTreeSettings & settings,
    ColumnsSubstreams & gathered_columns_substreams,
    SerializationInfoByName & part_serialization_infos,
    const Names & storage_column_names);

}
