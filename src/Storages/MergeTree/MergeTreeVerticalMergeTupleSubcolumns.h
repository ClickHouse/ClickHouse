#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <unordered_map>

namespace DB
{

struct MergeTreeSettings;

/// Column count for `vertical_merge_algorithm_min_columns_to_activate`.
/// When the experimental setting is off, this is `gathering_columns.size()`.
/// When it is on, a flattenable `Tuple` contributes its top-level elements
/// (nested flattenable `Tuple` is not expanded). Any nested flattenable `Tuple`
/// or dynamic-subcolumn leaf falls back to `gathering_columns.size()`.
size_t countGatheringColumnsForVerticalActivation(
    const NamesAndTypesList & gathering_columns,
    const MergeTreeSettings & settings);

/// After Vertical has been chosen, replace flattenable gathering parents with
/// leaf pairs and re-key skip indexes that were stored under the parent name
/// onto the exact leaf they require. Logs one line per gathering column.
void tryFlattenGatheringColumns(
    const MergeTreeSettings & settings,
    NamesAndTypesList & gathering_columns,
    const NamesAndTypesList & merging_columns,
    const NamesAndTypesList & storage_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartsVector & parts,
    const MergeTreeDataPartsVector & patch_parts,
    const NameSet & expired_columns,
    std::unordered_map<String, IndicesDescription> & skip_indexes_by_column,
    LoggerPtr log);

/// Number of on-disk streams a whole-parent write of `parent` would open.
/// Used so a flattened leaf writer applies the adaptive compress-buffer threshold
/// against the group's stream count, not the leaf writer's 1–3 streams.
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
