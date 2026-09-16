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

/// Before choosing the merge algorithm, replace flattenable gathering parents
/// with leaf pairs and re-key skip indexes that were stored under the parent
/// name onto the exact leaf they require. Logs one line per gathering column.
/// Horizontal merge later discards `gathering_columns`.
void tryFlattenGatheringColumns(
    const MergeTreeSettings & settings,
    NamesAndTypesList & gathering_columns,
    const NamesAndTypesList & merging_columns,
    const NamesAndTypesList & storage_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartsVector & parts,
    const MergeTreeDataPartsVector & patch_parts,
    const NameSet & expired_columns,
    const NameSet & columns_with_statistics_to_rebuild,
    std::unordered_map<String, IndicesDescription> & skip_indexes_by_column,
    LoggerPtr log);

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
