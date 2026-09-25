#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <map>
#include <unordered_map>

namespace DB
{

struct MergeTreeSettings;

/// Before choosing the merge algorithm, replace flattenable gathering parents
/// with leaf pairs and re-key skip indexes that were stored under the parent
/// name onto the exact leaf they require. Logs one line per gathering column.
/// Horizontal merge later discards `gathering_columns`.
void tryFlattenGatheringColumns(
    NamesAndTypesList & gathering_columns,
    const NamesAndTypesList & storage_columns,
    const NamesAndTypesList & virtual_columns,
    const MergeTreeDataPartsVector & parts,
    const std::vector<AlterConversionsPtr> & alter_conversions,
    const NameSet & columns_with_statistics_to_rebuild,
    std::unordered_map<String, IndicesDescription> & skip_indexes_by_column,
    const IndicesDescription & text_indexes_to_rebuild,
    LoggerPtr log);

/// Add compressed sizes for flattened gathering leaves. Ordinary storage columns
/// are already covered by `accumulateColumnSizes`.
void addVerticalMergeTupleSubcolumnSizes(
    const NamesAndTypesList & gathering_columns,
    const MergeTreeDataPartsVector & parts,
    std::map<String, UInt64> & column_sizes);

/// Accumulates per-leaf serialization data and commits it to the parent once all
/// consecutive gathering leaves of that parent have been written.
class VerticalMergeTupleSubcolumnsState
{
public:
    void addLeaf(
        const NameAndTypePair & leaf,
        const SerializationInfoByName & leaf_infos);

    bool commitIfComplete(
        const NameAndTypePair * next_column,
        const NamesAndTypesList & storage_columns,
        const MergeTreeMutableDataPartPtr & new_data_part,
        size_t gathered_rows,
        const MergeTreeSettings & settings,
        ColumnsSubstreams & gathered_columns_substreams,
        Int32 metadata_version);

    void assertComplete() const;

private:
    SerializationInfoByName pending_leaf_infos{{}};
    String pending_parent;
};

}
