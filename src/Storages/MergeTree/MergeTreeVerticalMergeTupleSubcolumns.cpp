#include <Storages/MergeTree/MergeTreeVerticalMergeTupleSubcolumns.h>

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/MarkInCompressedFile.h>
#include <IO/NullWriteBuffer.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/Statistics/Statistics.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_experimental_vertical_merge_tuple_subcolumns;
}

namespace
{

NameSet namesOf(const NamesAndTypesList & columns)
{
    NameSet names;
    for (const auto & column : columns)
        names.insert(column.name);
    return names;
}

bool expressionPinsParent(
    const Names & required_columns,
    const String & parent,
    const NameSet & leaf_names,
    const NameSet & storage_names,
    const NameSet & virtual_names)
{
    for (const auto & required : required_columns)
    {
        String storage_name = storage_names.contains(required) || virtual_names.contains(required)
            ? required
            : String(Nested::getColumnFromSubcolumn(required, storage_names));

        if (storage_name != parent)
            continue;

        if (required == parent || !leaf_names.contains(required))
            return true;
    }
    return false;
}

bool skipOrTextOrStatsPinsParent(
    const StorageMetadataPtr & metadata_snapshot,
    const String & parent,
    const NameSet & leaf_names,
    const NameSet & storage_names)
{
    const NameSet virtual_names;
    const auto & skip_indexes = metadata_snapshot->getSecondaryIndices();
    for (const auto & index : skip_indexes)
    {
        if (!index.expression)
            continue;
        if (expressionPinsParent(index.expression->getRequiredColumns(), parent, leaf_names, storage_names, virtual_names))
            return true;
    }

    if (auto column = metadata_snapshot->getColumns().tryGet(parent))
    {
        /// Skip indexes that need the parent as a whole are handled above. Statistics pin the
        /// parent when building them requires the parent column in the gather pipeline:
        /// Vertical flatten only produces leaf names (`t.a`), so `addBuildStatisticsStep` would
        /// never see `t`. Implicit `basic` on a flattenable `Tuple` does not store min/max or
        /// string length, so it does not pin here: ordinary merges still fold existing part
        /// stats by parent name without reading the column. Merges that must rebuild parent
        /// stats from the gather pipeline pin separately via `columns_with_statistics_to_rebuild`.
        for (const auto & [type, desc] : column->statistics.types_to_desc)
        {
            if (type != StatisticsType::Basic || !desc.is_implicit)
                return true;
            if (canStatisticsTrackMinMax(column->type))
                return true;
            const auto unwrapped = removeLowCardinalityAndNullable(removeNullable(column->type));
            if (isStringOrFixedString(unwrapped))
                return true;
        }
    }

    return false;
}

bool leafNameCollides(const Names & leaf_names, const NameSet & storage_names, const String & parent)
{
    for (const auto & leaf : leaf_names)
    {
        if (storage_names.contains(leaf) && leaf != parent)
            return true;
    }
    return false;
}

bool anyPartIsCompact(const MergeTreeDataPartsVector & parts)
{
    for (const auto & part : parts)
    {
        if (isCompactPart(part))
            return true;
    }
    return false;
}

bool anyLeafHasDynamicSubcolumns(const std::vector<NameAndTypePair> & leaves)
{
    for (const auto & leaf : leaves)
    {
        if (leaf.type->hasDynamicSubcolumns())
            return true;
    }
    return false;
}

bool partCanReadLeafDirectly(const IMergeTreeDataPart & part, const NameAndTypePair & leaf, const String & parent)
{
    auto column = part.tryGetColumn(leaf.name);
    if (!column)
        return false;
    return column->isSubcolumn() && column->getNameInStorage() == parent;
}

bool anySourceOrPatchCannotReadLeaves(
    const MergeTreeDataPartsVector & parts,
    const MergeTreeDataPartsVector & patch_parts,
    const std::vector<NameAndTypePair> & leaves,
    const String & parent)
{
    auto check_parts = [&](const MergeTreeDataPartsVector & source_parts)
    {
        for (const auto & part : source_parts)
        {
            for (const auto & leaf : leaves)
            {
                if (!partCanReadLeafDirectly(*part, leaf, parent))
                    return true;
            }
        }
        return false;
    };

    return check_parts(parts) || check_parts(patch_parts);
}

/// Recurse flattenable `Tuple` nodes and emit one four-arg subcolumn pair per leaf.
void appendFlattenedLeafPairs(
    const NameAndTypePair & parent,
    const String & subcolumn_path,
    const DataTypePtr & type,
    std::vector<NameAndTypePair> & leaves)
{
    const auto * tuple_type = Nested::tryGetFlattenableTuple(type);
    if (!tuple_type)
    {
        if (!subcolumn_path.empty())
            leaves.emplace_back(parent.name, subcolumn_path, parent.type, type);
        return;
    }

    const auto & element_names = tuple_type->getElementNames();
    const auto & element_types = tuple_type->getElements();
    for (size_t i = 0; i < element_names.size(); ++i)
    {
        const String child_path = subcolumn_path.empty()
            ? element_names[i]
            : Nested::concatenateName(subcolumn_path, element_names[i]);
        appendFlattenedLeafPairs(parent, child_path, element_types[i], leaves);
    }
}

struct TupleSubcolumnsClassifyResult
{
    bool flatten = false;
    String reason;
    std::vector<NameAndTypePair> leaves;
};

TupleSubcolumnsClassifyResult classifyOneGatheringColumn(
    const NameAndTypePair & column,
    const NameSet & merging_names,
    const NameSet & storage_names,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartsVector & parts,
    const MergeTreeDataPartsVector & patch_parts,
    const NameSet & expired_columns,
    const NameSet & columns_with_statistics_to_rebuild)
{
    TupleSubcolumnsClassifyResult result;

    if (!Nested::tryGetFlattenableTuple(column.type))
    {
        result.reason = "not_flattenable_tuple";
        return result;
    }

    if (merging_names.contains(column.name))
    {
        result.reason = "in_merging_columns";
        return result;
    }

    if (expired_columns.contains(column.name))
    {
        result.reason = "expired";
        return result;
    }

    if (anyPartIsCompact(parts))
    {
        result.reason = "compact_source";
        return result;
    }

    std::vector<NameAndTypePair> leaves;
    appendFlattenedLeafPairs(column, /*subcolumn_path=*/ "", column.type, leaves);
    if (leaves.empty())
    {
        result.reason = "cannot_build_leaf_pair";
        return result;
    }

    Names leaf_names;
    leaf_names.reserve(leaves.size());
    for (const auto & leaf : leaves)
        leaf_names.push_back(leaf.name);
    const NameSet leaf_name_set(leaf_names.begin(), leaf_names.end());

    if (leafNameCollides(leaf_names, storage_names, column.name))
    {
        result.reason = "leaf_name_collision";
        return result;
    }

    if (skipOrTextOrStatsPinsParent(metadata_snapshot, column.name, leaf_name_set, storage_names))
    {
        result.reason = "index_or_stats_pins_parent";
        return result;
    }

    if (columns_with_statistics_to_rebuild.contains(column.name))
    {
        result.reason = "stats_rebuild_pins_parent";
        return result;
    }

    if (anyLeafHasDynamicSubcolumns(leaves))
    {
        result.reason = "dynamic_subcolumns";
        return result;
    }

    if (anySourceOrPatchCannotReadLeaves(parts, patch_parts, leaves, column.name))
    {
        result.reason = "cannot_read_leaf_as_subcolumn";
        return result;
    }

    result.flatten = true;
    result.reason = "flatten";
    result.leaves = std::move(leaves);
    return result;
}

void logClassifyResult(LoggerPtr log, const NameAndTypePair & column, const TupleSubcolumnsClassifyResult & result)
{
    Names leaf_names;
    leaf_names.reserve(result.leaves.size());
    for (const auto & leaf : result.leaves)
        leaf_names.push_back(leaf.name);

    LOG_DEBUG(
        log,
        "Vertical merge tuple subcolumns classify: column='{}' flatten={} reason='{}' leaves=[{}]",
        column.name,
        result.flatten,
        result.reason,
        fmt::join(leaf_names, ", "));
}

void rerouteSkipIndexesOntoLeaves(
    const String & parent,
    const NameSet & leaf_names,
    std::unordered_map<String, IndicesDescription> & skip_indexes_by_column)
{
    auto it = skip_indexes_by_column.find(parent);
    if (it == skip_indexes_by_column.end())
        return;

    /// Take the parent entry out before inserting leaf keys. `operator[]` may rehash the map
    /// and invalidate `it`; erasing or assigning through that iterator afterwards is UB and
    /// showed up as a NULL deref in `getSkipIndicesColumns` while hashing `column_names`.
    IndicesDescription parent_indexes = std::move(it->second);
    skip_indexes_by_column.erase(it);

    IndicesDescription leftover;
    for (auto & index : parent_indexes)
    {
        if (!index.expression)
        {
            leftover.push_back(std::move(index));
            continue;
        }

        const auto required = index.expression->getRequiredColumns();
        if (required.size() == 1 && leaf_names.contains(required.front()))
            skip_indexes_by_column[required.front()].push_back(std::move(index));
        else
            leftover.push_back(std::move(index));
    }

    if (!leftover.empty())
        skip_indexes_by_column[parent] = std::move(leftover);
}

}

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
    LoggerPtr log)
{
    if (!settings[MergeTreeSetting::allow_experimental_vertical_merge_tuple_subcolumns])
        return;

    NamesAndTypesList new_gathering;
    const NameSet merging_names = namesOf(merging_columns);
    const NameSet storage_names = namesOf(storage_columns);

    for (const auto & column : gathering_columns)
    {
        auto result = classifyOneGatheringColumn(
            column,
            merging_names,
            storage_names,
            metadata_snapshot,
            parts,
            patch_parts,
            expired_columns,
            columns_with_statistics_to_rebuild);
        logClassifyResult(log, column, result);

        if (!result.flatten)
        {
            new_gathering.push_back(column);
            continue;
        }

        NameSet leaf_names;
        for (const auto & leaf : result.leaves)
        {
            leaf_names.insert(leaf.name);
            new_gathering.push_back(leaf);
        }

        rerouteSkipIndexesOntoLeaves(column.name, leaf_names, skip_indexes_by_column);

        LOG_DEBUG(
            log,
            "Vertical merge tuple subcolumns apply: column='{}' leaves=[{}]",
            column.name,
            fmt::join(leaf_names, ", "));
    }

    gathering_columns = std::move(new_gathering);
}

namespace
{

void foldLeafDataIntoParent(
    SerializationInfo & info,
    const DataTypePtr & type,
    const String & current_name,
    const String & leaf_name,
    const SerializationInfo & leaf_info)
{
    if (current_name == leaf_name)
    {
        info.replaceData(leaf_info);
        return;
    }

    const auto * tuple_type = Nested::tryGetFlattenableTuple(type);
    auto * tuple_info = typeid_cast<SerializationInfoTuple *>(&info);
    if (!tuple_type || !tuple_info)
        return;

    const auto & element_names = tuple_type->getElementNames();
    const auto & element_types = tuple_type->getElements();
    for (size_t i = 0; i < element_names.size(); ++i)
    {
        const String child_name = Nested::concatenateName(current_name, element_names[i]);
        if (leaf_name == child_name || leaf_name.starts_with(child_name + "."))
            foldLeafDataIntoParent(*tuple_info->getElementInfo(i), element_types[i], child_name, leaf_name, leaf_info);
    }
}

void setTupleNodesInexact(SerializationInfo & info, const DataTypePtr & type, size_t gathered_rows)
{
    const auto * tuple_type = Nested::tryGetFlattenableTuple(type);
    auto * tuple_info = typeid_cast<SerializationInfoTuple *>(&info);
    if (!tuple_type || !tuple_info)
        return;

    SerializationInfo dummy(info.getKindStack(), info.getSettings(), SerializationInfo::Data{gathered_rows, 0, false});
    info.replaceData(dummy);

    const auto & element_types = tuple_type->getElements();
    for (size_t i = 0; i < element_types.size(); ++i)
        setTupleNodesInexact(*tuple_info->getElementInfo(i), element_types[i], gathered_rows);
}

ColumnsSubstreams synthesizeParentColumnsSubstreams(
    const NameAndTypePair & parent,
    const SerializationPtr & serialization,
    const MergeTreeSettings & settings)
{
    ColumnsSubstreams result;
    result.addColumn(parent.name);

    NullWriteBuffer buf;
    ISerialization::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = [&](const ISerialization::SubstreamPath & path)
    {
        result.addSubstreamToLastColumn(
            ISerialization::getFileNameForStream(parent, path, ISerialization::StreamFileNameSettings(settings)));
        return static_cast<WriteBuffer *>(&buf);
    };
    serialize_settings.stream_mark_getter = [&](const ISerialization::SubstreamPath &)
    {
        return MarkInCompressedFile();
    };

    auto empty_column = parent.type->createColumn();
    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(*empty_column, serialize_settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*empty_column, 0, 0, serialize_settings, state);
    serialization->serializeBinaryBulkStateSuffix(serialize_settings, state);
    return result;
}

}

void commitFlattenedTupleGroupMetadata(
    const NameAndTypePair & parent,
    const SerializationPtr & parent_serialization,
    const SerializationInfoByName & leaf_infos,
    size_t gathered_rows,
    const MergeTreeSettings & settings,
    ColumnsSubstreams & gathered_columns_substreams,
    SerializationInfoByName & part_serialization_infos,
    const Names & storage_column_names)
{
    auto parent_substreams = synthesizeParentColumnsSubstreams(parent, parent_serialization, settings);
    gathered_columns_substreams = ColumnsSubstreams::merge(gathered_columns_substreams, parent_substreams, storage_column_names);

    auto parent_info = part_serialization_infos.tryGet(parent.name);
    if (!parent_info)
    {
        if (leaf_infos.empty())
            return;

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot fold flattened Tuple leaf SerializationInfo: parent {} is missing",
            parent.name);
    }

    for (const auto & [leaf_name, leaf_info] : leaf_infos)
        foldLeafDataIntoParent(*parent_info, parent.type, parent.name, leaf_name, *leaf_info);

    setTupleNodesInexact(*parent_info, parent.type, gathered_rows);
}

}
