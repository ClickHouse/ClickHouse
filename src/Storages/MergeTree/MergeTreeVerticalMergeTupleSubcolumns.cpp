#include <Storages/MergeTree/MergeTreeVerticalMergeTupleSubcolumns.h>

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/MarkInCompressedFile.h>
#include <IO/NullWriteBuffer.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

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

bool indexesPinParent(
    const String & parent,
    const NameSet & leaf_names,
    const NameSet & storage_names,
    const NameSet & virtual_names,
    const std::unordered_map<String, IndicesDescription> & skip_indexes_by_column,
    const IndicesDescription & text_indexes_to_merge)
{
    auto indexes_pin_parent = [&](const IndicesDescription & indexes)
    {
        for (const auto & index : indexes)
        {
            if (index.expression
                && expressionPinsParent(index.expression->getRequiredColumns(), parent, leaf_names, storage_names, virtual_names))
                return true;
        }
        return false;
    };

    auto skip_indexes_it = skip_indexes_by_column.find(parent);
    if (skip_indexes_it != skip_indexes_by_column.end() && indexes_pin_parent(skip_indexes_it->second))
        return true;

    return indexes_pin_parent(text_indexes_to_merge);
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

bool anyLeafNameIsAmbiguous(const DataTypePtr & parent_type, const std::vector<NameAndTypePair> & leaves)
{
    /// `getSubcolumnNames` preserves duplicate spellings from distinct serialization paths,
    /// while subcolumn lookup keeps only the first match. A gathering leaf stores only the
    /// spelling, so it is safe to use only when that spelling identifies exactly one path.
    std::unordered_map<String, size_t> subcolumn_name_counts;
    for (const auto & subcolumn_name : parent_type->getSubcolumnNames())
        ++subcolumn_name_counts[subcolumn_name];

    for (const auto & leaf : leaves)
    {
        if (subcolumn_name_counts[leaf.getSubcolumnName()] != 1)
            return true;
    }
    return false;
}

template <typename Part>
bool partCanReadLeafDirectly(const Part & part, const NameAndTypePair & leaf, const String & parent)
{
    auto column = part.tryGetColumn(leaf.name);
    if (!column)
        return false;
    return column->isSubcolumn() && column->getNameInStorage() == parent;
}

bool anySourceOrApplicablePatchCannotReadLeaves(
    const MergeTreeDataPartsVector & parts,
    const std::vector<AlterConversionsPtr> & alter_conversions,
    const std::vector<NameAndTypePair> & leaves,
    const String & parent)
{
    for (const auto & part : parts)
    {
        for (const auto & leaf : leaves)
        {
            if (!partCanReadLeafDirectly(*part, leaf, parent))
                return true;
        }
    }

    const NamesAndTypesList leaf_columns(leaves.begin(), leaves.end());
    for (const auto & conversions : alter_conversions)
    {
        /// Delete-only patches are applicable to every read because they contribute `_row_exists`,
        /// but the reader does not request data columns from them.
        auto data_patches = conversions->getPatchesForColumns(
            leaf_columns,
            /*apply_deleted_mask=*/ false,
            /*record_profile_events=*/ false);
        for (const auto & patch : data_patches)
        {
            for (const auto & leaf : leaves)
            {
                if (!partCanReadLeafDirectly(*patch.part, leaf, parent))
                    return true;
            }
        }
    }

    return false;
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
    std::string_view reason;
    std::vector<NameAndTypePair> leaves{};

    bool shouldFlatten() const { return !leaves.empty(); }
};

TupleSubcolumnsClassifyResult classifyOneGatheringColumn(
    const NameAndTypePair & column,
    const NameSet & storage_names,
    const NameSet & virtual_names,
    const MergeTreeDataPartsVector & parts,
    const std::vector<AlterConversionsPtr> & alter_conversions,
    const NameSet & columns_with_statistics_to_rebuild,
    const std::unordered_map<String, IndicesDescription> & skip_indexes_by_column,
    const IndicesDescription & text_indexes_to_merge)
{
    if (!Nested::tryGetFlattenableTuple(column.type))
        return {.reason = "not_flattenable_tuple"};

    if (anyPartIsCompact(parts))
        return {.reason = "compact_source"};

    std::vector<NameAndTypePair> leaves;
    appendFlattenedLeafPairs(column, /*subcolumn_path=*/ "", column.type, leaves);
    if (leaves.empty())
        return {.reason = "cannot_build_leaf_pair"};

    Names leaf_names;
    leaf_names.reserve(leaves.size());
    for (const auto & leaf : leaves)
        leaf_names.push_back(leaf.name);
    const NameSet leaf_name_set(leaf_names.begin(), leaf_names.end());

    if (anyLeafNameIsAmbiguous(column.type, leaves))
        return {.reason = "ambiguous_leaf_name"};

    if (leafNameCollides(leaf_names, storage_names, column.name))
        return {.reason = "leaf_name_collision"};

    if (indexesPinParent(
            column.name,
            leaf_name_set,
            storage_names,
            virtual_names,
            skip_indexes_by_column,
            text_indexes_to_merge))
        return {.reason = "index_pins_parent"};

    if (columns_with_statistics_to_rebuild.contains(column.name))
        return {.reason = "stats_rebuild_pins_parent"};

    if (anyLeafHasDynamicSubcolumns(leaves))
        return {.reason = "dynamic_subcolumns"};

    if (anySourceOrApplicablePatchCannotReadLeaves(parts, alter_conversions, leaves, column.name))
        return {.reason = "cannot_read_leaf_as_subcolumn"};

    return {.reason = "flatten", .leaves = std::move(leaves)};
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
        result.shouldFlatten(),
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
    NamesAndTypesList & gathering_columns,
    const NamesAndTypesList & storage_columns,
    const NamesAndTypesList & virtual_columns,
    const MergeTreeDataPartsVector & parts,
    const std::vector<AlterConversionsPtr> & alter_conversions,
    const NameSet & columns_with_statistics_to_rebuild,
    std::unordered_map<String, IndicesDescription> & skip_indexes_by_column,
    const IndicesDescription & text_indexes_to_merge,
    LoggerPtr log)
{
    NamesAndTypesList new_gathering;
    const NameSet storage_names = storage_columns.getNameSet();
    const NameSet virtual_names = virtual_columns.getNameSet();

    for (const auto & column : gathering_columns)
    {
        auto result = classifyOneGatheringColumn(
            column,
            storage_names,
            virtual_names,
            parts,
            alter_conversions,
            columns_with_statistics_to_rebuild,
            skip_indexes_by_column,
            text_indexes_to_merge);
        logClassifyResult(log, column, result);

        if (!result.shouldFlatten())
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

void addVerticalMergeTupleSubcolumnSizes(
    const NamesAndTypesList & gathering_columns,
    const MergeTreeDataPartsVector & parts,
    std::map<String, UInt64> & column_sizes)
{
    for (const auto & column : gathering_columns)
    {
        if (!column.isSubcolumn())
            continue;

        UInt64 size = 0;
        for (const auto & part : parts)
            size += part->getSubcolumnSize(column.name).data_compressed;
        column_sizes[column.name] = size;
    }
}

namespace
{

void setDataInexact(SerializationInfo & info, size_t gathered_rows)
{
    SerializationInfo dummy(info.getKindStack(), info.getSettings(), SerializationInfo::Data{gathered_rows, 0, false});
    info.replaceData(dummy);
}

void foldLeafDataIntoParent(
    SerializationInfo & info,
    const DataTypePtr & type,
    const String & current_name,
    const SerializationInfoByName & leaf_infos,
    size_t gathered_rows)
{
    const auto * tuple_type = Nested::tryGetFlattenableTuple(type);
    if (!tuple_type)
    {
        if (auto leaf_info = leaf_infos.tryGet(current_name))
            info.replaceData(*leaf_info);
        else
            setDataInexact(info, gathered_rows);
        return;
    }

    auto * tuple_info = typeid_cast<SerializationInfoTuple *>(&info);
    if (!tuple_info)
        return;

    const auto & element_names = tuple_type->getElementNames();
    const auto & element_types = tuple_type->getElements();
    for (size_t i = 0; i < element_names.size(); ++i)
    {
        const String child_name = Nested::concatenateName(current_name, element_names[i]);
        foldLeafDataIntoParent(*tuple_info->getElementInfo(i), element_types[i], child_name, leaf_infos, gathered_rows);
    }
}

void setTupleNodesInexact(SerializationInfo & info, const DataTypePtr & type, size_t gathered_rows)
{
    const auto * tuple_type = Nested::tryGetFlattenableTuple(type);
    auto * tuple_info = typeid_cast<SerializationInfoTuple *>(&info);
    if (!tuple_type || !tuple_info)
        return;

    setDataInexact(info, gathered_rows);

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

    foldLeafDataIntoParent(*parent_info, parent.type, parent.name, leaf_infos, gathered_rows);

    setTupleNodesInexact(*parent_info, parent.type, gathered_rows);
}

}

void VerticalMergeTupleSubcolumnsState::addLeaf(
    const NameAndTypePair & leaf,
    const SerializationInfoByName & leaf_infos)
{
    const String parent_name = leaf.getNameInStorage();
    if (pending_parent.empty())
        pending_parent = parent_name;
    else if (pending_parent != parent_name)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Flattened Tuple units must be consecutive, got parent {} after {}",
            parent_name,
            pending_parent);

    for (const auto & [name, info] : leaf_infos)
        pending_leaf_infos[name] = info->clone();
}

bool VerticalMergeTupleSubcolumnsState::commitIfComplete(
    const NameAndTypePair * next_column,
    const NamesAndTypesList & storage_columns,
    const MergeTreeMutableDataPartPtr & new_data_part,
    size_t gathered_rows,
    const MergeTreeSettings & settings,
    ColumnsSubstreams & gathered_columns_substreams,
    Int32 metadata_version)
{
    const bool group_complete =
        !next_column
        || !next_column->isSubcolumn()
        || next_column->getNameInStorage() != pending_parent;
    if (!group_complete)
        return false;

    auto parent = storage_columns.tryGetByName(pending_parent);
    if (!parent)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot commit flattened Tuple group: storage column {} is missing",
            pending_parent);

    auto serialization_infos = new_data_part->getSerializationInfos();
    commitFlattenedTupleGroupMetadata(
        *parent,
        new_data_part->getSerialization(pending_parent),
        pending_leaf_infos,
        gathered_rows,
        settings,
        gathered_columns_substreams,
        serialization_infos,
        new_data_part->getColumns().getNames());

    new_data_part->setColumns(new_data_part->getColumns(), serialization_infos, metadata_version);

    pending_leaf_infos = SerializationInfoByName{{}};
    pending_parent.clear();
    return true;
}

void VerticalMergeTupleSubcolumnsState::assertComplete() const
{
    if (!pending_parent.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Flattened Tuple group {} was not committed after the Vertical stage",
            pending_parent);
}

}
