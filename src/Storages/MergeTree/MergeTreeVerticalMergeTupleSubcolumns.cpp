#include <Storages/MergeTree/MergeTreeVerticalMergeTupleSubcolumns.h>

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/MarkInCompressedFile.h>
#include <IO/NullWriteBuffer.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <optional>

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
    extern const MergeTreeSettingsUInt64 vertical_merge_tuple_subcolumns_fat_threshold_bytes;
}

namespace
{

String gatherUnitKindToString(GatherUnit::Kind kind)
{
    switch (kind)
    {
        case GatherUnit::Kind::StorageColumn:
            return "StorageColumn";
        case GatherUnit::Kind::FatLeaf:
            return "FatLeaf";
        case GatherUnit::Kind::TinyLeafBatch:
            return "TinyLeafBatch";
    }
}

GatherUnit makeStorageColumnUnit(const NameAndTypePair & column)
{
    GatherUnit unit;
    unit.id = column.name;
    unit.parent = column.getNameInStorage();
    unit.columns = NamesAndTypesList{column};
    unit.kind = GatherUnit::Kind::StorageColumn;
    return unit;
}

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
        if (!column->statistics.empty())
            return true;
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

size_t maxMarkRows(const IMergeTreeDataPart & part)
{
    if (!part.index_granularity)
        return 0;

    const auto & granularity = *part.index_granularity;
    const size_t marks = granularity.getMarksCountWithoutFinal();
    size_t max_rows = 0;
    for (size_t i = 0; i < marks; ++i)
        max_rows = std::max(max_rows, granularity.getMarkRows(i));
    return max_rows;
}

bool tryFixedWidthChunk(const DataTypePtr & type, size_t max_mark_rows, UInt64 & chunk)
{
    if (const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(type.get()))
    {
        chunk = static_cast<UInt64>(max_mark_rows) * fixed_string->getN();
        return true;
    }

    if (type->isValueRepresentedByNumber() && !type->haveSubtypes())
    {
        chunk = static_cast<UInt64>(max_mark_rows) * type->getSizeOfValueInMemory();
        return true;
    }

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        UInt64 nested_chunk = 0;
        if (!tryFixedWidthChunk(nullable->getNestedType(), max_mark_rows, nested_chunk))
            return false;
        chunk = static_cast<UInt64>(max_mark_rows) + nested_chunk;
        return true;
    }

    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        const auto & dictionary_type = low_cardinality->getDictionaryType();
        DataTypePtr nested = dictionary_type;
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(dictionary_type.get()))
            nested = nullable->getNestedType();

        if (!nested->isValueRepresentedByNumber() || nested->haveSubtypes())
            return false;

        chunk = static_cast<UInt64>(max_mark_rows) * nested->getSizeOfValueInMemory()
            + static_cast<UInt64>(max_mark_rows) * sizeof(UInt64);
        return true;
    }

    return false;
}

bool collectLeafStreamUncompressedSize(
    const NameAndTypePair & leaf,
    const IMergeTreeDataPart & part,
    const MergeTreeSettings & settings,
    UInt64 & uncompressed_size)
{
    auto serialization = leaf.type->getDefaultSerialization();
    ISerialization::EnumerateStreamsSettings enumerate_settings;
    enumerate_settings.enumerate_dynamic_streams = false;

    ISerialization::StreamFileNameSettings file_name_settings(settings);
    bool missing = false;
    UInt64 sum = 0;

    serialization->enumerateStreams(
        enumerate_settings,
        [&](const ISerialization::SubstreamPath & path)
        {
            if (ISerialization::isEphemeralSubcolumn(path, path.size()))
                return;

            const String stream_name = ISerialization::getFileNameForStream(leaf, path, file_name_settings);
            const String file_name = stream_name + IMergeTreeDataPart::DATA_FILE_EXTENSION;
            auto it = part.checksums.files.find(file_name);
            if (it == part.checksums.files.end())
            {
                missing = true;
                return;
            }
            sum += it->second.uncompressed_size;
        },
        ISerialization::SubstreamData(serialization).withType(leaf.type));

    if (missing)
        return false;

    uncompressed_size = sum;
    return true;
}

/// Per-granule working set for one leaf on one Wide part. Returns false if it cannot be estimated.
bool tryLeafWorkingSetOnPart(
    const NameAndTypePair & leaf,
    const IMergeTreeDataPart & part,
    const MergeTreeSettings & settings,
    UInt64 & working_set)
{
    const size_t mark_rows = maxMarkRows(part);
    if (mark_rows == 0)
        return false;

    UInt64 fixed_chunk = 0;
    if (tryFixedWidthChunk(leaf.type, mark_rows, fixed_chunk))
    {
        working_set = fixed_chunk;
        return true;
    }

    UInt64 uncompressed_size = 0;
    if (!collectLeafStreamUncompressedSize(leaf, part, settings, uncompressed_size))
        return false;

    working_set = uncompressed_size;
    return true;
}

bool tryLeafWorkingSet(
    const NameAndTypePair & leaf,
    const MergeTreeDataPartsVector & parts,
    const MergeTreeSettings & settings,
    UInt64 & working_set)
{
    UInt64 max_chunk = 0;
    bool any = false;
    for (const auto & part : parts)
    {
        if (isCompactPart(part))
            continue;

        UInt64 chunk = 0;
        if (!tryLeafWorkingSetOnPart(leaf, *part, settings, chunk))
            return false;
        max_chunk = std::max(max_chunk, chunk);
        any = true;
    }

    if (!any)
        return false;

    working_set = max_chunk;
    return true;
}

std::optional<NameAndTypePair> buildLeafPair(
    const ColumnsDescription & columns,
    const NameAndTypePair & parent,
    const String & leaf_name)
{
    auto pair = columns.tryGetColumn(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns(), leaf_name);
    if (!pair)
        return {};

    if (!pair->isSubcolumn() || pair->getNameInStorage() != parent.name)
    {
        const String prefix = parent.name + ".";
        if (!leaf_name.starts_with(prefix))
            return {};
        return NameAndTypePair(parent.name, leaf_name.substr(prefix.size()), parent.type, pair->type);
    }

    return pair;
}

TupleSubcolumnsClassifyResult classifyOneGatheringColumn(
    const NameAndTypePair & column,
    const MergeTreeSettings & settings,
    const NameSet & merging_names,
    const NameSet & storage_names,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartsVector & parts,
    const MergeTreeDataPartsVector & patch_parts,
    const NameSet & expired_columns)
{
    TupleSubcolumnsClassifyResult result;
    result.units = {makeStorageColumnUnit(column)};

    if (!settings[MergeTreeSetting::allow_experimental_vertical_merge_tuple_subcolumns])
    {
        result.reason = "setting_off";
        return result;
    }

    const UInt64 fat_threshold = settings[MergeTreeSetting::vertical_merge_tuple_subcolumns_fat_threshold_bytes];
    if (fat_threshold == 0)
    {
        result.reason = "fat_threshold_zero";
        return result;
    }

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

    Names leaf_names;
    Nested::flattenTupleLeafNames(column.name, column.type, leaf_names);
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

    std::vector<NameAndTypePair> leaves;
    leaves.reserve(leaf_names.size());
    const auto & columns_desc = metadata_snapshot->getColumns();
    for (const auto & leaf_name : leaf_names)
    {
        auto leaf = buildLeafPair(columns_desc, column, leaf_name);
        if (!leaf)
        {
            result.reason = "cannot_build_leaf_pair";
            return result;
        }
        leaves.push_back(*leaf);
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

    struct ClassifiedLeaf
    {
        NameAndTypePair pair;
        UInt64 working_set = 0;
        bool fat = false;
    };

    std::vector<ClassifiedLeaf> classified;
    classified.reserve(leaves.size());
    UInt64 tiny_sum = 0;
    bool has_fat = false;

    for (const auto & leaf : leaves)
    {
        UInt64 working_set = 0;
        if (!tryLeafWorkingSet(leaf, parts, settings, working_set))
        {
            result.reason = "cannot_estimate_working_set";
            return result;
        }

        ClassifiedLeaf item{leaf, working_set, working_set >= fat_threshold};
        if (item.fat)
            has_fat = true;
        else
            tiny_sum += working_set;
        classified.push_back(std::move(item));
    }

    if (!has_fat)
    {
        result.reason = "no_fat_leaf";
        return result;
    }

    if (tiny_sum >= fat_threshold)
    {
        result.reason = "tiny_batch_at_least_fat_threshold";
        return result;
    }

    result.flatten = true;
    result.reason = "flatten";
    result.units.clear();

    NamesAndTypesList tiny_columns;
    UInt64 tiny_working_set = 0;
    for (const auto & item : classified)
    {
        if (item.fat)
        {
            GatherUnit unit;
            unit.id = item.pair.name;
            unit.parent = column.name;
            unit.columns = NamesAndTypesList{item.pair};
            unit.kind = GatherUnit::Kind::FatLeaf;
            unit.working_set_bytes = item.working_set;
            result.units.push_back(std::move(unit));
        }
        else
        {
            tiny_columns.push_back(item.pair);
            tiny_working_set += item.working_set;
        }
    }

    if (!tiny_columns.empty())
    {
        GatherUnit unit;
        unit.id = column.name + ".#tiny";
        unit.parent = column.name;
        unit.columns = std::move(tiny_columns);
        unit.kind = GatherUnit::Kind::TinyLeafBatch;
        unit.working_set_bytes = tiny_working_set;
        result.units.push_back(std::move(unit));
    }

    return result;
}

void logClassifyResult(LoggerPtr log, const NameAndTypePair & column, const TupleSubcolumnsClassifyResult & result)
{
    std::vector<String> unit_descriptions;
    unit_descriptions.reserve(result.units.size());
    for (const auto & unit : result.units)
    {
        Names names;
        for (const auto & pair : unit.columns)
            names.push_back(pair.name);
        unit_descriptions.push_back(fmt::format(
            "{}:{}:{}",
            gatherUnitKindToString(unit.kind),
            fmt::join(names, ","),
            unit.working_set_bytes));
    }

    LOG_DEBUG(
        log,
        "Vertical merge tuple subcolumns classify: column='{}' flatten={} reason='{}' units=[{}]",
        column.name,
        result.flatten,
        result.reason,
        fmt::join(unit_descriptions, "; "));
}

}

std::vector<TupleSubcolumnsClassifyResult> classifyVerticalMergeTupleSubcolumns(
    const MergeTreeSettings & settings,
    const NamesAndTypesList & gathering_columns,
    const NamesAndTypesList & merging_columns,
    const NamesAndTypesList & storage_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartsVector & parts,
    const MergeTreeDataPartsVector & patch_parts,
    const NameSet & expired_columns,
    LoggerPtr log)
{
    if (!settings[MergeTreeSetting::allow_experimental_vertical_merge_tuple_subcolumns])
        return {};

    std::vector<TupleSubcolumnsClassifyResult> results;
    results.reserve(gathering_columns.size());

    const NameSet merging_names = namesOf(merging_columns);
    const NameSet storage_names = namesOf(storage_columns);

    for (const auto & column : gathering_columns)
    {
        auto result = classifyOneGatheringColumn(
            column,
            settings,
            merging_names,
            storage_names,
            metadata_snapshot,
            parts,
            patch_parts,
            expired_columns);
        logClassifyResult(log, column, result);
        results.push_back(std::move(result));
    }

    return results;
}

namespace
{

bool canApplyFlatten(const TupleSubcolumnsClassifyResult & result)
{
    return result.flatten && !result.units.empty();
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

void applyVerticalMergeTupleSubcolumns(
    const std::vector<TupleSubcolumnsClassifyResult> & results,
    NamesAndTypesList & gathering_columns,
    std::vector<GatherUnit> & gathering_units,
    std::unordered_map<String, IndicesDescription> & skip_indexes_by_column,
    LoggerPtr log)
{
    gathering_units.clear();

    if (results.empty() || results.size() != gathering_columns.size())
    {
        gathering_units.reserve(gathering_columns.size());
        for (const auto & column : gathering_columns)
            gathering_units.push_back(makeStorageColumnUnit(column));
        return;
    }

    NamesAndTypesList new_gathering;
    auto result_it = results.begin();
    for (const auto & column : gathering_columns)
    {
        const auto & result = *result_it++;
        if (!canApplyFlatten(result))
        {
            new_gathering.push_back(column);
            gathering_units.push_back(makeStorageColumnUnit(column));
            continue;
        }

        NameSet leaf_names;
        for (const auto & unit : result.units)
        {
            gathering_units.push_back(unit);
            for (const auto & pair : unit.columns)
            {
                leaf_names.insert(pair.name);
                new_gathering.push_back(pair);
            }
        }

        rerouteSkipIndexesOntoLeaves(column.name, leaf_names, skip_indexes_by_column);

        LOG_DEBUG(
            log,
            "Vertical merge tuple subcolumns apply: column='{}' units={} leaves=[{}]",
            column.name,
            result.units.size(),
            fmt::join(leaf_names, ", "));
    }

    gathering_columns = std::move(new_gathering);
}

size_t countFlattenedTupleParentStreams(
    const NameAndTypePair & parent,
    const SerializationPtr & parent_serialization,
    const MergeTreeSettings & settings)
{
    return synthesizeParentColumnsSubstreams(parent, parent_serialization, settings).getTotalSubstreams();
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
