#include <Storages/prepareReadingFromFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Storages/VirtualColumnUtils.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <base/scope_guard.h>
#include <Common/getNumberOfCPUCoresToUse.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool enable_parsing_to_custom_serialization;
}

namespace
{

bool containsCustomNamedType(const IDataType * type)
{
    if (!type)
        return false;

    bool result = type->hasCustomName();
    type->forEachChild([&](const IDataType & child)
    {
        result |= child.hasCustomName();
    });
    return result;
}

void addRequiredColumnsToHeader(Block & header, const ActionsDAG & dag)
{
    for (const auto & col : dag.getRequiredColumns())
    {
        if (header.has(col.name))
            continue;

        header.insert({col.type, col.name});
    }
}

bool canExtractColumnFromFormatHeader(const Block & format_header, const NameAndTypePair & column)
{
    return format_header.has(column.name) || format_header.has(column.getNameInStorage());
}

void addRetainedActionInputs(
    Block & header,
    const Block & header_before_actions,
    const ActionsDAG & dag,
    const NameSet & requested_input_names,
    const std::optional<String> & removed_column_name,
    bool retain_subcolumn_inputs)
{
    for (const auto & col : dag.getRequiredColumns())
    {
        const bool is_requested_input = requested_input_names.contains(col.name);
        const bool is_retained_subcolumn_input = retain_subcolumn_inputs && !Nested::splitName(col.name).second.empty();
        if (!is_requested_input && !is_retained_subcolumn_input)
            continue;
        if (removed_column_name && col.name == *removed_column_name && !is_retained_subcolumn_input)
            continue;
        if (header.has(col.name))
            continue;

        header.insert(header_before_actions.getByName(col.name));
    }
}

Block applyRowLevelFilterHeaderActions(
    Block header,
    const FilterDAGInfoPtr & row_level_filter,
    const NameSet & requested_input_names)
{
    addRequiredColumnsToHeader(header, row_level_filter->actions);
    const Block header_before_actions = header;
    header = SourceStepWithFilter::applyPrewhereActions(std::move(header), row_level_filter, nullptr);
    addRetainedActionInputs(
        header,
        header_before_actions,
        row_level_filter->actions,
        requested_input_names,
        row_level_filter->do_remove_column ? std::optional<String>{row_level_filter->column_name} : std::nullopt,
        false);

    return header;
}

Block applyPrewhereHeaderActions(
    Block header,
    const PrewhereInfoPtr & prewhere_info,
    const NameSet & requested_input_names)
{
    addRequiredColumnsToHeader(header, prewhere_info->prewhere_actions);
    const Block header_before_actions = header;
    header = SourceStepWithFilter::applyPrewhereActions(std::move(header), nullptr, prewhere_info);
    addRetainedActionInputs(
        header,
        header_before_actions,
        prewhere_info->prewhere_actions,
        requested_input_names,
        prewhere_info->remove_prewhere_column ? std::optional<String>{prewhere_info->prewhere_column_name} : std::nullopt,
        true);

    return header;
}

void addInternalFilterInputColumns(Block & filter_input_header, const Block & format_header, const ActionsDAG & dag)
{
    for (const auto & col : dag.getRequiredColumns())
    {
        if (format_header.has(col.name) || filter_input_header.has(col.name))
            continue;

        filter_input_header.insert({col.type, col.name});
    }
}

NameSet getRemovedFilterColumns(const FilterDAGInfoPtr & row_level_filter, const PrewhereInfoPtr & prewhere_info)
{
    NameSet result;
    if (row_level_filter && row_level_filter->do_remove_column)
        result.insert(row_level_filter->column_name);
    if (prewhere_info && prewhere_info->remove_prewhere_column)
        result.insert(prewhere_info->prewhere_column_name);
    return result;
}

void addRetainedFilterOutputs(
    NamesAndTypesList & requested_columns,
    const Block & format_header,
    const ActionsDAG & dag,
    const std::optional<String> & removed_output_name)
{
    for (const auto * output : dag.getOutputs())
    {
        if (removed_output_name && output->result_name == *removed_output_name)
            continue;
        if (requested_columns.contains(output->result_name))
            continue;

        const auto & column = format_header.getByName(output->result_name);
        requested_columns.emplace_back(column.name, column.type);
    }
}

Block buildSourceHeader(
    const NamesAndTypesList & requested_columns,
    const NamesAndTypesList & hive_partition_columns,
    const NamesAndTypesList & requested_virtual_columns)
{
    Block source_header;
    for (const auto & column : requested_columns)
        source_header.insert({column.type->createColumn(), column.type, column.name});
    for (const auto & column : hive_partition_columns)
        source_header.insert({column.type->createColumn(), column.type, column.name});
    for (const auto & column : requested_virtual_columns)
        source_header.insert({column.type->createColumn(), column.type, column.name});
    return source_header;
}

ColumnsDescription buildColumnsDescriptionAfterPrewhere(const ReadFromFormatInfo & info, const Block & format_header)
{
    ColumnsDescription result;
    for (const auto & col : format_header)
    {
        if (info.format_header.has(col.name))
            result.add(info.columns_description.get(col.name));
        else
            result.add(ColumnDescription(col.name, col.type));
    }
    return result;
}

}

ReadFromFormatInfo prepareReadingFromFormat(
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    bool supports_subset_of_columns,
    bool supports_tuple_elements,
    const PrepareReadingFromFormatHiveParams & hive_parameters)
{
    const NamesAndTypesList & columns_in_data_file =
        hive_parameters.file_columns.empty() ? storage_snapshot->metadata->getColumns().getAllPhysical() : hive_parameters.file_columns;
    ReadFromFormatInfo info;
    /// Collect requested virtual columns and remove them from requested columns.
    Strings columns_to_read;
    for (const auto & column_name : requested_columns)
    {
        if (auto virtual_column = storage_snapshot->metadata->virtuals.tryGet(column_name, VirtualsKind::All, VirtualsMaterializationPlace::Reader))
        {
            info.requested_virtual_columns.emplace_back(std::move(*virtual_column));
        }
        else if (auto it = hive_parameters.hive_partition_columns_to_read_from_file_path_map.find(column_name);
                 it != hive_parameters.hive_partition_columns_to_read_from_file_path_map.end())
        {
            info.hive_partition_columns_to_read_from_file_path.emplace_back(it->first, it->second);
        }
        else
            columns_to_read.push_back(column_name);
    }

    info.source_header = storage_snapshot->getSampleBlockForColumns(columns_to_read);

    /// Create header for Source that will contain all requested columns including hive columns (which should be part of the schema) and virtual at the end
    /// (because they will be added to the chunk after reading regular columns).
    /// The order is important, hive partition columns must be added before virtual columns because they are part of the schema
    for (const auto & column_from_file_path : info.hive_partition_columns_to_read_from_file_path)
        info.source_header.insert({column_from_file_path.type->createColumn(), column_from_file_path.type, column_from_file_path.name});

    for (const auto & requested_virtual_column : info.requested_virtual_columns)
        info.source_header.insert({requested_virtual_column.type->createColumn(), requested_virtual_column.type, requested_virtual_column.name});

    /// Set requested columns that should be read from data.
    info.requested_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), columns_to_read);

    if (supports_subset_of_columns)
    {
        if (supports_tuple_elements)
        {
            columns_to_read = filterTupleColumnsToRead(info.requested_columns);
        }
        else if (!columns_to_read.empty())
        {
            /// We need to replace all subcolumns with their nested columns (e.g `a.b`, `a.b.c`, `x.y` -> `a`, `x`),
            /// because most formats cannot extract subcolumns on their own.
            /// All requested subcolumns will be extracted after reading.
            std::unordered_set<String> columns_to_read_set;
            /// Save original order of columns.
            std::vector<String> new_columns_to_read;
            for (const auto & column_to_read : info.requested_columns)
            {
                auto name = column_to_read.getNameInStorage();
                if (!columns_to_read_set.contains(name))
                {
                    columns_to_read_set.insert(name);
                    new_columns_to_read.push_back(name);
                }
            }
            columns_to_read = std::move(new_columns_to_read);
        }

        /// If only virtual columns were requested, just read the smallest column.
        if (columns_to_read.empty())
        {
            columns_to_read.push_back(ExpressionActions::getSmallestColumn(columns_in_data_file).name);
        }

        info.columns_description = storage_snapshot->getDescriptionForColumns(columns_to_read);
    }
    else
    {
        /// If format doesn't support reading subset of columns, read all columns.
        /// Requested columns/subcolumns will be extracted after reading.
        info.columns_description = storage_snapshot->getDescriptionForColumns(columns_in_data_file.getNames());
    }

    /// Create header for InputFormat with columns that will be read from the data.
    for (const auto & column : info.columns_description)
    {
        /// Never read hive partition columns from the data file. This fixes https://github.com/ClickHouse/ClickHouse/issues/87515
        if (!hive_parameters.hive_partition_columns_to_read_from_file_path_map.contains(column.name))
            info.format_header.insert(ColumnWithTypeAndName{column.type, column.name});
    }

    info.serialization_hints = getSerializationHintsForFileLikeStorage(storage_snapshot->metadata, context);

    return info;
}

Names filterTupleColumnsToRead(NamesAndTypesList & requested_columns)
{
    /// Format can read tuple element subcolumns, e.g. `t.x` or `t.a.x`.
    /// Some formats can also read fixed `JSON` / `Dynamic` subcolumns directly.
    /// But we still need to do some processing on the set of requested columns:
    ///  * If a subcolumn isn't one of these supported cases, request the whole parent column.
    ///  * Don't request tuple element if the whole tuple is also requested.
    ///    E.g. `SELECT t, t.x` should just read `t`.

    struct SubcolumnInfo
    {
        ISerialization::SubstreamPath path;
        String name;
        String name_in_storage;
        DataTypePtr type;
        DataTypePtr type_in_storage;
        bool is_duplicate = false;
    };

    std::vector<SubcolumnInfo> columns_info(requested_columns.size());
    std::unordered_map<String, size_t> parent_read_by_storage_name;
    size_t idx = 0;
    for (const auto & column_to_read : requested_columns)
    {
        SCOPE_EXIT({ ++idx; });

        /// Suppose column `t.a.b.c` was requested, and `t` and `t.a` are tuples,
        /// but `t.a.b` is an Object (with dynamic subcolumn `c`). We want to read `t.a.b`.
        /// So, we're looking for the longest prefix of the requested path that consists
        /// only of tuple element accesses. In this example we want path = {a, b}.
        /// (Note that `t.a.b.c` will not be listed by enumerateStreams because `c`
        ///  is a dynamic subcolumn.)
        auto & column_info = columns_info[idx];
        bool found_full_path = false;
        column_info.name_in_storage = column_to_read.getNameInStorage();
        column_info.type = column_to_read.getTypeInStorage();
        column_info.type_in_storage = column_to_read.getTypeInStorage();

        if (column_to_read.isSubcolumn())
        {
            /// Do subcolumn lookup similar to getColumnFromBlock.

            auto type = column_to_read.getTypeInStorage();
            auto subcolumn_name = column_to_read.getSubcolumnName();
            if (!type->hasCustomName())
            {
                auto data = ISerialization::SubstreamData(type->getDefaultSerialization()).withType(type);

                ISerialization::StreamCallback callback_with_data = [&](const auto & subpath)
                {
                    if (found_full_path)
                        return;

                    for (size_t i = 0; i < subpath.size(); ++i)
                    {
                        /// Allow `a.x` where `a` is array of tuples.
                        if (subpath[i].type == ISerialization::Substream::ArrayElements)
                            continue;

                        if (subpath[i].type != ISerialization::Substream::TupleElement)
                            break;

                        if (subpath[i].visited)
                            continue;
                        subpath[i].visited = true;
                        size_t prefix_len = i + 1;
                        if (prefix_len <= column_info.path.size())
                            continue;

                        auto name = ISerialization::getSubcolumnNameForStream(subpath, prefix_len);
                        if (containsCustomNamedType(subpath[i].data.type.get()))
                            continue;

                        if (name == subcolumn_name)
                            found_full_path = true;
                        else if (!subcolumn_name.starts_with(name + "."))
                            continue;

                        column_info.path.insert(column_info.path.end(), subpath.begin() + column_info.path.size(), subpath.begin() + prefix_len);
                        if (found_full_path)
                            break;
                    }
                };

                ISerialization::EnumerateStreamsSettings settings;
                settings.position_independent_encoding = false;
                settings.enumerate_dynamic_streams = false;
                data.serialization->enumerateStreams(settings, callback_with_data, data);
            }

            if (!column_info.path.empty())
                column_info.type = ISerialization::createFromPath(column_info.path, column_info.path.size()).type;
            else if (const auto * object_type = typeid_cast<const DataTypeObject *>(column_to_read.getTypeInStorage().get()))
            {
                if (object_type->getTypedPaths().contains(column_to_read.getSubcolumnName()))
                    column_info.name = column_to_read.name;
            }
        }

        if (column_info.name.empty())
            column_info.name = column_info.name_in_storage;

        if (!column_info.path.empty())
        {
            column_info.name += '.';
            column_info.name += ISerialization::getSubcolumnNameForStream(column_info.path);
        }

        if (column_info.name == column_info.name_in_storage)
            parent_read_by_storage_name.emplace(column_info.name_in_storage, idx);
    }

    for (auto & column_info : columns_info)
    {
        auto it = parent_read_by_storage_name.find(column_info.name_in_storage);
        if (it == parent_read_by_storage_name.end() || column_info.name == column_info.name_in_storage)
            continue;

        column_info.path.clear();
        column_info.name = column_info.name_in_storage;
        column_info.type = column_info.type_in_storage;
    }

    std::unordered_map<String, size_t> name_to_idx;
    for (idx = 0; idx < columns_info.size(); ++idx)
    {
        auto & column_info = columns_info[idx];
        bool emplaced = name_to_idx.emplace(column_info.name, idx).second;
        column_info.is_duplicate = !emplaced;
    }

    std::vector<String> new_columns_to_read;
    idx = 0;
    for (auto & column_to_read : requested_columns)
    {
        SCOPE_EXIT({ ++idx; });

        /// Check if any ancestor subcolumn is requested.
        /// (This is why we iterate over requested_columns twice: first to form name_to_idx,
        ///  then to check this. E.g. consider `SELECT t.x, t.y, t`.)
        bool ancestor_requested = false;
        const auto & column_info = columns_info[idx];
        for (size_t prefix_len = 0; prefix_len < column_info.path.size(); ++prefix_len)
        {
            String ancestor_name = column_to_read.getNameInStorage();
            if (prefix_len)
            {
                ancestor_name += '.';
                ancestor_name += ISerialization::getSubcolumnNameForStream(column_info.path, prefix_len);
            }
            auto it = name_to_idx.find(ancestor_name);
            if (it != name_to_idx.end())
            {
                const auto & ancestor_info = columns_info[it->second];
                const auto & ancestor_type_in_storage = ancestor_name == column_to_read.name ? column_to_read.type : ancestor_info.type;
                column_to_read.setDelimiterAndTypeInStorage(ancestor_name, ancestor_type_in_storage);
                ancestor_requested = true;
                break;
            }
        }
        if (ancestor_requested)
            continue;

        const auto & type_in_storage = column_info.name == column_to_read.name ? column_to_read.type : column_info.type;
        column_to_read.setDelimiterAndTypeInStorage(column_info.name, type_in_storage);
        if (!column_info.is_duplicate)
            new_columns_to_read.push_back(column_info.name);
    }
    return new_columns_to_read;

    /// (Not checking columns_to_read.empty() in this case, assuming that formats with
    ///  supports_tuple_elements also support empty list of columns.)
}

NameSet getSupportedPrewhereColumnsForFormat(
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context,
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    const NamesAndTypesList & exclude)
{
    NameSet names = metadata_snapshot->getColumnsWithoutDefaultExpressions(exclude);

    /// Direct subcolumn reads in the file-like `PREWHERE` path are currently a `Parquet` reader feature.
    if (format_name != "Parquet"
        || !FormatFactory::instance().checkIfFormatSupportsPrewhere(format_name, context, format_settings))
        return names;

    const auto exclude_map = exclude.getNameToTypeMap();
    const auto columns_with_subcolumns = metadata_snapshot->columns.get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns());
    for (const auto & column : columns_with_subcolumns)
    {
        if (!column.isSubcolumn())
            continue;

        const auto name_in_storage = column.getNameInStorage();
        const auto * column_in_storage = metadata_snapshot->columns.tryGet(name_in_storage);
        if (!column_in_storage || column_in_storage->default_desc.expression || exclude_map.contains(name_in_storage))
            continue;

        NamesAndTypesList requested_column{column};
        filterTupleColumnsToRead(requested_column);
        const auto & readable_column = requested_column.front();
        if (readable_column.getNameInStorage() != name_in_storage)
            names.insert(column.name);
    }

    return names;
}

ReadFromFormatInfo updateFormatPrewhereInfo(
    const ReadFromFormatInfo & info,
    const FilterDAGInfoPtr & row_level_filter,
    const PrewhereInfoPtr & prewhere_info)
{
    chassert(prewhere_info || row_level_filter);

    if (info.prewhere_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "updateFormatPrewhereInfo called more than once");

    ReadFromFormatInfo new_info = info;
    new_info.prewhere_info = prewhere_info;
    new_info.row_level_filter = row_level_filter;

    /// `ActionsDAG::updateHeader` removes columns used as inputs unless they are outputs.
    /// Keep requested input columns explicitly and put filter-only inputs into
    /// `format_filter_input_header`.
    Block format_header = info.format_header;
    const NameSet requested_input_names = info.requested_columns.getNameSet();
    const FilterDAGInfoPtr row_level_filter_to_apply = row_level_filter && !info.row_level_filter ? row_level_filter : nullptr;

    if (row_level_filter_to_apply)
        format_header = applyRowLevelFilterHeaderActions(std::move(format_header), row_level_filter_to_apply, requested_input_names);
    if (prewhere_info)
    {
        /// `Parquet` can defer `PREWHERE` on `VARIANT` subcolumns until after chunk delivery.
        /// Those inputs must stay in `format_header`, because `format_filter_input_header` is not delivered in that chunk.
        format_header = applyPrewhereHeaderActions(std::move(format_header), prewhere_info, requested_input_names);
    }
    new_info.format_header = std::move(format_header);

    new_info.format_filter_input_header = {};
    if (row_level_filter)
        addInternalFilterInputColumns(new_info.format_filter_input_header, new_info.format_header, row_level_filter->actions);
    if (prewhere_info)
        addInternalFilterInputColumns(new_info.format_filter_input_header, new_info.format_header, prewhere_info->prewhere_actions);

    const auto removed_filter_columns = getRemovedFilterColumns(row_level_filter, prewhere_info);
    new_info.requested_columns = {};
    for (const auto & column : info.requested_columns)
    {
        if (removed_filter_columns.contains(column.name))
            continue;
        if (canExtractColumnFromFormatHeader(new_info.format_header, column))
            new_info.requested_columns.push_back(column);
    }

    if (row_level_filter)
        addRetainedFilterOutputs(
            new_info.requested_columns,
            new_info.format_header,
            row_level_filter->actions,
            row_level_filter->do_remove_column ? std::optional<String>{row_level_filter->column_name} : std::nullopt);

    if (prewhere_info)
        addRetainedFilterOutputs(
            new_info.requested_columns,
            new_info.format_header,
            prewhere_info->prewhere_actions,
            prewhere_info->remove_prewhere_column ? std::optional<String>{prewhere_info->prewhere_column_name} : std::nullopt);

    new_info.source_header = buildSourceHeader(
        new_info.requested_columns,
        new_info.hive_partition_columns_to_read_from_file_path,
        new_info.requested_virtual_columns);
    new_info.columns_description = buildColumnsDescriptionAfterPrewhere(info, new_info.format_header);

    return new_info;
}

SerializationInfoByName getSerializationHintsForFileLikeStorage(const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context)
{
    if (!context->getSettingsRef()[Setting::enable_parsing_to_custom_serialization])
        return SerializationInfoByName{{}};

    auto insertion_table = context->getInsertionTable();
    if (!insertion_table)
        return SerializationInfoByName{{}};

    auto storage_ptr = DatabaseCatalog::instance().tryGetTable(insertion_table, context);
    if (!storage_ptr)
        return SerializationInfoByName{{}};

    const auto storage_metadata_snapshot = storage_ptr->getInMemoryMetadataPtr(context, false);
    const auto & our_columns = metadata_snapshot->getColumns();
    const auto & storage_columns = storage_metadata_snapshot->getColumns();
    auto storage_hints = storage_ptr->getSerializationHints();
    SerializationInfoByName res({});

    for (const auto & hint : storage_hints)
    {
        if (our_columns.tryGetPhysical(hint.first) == storage_columns.tryGetPhysical(hint.first))
            res.insert(hint);
    }

    return res;
}

void ReadFromFormatInfo::serialize(IQueryPlanStep::Serialization & ctx) const
{
    source_header.getNamesAndTypesList().writeTextWithNamesInStorage(ctx.out);
    format_header.getNamesAndTypesList().writeTextWithNamesInStorage(ctx.out);
    format_filter_input_header.getNamesAndTypesList().writeTextWithNamesInStorage(ctx.out);
    writeStringBinary(columns_description.toString(false), ctx.out);
    requested_columns.writeTextWithNamesInStorage(ctx.out);
    requested_virtual_columns.writeTextWithNamesInStorage(ctx.out);
    serialization_hints.writeJSON(ctx.out);

    ctx.out << "\n";

    hive_partition_columns_to_read_from_file_path.writeTextWithNamesInStorage(ctx.out);
    writeBinary(prewhere_info != nullptr, ctx.out);
    if (prewhere_info != nullptr)
        prewhere_info->serialize(ctx);

    ctx.out << "\n";
}

ReadFromFormatInfo ReadFromFormatInfo::deserialize(IQueryPlanStep::Deserialization & ctx)
{
    ReadFromFormatInfo result;

    NamesAndTypesList source_header_names_and_type;
    source_header_names_and_type.readTextWithNamesInStorage(ctx.in);
    for (const auto & name_and_type : source_header_names_and_type)
    {
        ColumnWithTypeAndName elem(name_and_type.type, name_and_type.name);
        result.source_header.insert(elem);
    }

    NamesAndTypesList format_header_names_and_type;
    format_header_names_and_type.readTextWithNamesInStorage(ctx.in);
    for (const auto & name_and_type : format_header_names_and_type)
    {
        ColumnWithTypeAndName elem(name_and_type.type, name_and_type.name);
        result.format_header.insert(elem);
    }

    NamesAndTypesList format_filter_input_header_names_and_type;
    format_filter_input_header_names_and_type.readTextWithNamesInStorage(ctx.in);
    for (const auto & name_and_type : format_filter_input_header_names_and_type)
    {
        ColumnWithTypeAndName elem(name_and_type.type, name_and_type.name);
        result.format_filter_input_header.insert(elem);
    }

    std::string columns_desc;
    readStringBinary(columns_desc, ctx.in);
    result.columns_description = ColumnsDescription::parse(columns_desc);
    result.requested_columns.readTextWithNamesInStorage(ctx.in);
    result.requested_virtual_columns.readTextWithNamesInStorage(ctx.in);
    std::string json;
    readString(json, ctx.in);
    result.serialization_hints = SerializationInfoByName::readJSONFromString(result.columns_description.getAll(), json);

    ctx.in >> "\n";

    result.hive_partition_columns_to_read_from_file_path.readTextWithNamesInStorage(ctx.in);
    bool has_prewhere_info = false;
    readBinary(has_prewhere_info, ctx.in);
    if (has_prewhere_info)
        result.prewhere_info = std::make_shared<PrewhereInfo>(PrewhereInfo::deserialize(ctx));

    ctx.in >> "\n";

    return result;
}

Block buildAllowedFilterInputs(
    const StorageSnapshotPtr & storage_snapshot,
    const Block & source_header,
    const PrewhereInfoPtr & prewhere_info,
    const FilterDAGInfoPtr & row_level_filter)
{
    Block base = storage_snapshot->metadata->getSampleBlock();
    for (const auto & col : source_header)
        if (!base.has(col.name))
            base.insert(col);
    return FormatFilterInfo::buildKeyConditionInputs(std::move(base), prewhere_info, row_level_filter);
}

void prepareEagerKeyConditionSets(
    const std::shared_ptr<const ActionsDAG> & filter_actions_dag,
    const StorageSnapshotPtr & storage_snapshot,
    const Block & source_header,
    const PrewhereInfoPtr & prewhere_info,
    const FilterDAGInfoPtr & row_level_filter,
    const ContextPtr & context)
{
    if (!filter_actions_dag)
        return;

    auto allowed_inputs = buildAllowedFilterInputs(
        storage_snapshot, source_header, prewhere_info, row_level_filter);
    if (auto split = VirtualColumnUtils::splitFilterDagForAllowedInputs(
            filter_actions_dag->getOutputs().at(0), &allowed_inputs, context,
            /*allow_partial_result=*/ true))
        VirtualColumnUtils::buildSetsForDAGExcludingGlobalIn(*split, context);
}

size_t clampClusterFunctionNumStreams(UInt64 num_streams)
{
    /// 256 * cores is the ceiling max_threads gets in Context::setSetting; reuse it so a *Cluster
    /// read step never reserves/resizes a pipe vector for a pathological user-supplied value.
    return std::min<UInt64>(num_streams, 256 * getNumberOfCPUCoresToUse());
}

}
