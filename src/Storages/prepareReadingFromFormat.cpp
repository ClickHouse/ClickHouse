#include <Storages/prepareReadingFromFormat.h>
#include <Formats/FormatFactory.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

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
        if (auto virtual_column = storage_snapshot->virtual_columns->tryGet(column_name))
            info.requested_virtual_columns.emplace_back(std::move(*virtual_column));
        else if (auto it = hive_parameters.hive_partition_columns_to_read_from_file_path_map.find(column_name); it != hive_parameters.hive_partition_columns_to_read_from_file_path_map.end())
            info.hive_partition_columns_to_read_from_file_path.emplace_back(it->first, it->second);
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
        else if (columns_to_read.empty())
        {
            /// If only virtual columns were requested, just read the smallest column.
            columns_to_read.push_back(ExpressionActions::getSmallestColumn(columns_in_data_file).name);
        }
        else
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
    /// But we still need to do some processing on the set of requested columns:
    ///  * If a non-tuple-element subcolumn is requested, request the whole column.
    ///    E.g. if the type of `t` is Object, `t.x` is a dynamic subcolumn, and we should
    ///    request the whole `t` instead. Reading a subset of dynamic subcolumns is
    ///    currently not supported by any format parser (though we might want to add it in
    ///    future for parquet variant columns).
    ///  * Don't request tuple element if the whole tuple is also requested.
    ///    E.g. `SELECT t, t.x` should just read `t`.

    struct SubcolumnInfo
    {
        ISerialization::SubstreamPath path;
        String name;
        DataTypePtr type;
        bool is_duplicate = false;
    };

    std::vector<SubcolumnInfo> columns_info(requested_columns.size());
    std::unordered_map<String, size_t> name_to_idx;
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
        column_info.type = column_to_read.getTypeInStorage();

        if (column_to_read.isSubcolumn())
        {
            /// Do subcolumn lookup similar to getColumnFromBlock.

            auto type = column_to_read.getTypeInStorage();
            auto data = ISerialization::SubstreamData(type->getDefaultSerialization()).withType(type);
            auto subcolumn_name = column_to_read.getSubcolumnName();

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

            if (!column_info.path.empty())
                column_info.type = ISerialization::createFromPath(column_info.path, column_info.path.size()).type;
        }

        column_info.name = column_to_read.getNameInStorage();
        if (!column_info.path.empty())
        {
            column_info.name += '.';
            column_info.name += ISerialization::getSubcolumnNameForStream(column_info.path);
        }
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
                column_to_read.setDelimiterAndTypeInStorage(ancestor_name, ancestor_info.type);
                ancestor_requested = true;
                break;
            }
        }
        if (ancestor_requested)
            continue;

        column_to_read.setDelimiterAndTypeInStorage(column_info.name, column_info.type);
        if (!column_info.is_duplicate)
            new_columns_to_read.push_back(column_info.name);
    }
    return new_columns_to_read;

    /// (Not checking columns_to_read.empty() in this case, assuming that formats with
    ///  supports_tuple_elements also support empty list of columns.)
}

ReadFromFormatInfo updateFormatPrewhereInfo(const ReadFromFormatInfo & info, const FilterDAGInfoPtr & row_level_filter, const PrewhereInfoPtr & prewhere_info)
{
    chassert(prewhere_info);

    if (info.prewhere_info || info.row_level_filter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "updateFormatPrewhereInfo called more than once");

    ReadFromFormatInfo new_info;
    new_info.prewhere_info = prewhere_info;

    /// Removes columns that are only used as prewhere input.
    /// Adds prewhere result column if !remove_prewhere_column.
    new_info.format_header = SourceStepWithFilter::applyPrewhereActions(info.format_header, row_level_filter, prewhere_info);

    /// We assume that any format that supports prewhere also supports subset of subcolumns, so we
    /// don't need to replace subcolumns with their nested columns etc.
    new_info.source_header = new_info.format_header;

    new_info.requested_virtual_columns = info.requested_virtual_columns;
    for (const auto & requested_virtual_column : new_info.requested_virtual_columns)
        new_info.source_header.insert({requested_virtual_column.type->createColumn(), requested_virtual_column.type, requested_virtual_column.name});

    for (const auto & col : new_info.format_header)
    {
        new_info.requested_columns.emplace_back(col.name, col.type);
        if (info.format_header.has(col.name))
        {
            new_info.columns_description.add(info.columns_description.get(col.name));
        }
        else
        {
            chassert(col.name == prewhere_info->prewhere_column_name);
            chassert(!prewhere_info->remove_prewhere_column);
            new_info.columns_description.add(ColumnDescription(col.name, col.type));
        }
    }

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

    const auto & our_columns = metadata_snapshot->getColumns();
    const auto & storage_columns = storage_ptr->getInMemoryMetadataPtr()->getColumns();
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
    bool has_prewhere_info;
    readBinary(has_prewhere_info, ctx.in);
    if (has_prewhere_info)
        result.prewhere_info = std::make_shared<PrewhereInfo>(PrewhereInfo::deserialize(ctx));

    ctx.in >> "\n";

    return result;
}

}
