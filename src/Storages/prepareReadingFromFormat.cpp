#include <Storages/prepareReadingFromFormat.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
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
    chassert(prewhere_info || row_level_filter);

    if (info.prewhere_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "updateFormatPrewhereInfo called more than once");

    ReadFromFormatInfo new_info;
    new_info.prewhere_info = prewhere_info;
    new_info.row_level_filter = row_level_filter;

    /// Removes columns that are only used as prewhere input.
    /// Adds prewhere outputs (the actual prewhere filter column is only added if
    /// !remove_prewhere_column; but there may also be subexpressions computed by prewhere
    /// expression and preserved for use further down the query pipeline).
    /// If row_level_filter was already applied in a previous call, don't re-apply it;
    /// only apply the new prewhere_info on top.
    new_info.format_header = SourceStepWithFilter::applyPrewhereActions(
        info.format_header, info.row_level_filter ? nullptr : row_level_filter, prewhere_info);

    /// We assume that any format that supports prewhere also supports subset of subcolumns, so we
    /// don't need to replace subcolumns with their nested columns etc.
    new_info.source_header = new_info.format_header;

    /// Hive partition columns come from the file path, not the data file, so prewhere column
    /// pruning above does not concern them. Carry them over and keep their position before the
    /// virtual columns (as in prepareReadingFromFormat), otherwise the source skips appending them
    /// to the chunk and selecting a hive column while filtering a real one fails.
    new_info.hive_partition_columns_to_read_from_file_path = info.hive_partition_columns_to_read_from_file_path;
    for (const auto & column_from_file_path : new_info.hive_partition_columns_to_read_from_file_path)
        new_info.source_header.insert({column_from_file_path.type->createColumn(), column_from_file_path.type, column_from_file_path.name});

    new_info.requested_virtual_columns = info.requested_virtual_columns;
    for (const auto & requested_virtual_column : new_info.requested_virtual_columns)
        new_info.source_header.insert({requested_virtual_column.type->createColumn(), requested_virtual_column.type, requested_virtual_column.name});

    for (const auto & col : new_info.format_header)
    {
        new_info.requested_columns.emplace_back(col.name, col.type);
        if (info.format_header.has(col.name))
        {
            /// Column read from file.
            new_info.columns_description.add(info.columns_description.get(col.name));
        }
        else
        {
            /// Column produced by prewhere expression.
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

std::optional<ReadFromFormatInfo> splitLazilyReadColumnsFromFormatInfo(ReadFromFormatInfo & info, const NameSet & required_names)
{
    /// Columns that the PREWHERE / row-level filter needs as inputs must stay in the main read
    /// because filtering happens there.
    NameSet columns_to_keep = required_names;
    if (info.row_level_filter)
        for (const auto & column : info.row_level_filter->actions.getRequiredColumns())
            columns_to_keep.insert(column.name);
    if (info.prewhere_info)
        for (const auto & column : info.prewhere_info->prewhere_actions.getRequiredColumns())
            columns_to_keep.insert(column.name);

    /// Hive partition columns are parsed from the file path, reading them is cheap; keep them.
    for (const auto & column : info.hive_partition_columns_to_read_from_file_path)
        columns_to_keep.insert(column.name);

    /// Virtual columns are cheap as well.
    for (const auto & column : info.requested_virtual_columns)
        columns_to_keep.insert(column.name);

    NameSet requested_from_format;
    for (const auto & column : info.requested_columns)
        requested_from_format.insert(column.name);

    NameSet source_header_names;
    for (const auto & column : info.source_header)
        source_header_names.insert(column.name);

    /// `AddingDefaultsTransform` runs independently inside every branch (see
    /// `StorageObjectStorageSource::createReader` and `StorageFileSource::generate`): for a column
    /// that a file does not contain it evaluates the column's `DEFAULT` expression over the columns
    /// of that branch alone. An input of the expression that the branch does not read is not an
    /// error - it is substituted with the type's default value (see `defaultRequiredExpressions`),
    /// so splitting a defaulted column away from the inputs of its expression would silently
    /// compute it from zeros instead of the row's real values, and with the defaulted column in the
    /// sort key the `LIMIT` would then pick the wrong rows. A defaulted column and the transitive
    /// inputs of its expression must therefore always land on the same branch.
    ///
    /// That does not have to be the main branch: a defaulted column that nothing needs before the
    /// `LIMIT` moves to the lazy branch whenever every input of its expression is deferred with it,
    /// and its expression is then evaluated there over just the surviving rows. Only a defaulted
    /// column that stays on the main branch - one something needs before the `LIMIT`, or one whose
    /// expression consumes an input the lazy branch would not see (an input pinned to the main
    /// branch, or one the format does not read at all) - pins the inputs of its expression to the
    /// main branch. The last rule feeds itself (pinning an input can strand another default's
    /// expression), so iterate to a fixpoint.
    ///
    /// A hive partition column is such a format-unread input: it is parsed from the file path and
    /// appended to the chunk only after the per-file reader pipeline, where `AddingDefaultsTransform`
    /// runs, so no branch ever sees its real value and a default over one cannot be evaluated at
    /// all - the single-pass plan fails with `UNKNOWN_IDENTIFIER` just the same (a pre-existing
    /// limitation of hive partitioning). Keeping such a defaulted column on the main branch
    /// preserves the single-pass behavior exactly.
    ///
    /// Only the defaults that this query can actually evaluate matter: `AddingDefaultsTransform`
    /// applies a default expression solely for a column present in the block of its own branch
    /// (`res.has(col_name)`), so a defaulted column that the query does not read at all imposes no
    /// dependency and must not pin its inputs to the main branch - otherwise a schema with unused
    /// `DEFAULT` / `ALIAS` columns would lose the I/O savings for no reason.
    const auto & column_defaults = info.columns_description.getDefaults();
    NameSet names_in_default_expressions;
    std::vector<String> names_to_visit;
    auto seed_defaulted_column = [&](const String & name)
    {
        if (column_defaults.contains(name) && names_in_default_expressions.insert(name).second)
            names_to_visit.push_back(name);
    };
    auto default_expression_inputs = [&](const String & name)
    {
        RequiredSourceColumnsVisitor::Data columns_context;
        auto expression = column_defaults.at(name).expression->clone();
        RequiredSourceColumnsVisitor(columns_context).visit(expression);
        return columns_context.requiredColumns();
    };
    for (const auto & column : info.source_header)
        if (columns_to_keep.contains(column.name))
            seed_defaulted_column(column.name);
    /// A defaulted column consumed only by the PREWHERE / row-level filter is stripped from
    /// `info.source_header` by `updateFormatPrewhereInfo`, but the main branch still reads it and
    /// `AddingDefaultsTransform` evaluates its expression there before the filter runs - so it
    /// pins the inputs of its expression to the main branch just like a visible column.
    if (info.row_level_filter)
        for (const auto & column : info.row_level_filter->actions.getRequiredColumns())
            seed_defaulted_column(column.name);
    if (info.prewhere_info)
        for (const auto & column : info.prewhere_info->prewhere_actions.getRequiredColumns())
            seed_defaulted_column(column.name);
    bool pinned_more = true;
    while (pinned_more)
    {
        while (!names_to_visit.empty())
        {
            const String name = names_to_visit.back();
            names_to_visit.pop_back();

            if (!column_defaults.contains(name))
                continue;

            for (const auto & required_name : default_expression_inputs(name))
                if (names_in_default_expressions.insert(required_name).second)
                    names_to_visit.push_back(required_name);
        }
        columns_to_keep.insert(names_in_default_expressions.begin(), names_in_default_expressions.end());

        /// Pin every still-deferred defaulted column whose expression has an input the lazy
        /// branch would not read the real value of.
        pinned_more = false;
        for (const auto & column : info.source_header)
        {
            if (columns_to_keep.contains(column.name) || !column_defaults.contains(column.name))
                continue;

            for (const auto & input : default_expression_inputs(column.name))
            {
                const bool input_is_deferred_too = source_header_names.contains(input)
                    && requested_from_format.contains(input)
                    && !columns_to_keep.contains(input);
                if (!input_is_deferred_too)
                {
                    seed_defaulted_column(column.name);
                    pinned_more = true;
                    break;
                }
            }
        }
    }

    /// Defer the physical columns that the format reads and nothing needs before the LIMIT.
    NameSet lazy_names;
    Block lazy_source_header;
    for (const auto & column : info.source_header)
    {
        if (!columns_to_keep.contains(column.name) && requested_from_format.contains(column.name))
        {
            lazy_names.insert(column.name);
            lazy_source_header.insert(column);
        }
    }

    if (!lazy_source_header.columns())
        return {};

    /// The info for the lazy read: only the deferred columns, no virtual columns, no filters.
    ReadFromFormatInfo lazy_info;
    lazy_info.source_header = lazy_source_header;
    lazy_info.columns_description = info.columns_description;
    lazy_info.serialization_hints = info.serialization_hints;
    for (const auto & column : info.requested_columns)
        if (lazy_names.contains(column.name))
            lazy_info.requested_columns.push_back(column);

    /// The format reads a requested subcolumn (e.g. `json.some.path`) as its whole parent column;
    /// the subcolumn is extracted afterwards by `ExtractColumnsTransform`. `format_header` therefore
    /// contains the parent's name, not the subcolumn's, so split it by the storage-level names each
    /// branch needs: the parent of a deferred subcolumn goes to the lazy branch, and it stays in the
    /// main branch as well when something there still needs it (a requested column of its own name,
    /// another subcolumn of the same parent, or any column pinned by `columns_to_keep`).
    NameSet lazy_format_names;
    for (const auto & column : lazy_info.requested_columns)
        lazy_format_names.insert(column.getNameInStorage());

    NameSet main_format_names;
    for (const auto & column : info.requested_columns)
        if (!lazy_names.contains(column.name))
            main_format_names.insert(column.getNameInStorage());

    for (const auto & column : info.format_header)
        if (lazy_names.contains(column.name) || lazy_format_names.contains(column.name))
            lazy_info.format_header.insert(column);

    /// Remove the deferred columns from the main read and make it produce the global row index.
    Block main_source_header;
    for (const auto & column : info.source_header)
        if (!lazy_names.contains(column.name))
            main_source_header.insert(column);
    main_source_header.insert({std::make_shared<DataTypeUInt64>(), "__global_row_index"});

    Block main_format_header;
    for (const auto & column : info.format_header)
    {
        const bool needed_by_lazy = lazy_names.contains(column.name) || lazy_format_names.contains(column.name);
        const bool needed_by_main = main_format_names.contains(column.name) || columns_to_keep.contains(column.name);
        if (!needed_by_lazy || needed_by_main)
            main_format_header.insert(column);
    }

    NamesAndTypesList main_requested_columns;
    for (const auto & column : info.requested_columns)
        if (!lazy_names.contains(column.name))
            main_requested_columns.push_back(column);

    info.source_header = std::move(main_source_header);
    info.format_header = std::move(main_format_header);
    info.requested_columns = std::move(main_requested_columns);

    return lazy_info;
}

}
