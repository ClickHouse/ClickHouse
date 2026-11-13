#include <Formats/FormatFilterInfo.h>
#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Common/FieldVisitorHash.h>
#include <Common/SipHash.h>
#include <Formats/FormatSettings.h>
#include <boost/functional/hash.hpp>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/convertFieldToType.h>
#include <Common/typeid_cast.h>
#include <Columns/IColumn.h>
#include <Core/TypeId.h>

#include <Interpreters/Context.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int TYPE_MISMATCH;
}

namespace Setting
{
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsBool input_format_parquet_filter_push_down;
    extern const SettingsBool input_format_parquet_bloom_filter_push_down;
    extern const SettingsUInt64 input_format_parquet_dictionary_filter_push_down;
    extern const SettingsBool input_format_parquet_page_filter_push_down;
    extern const SettingsBool input_format_parquet_spatial_filter_push_down;
    extern const SettingsBool input_format_orc_filter_push_down;
    extern const SettingsBool input_format_parquet_case_insensitive_column_matching;
    extern const SettingsBool input_format_orc_case_insensitive_column_matching;
    extern const SettingsBool input_format_arrow_case_insensitive_column_matching;
    extern const SettingsBool input_format_parquet_allow_missing_columns;
    extern const SettingsBool input_format_orc_allow_missing_columns;
    extern const SettingsBool input_format_arrow_allow_missing_columns;
    extern const SettingsBool input_format_parquet_use_offset_index;
    extern const SettingsBool input_format_parquet_local_time_as_utc;
    extern const SettingsBool input_format_null_as_default;
    extern const SettingsDateTimeOverflowBehavior date_time_overflow_behavior;
    extern const SettingsBool input_format_parquet_allow_geoparquet_parser;
    extern const SettingsBool input_format_parquet_enable_json_parsing;
    extern const SettingsBool input_format_parquet_detect_variant_by_structure;
    extern const SettingsBool type_json_use_partial_match_to_skip_paths_by_regexp;
    extern const SettingsBool precise_float_parsing;
}

/// The entries for files are written from the row groups the reader pruned, not from evaluating the
/// condition. With a condition that is not a filter of rows (e.g. `indexHint`), a pruned row group
/// still has rows the query returns when it is not pruned, so a read with another set of enabled
/// pruning methods must not consult the entries of this one. The column matching settings decide
/// which physical column a predicate is evaluated against, so they are part of the key as well. So are
/// the settings allowing missing columns: a read that fills a missing column with defaults may cache
/// a verdict, and a read that must throw for that column would otherwise skip the file instead.
/// `input_format_parquet_use_offset_index` enables page pruning, and `input_format_parquet_local_time_as_utc`
/// decides the time zone in which the timestamp statistics are decoded. `input_format_null_as_default`
/// decides whether a `NULL` read into a non-`Nullable` column becomes the default, or throws: a read that
/// fills defaults may cache a verdict, and a read that must throw would otherwise skip the file instead.
/// `date_time_overflow_behavior` decides whether an out-of-range date is saturated or throws, for the same reason.
/// The rest change the values decoded from a file without changing their types, as in the identity of the
/// columns cache (see `ParquetV3BlockInputFormat`): e.g. `type_json_use_partial_match_to_skip_paths_by_regexp`
/// decides which paths of a `JSON` column decoded from a variant are skipped.
/// `queryConditionCacheSettingsSalt` covers it for the session, but a table may pin another value at `CREATE`.
UInt64 fileFormatPruningSettingsSalt(const Settings & settings)
{
    SipHash hash;
    hash.update(settings[Setting::input_format_parquet_filter_push_down].value);
    hash.update(settings[Setting::input_format_parquet_bloom_filter_push_down].value);
    hash.update(settings[Setting::input_format_parquet_dictionary_filter_push_down].value);
    hash.update(settings[Setting::input_format_parquet_page_filter_push_down].value);
    hash.update(settings[Setting::input_format_parquet_spatial_filter_push_down].value);
    hash.update(settings[Setting::input_format_orc_filter_push_down].value);
    hash.update(settings[Setting::input_format_parquet_case_insensitive_column_matching].value);
    hash.update(settings[Setting::input_format_orc_case_insensitive_column_matching].value);
    hash.update(settings[Setting::input_format_arrow_case_insensitive_column_matching].value);
    hash.update(settings[Setting::input_format_parquet_allow_missing_columns].value);
    hash.update(settings[Setting::input_format_orc_allow_missing_columns].value);
    hash.update(settings[Setting::input_format_arrow_allow_missing_columns].value);
    hash.update(settings[Setting::input_format_parquet_use_offset_index].value);
    hash.update(settings[Setting::input_format_parquet_local_time_as_utc].value);
    hash.update(settings[Setting::input_format_null_as_default].value);
    hash.update(static_cast<UInt64>(settings[Setting::date_time_overflow_behavior].value));
    hash.update(settings[Setting::input_format_parquet_allow_geoparquet_parser].value);
    hash.update(settings[Setting::input_format_parquet_enable_json_parsing].value);
    hash.update(settings[Setting::input_format_parquet_detect_variant_by_structure].value);
    hash.update(settings[Setting::type_json_use_partial_match_to_skip_paths_by_regexp].value);
    hash.update(settings[Setting::precise_float_parsing].value);
    return hash.get64();
}

/// The same settings as above, taken from the format settings a reader actually uses. Keep them in sync.
UInt64 fileFormatPruningSettingsSalt(const FormatSettings & format_settings)
{
    SipHash hash;
    hash.update(format_settings.parquet.filter_push_down);
    hash.update(format_settings.parquet.bloom_filter_push_down);
    hash.update(static_cast<UInt64>(format_settings.parquet.dictionary_filter_push_down));
    hash.update(format_settings.parquet.page_filter_push_down);
    hash.update(format_settings.parquet.spatial_filter_push_down);
    hash.update(format_settings.orc.filter_push_down);
    hash.update(format_settings.parquet.case_insensitive_column_matching);
    hash.update(format_settings.orc.case_insensitive_column_matching);
    hash.update(format_settings.arrow.case_insensitive_column_matching);
    hash.update(format_settings.parquet.allow_missing_columns);
    hash.update(format_settings.orc.allow_missing_columns);
    hash.update(format_settings.arrow.allow_missing_columns);
    hash.update(format_settings.parquet.use_offset_index);
    hash.update(format_settings.parquet.local_time_as_utc);
    hash.update(format_settings.null_as_default);
    hash.update(static_cast<UInt64>(format_settings.date_time_overflow_behavior));
    hash.update(format_settings.parquet.allow_geoparquet_parser);
    hash.update(format_settings.parquet.enable_json_parsing);
    hash.update(format_settings.parquet.detect_variant_by_structure);
    hash.update(format_settings.json.type_json_use_partial_match_to_skip_paths_by_regexp);
    hash.update(format_settings.precise_float_parsing);
    return hash.get64();
}

void ColumnMapper::setStorageColumnEncoding(ColumnNameToFieldId && storage_encoding_)
{
    chassert(storage_encoding.empty());
    storage_encoding = std::move(storage_encoding_);
    for (const auto & [column_name, field_id] : storage_encoding)
        if (!field_id_to_clickhouse_name.emplace(field_id, column_name).second)
            throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Duplicate field id {}", field_id);
}

String ColumnMapper::getIcebergParentPath(const String & path) const
{
    if (const String * struct_path = getIcebergStructPath(path))
        return *struct_path;
    /// A list element, map key or map value is named `element`, `key` or `value`.
    auto pos = path.rfind('.');
    return pos == String::npos ? String{} : path.substr(0, pos);
}

String ColumnMapper::getIcebergFieldPath(const String & path) const
{
    for (size_t end = path.size(); end != String::npos && end > 0; end = path.rfind('.', end - 1))
    {
        String prefix = path.substr(0, end);
        if (storage_encoding.contains(prefix))
            return prefix;
    }
    return {};
}

UInt64 ColumnMapper::getIcebergInitialDefaultsHash() const
{
    if (iceberg_initial_defaults.empty())
        return 0;
    std::vector<std::pair<std::string_view, const Field *>> sorted;
    sorted.reserve(iceberg_initial_defaults.size());
    for (const auto & [path, initial_default] : iceberg_initial_defaults)
        sorted.emplace_back(path, &initial_default.value);
    std::sort(sorted.begin(), sorted.end());
    SipHash hash;
    hash.update(sorted.size());
    for (const auto & [path, field] : sorted)
    {
        hash.update(path.size());
        hash.update(path);
        applyVisitor(FieldVisitorHash(hash), *field);
    }
    return hash.get64();
}

/// `value` of the type `from` as a value of the type `to`, whose structs can have only some of the elements of the structs of
/// `from`, at any depth. A tuple cannot be converted to a tuple with fewer elements, so the elements are taken by name.
static Field projectIcebergInitialDefault(const Field & value, const DataTypePtr & from, const DataTypePtr & to)
{
    if (value.isNull())
        return convertFieldToTypeOrThrow(value, *to);

    const auto from_nested = removeNullable(from);
    const auto to_nested = removeNullable(to);
    const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from_nested.get());
    const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to_nested.get());
    if (from_tuple && to_tuple && from_tuple->hasExplicitNames() && to_tuple->hasExplicitNames()
        && value.getType() == Field::Types::Tuple)
    {
        const auto & elements = value.safeGet<Tuple>();
        Tuple result;
        result.reserve(to_tuple->getElements().size());
        for (size_t i = 0; i < to_tuple->getElements().size(); ++i)
        {
            const auto & name = to_tuple->getNameByPosition(i + 1);
            auto pos = from_tuple->tryGetPositionByName(name);
            if (!pos || *pos >= elements.size())
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH, "Cannot read the Iceberg initial-default of type {} as {}: it has no element {}",
                    from->getName(), to->getName(), name);
            result.push_back(projectIcebergInitialDefault(elements[*pos], from_tuple->getElement(*pos), to_tuple->getElement(i)));
        }
        return result;
    }

    const auto * from_array = typeid_cast<const DataTypeArray *>(from_nested.get());
    const auto * to_array = typeid_cast<const DataTypeArray *>(to_nested.get());
    if (from_array && to_array && value.getType() == Field::Types::Array)
    {
        const auto & elements = value.safeGet<Array>();
        Array result;
        result.reserve(elements.size());
        for (const auto & element : elements)
            result.push_back(projectIcebergInitialDefault(element, from_array->getNestedType(), to_array->getNestedType()));
        return result;
    }

    const auto * from_map = typeid_cast<const DataTypeMap *>(from_nested.get());
    const auto * to_map = typeid_cast<const DataTypeMap *>(to_nested.get());
    if (from_map && to_map && value.getType() == Field::Types::Map)
    {
        const auto & entries = value.safeGet<Map>();
        Map result;
        result.reserve(entries.size());
        for (const auto & entry : entries)
        {
            const auto & key_value = entry.safeGet<Tuple>();
            result.push_back(Tuple{
                projectIcebergInitialDefault(key_value.at(0), from_map->getKeyType(), to_map->getKeyType()),
                projectIcebergInitialDefault(key_value.at(1), from_map->getValueType(), to_map->getValueType())});
        }
        return result;
    }

    return convertFieldToTypeOrThrow(value, *to);
}

std::optional<Field> ColumnMapper::getIcebergInitialDefault(const String & path, const DataTypePtr & type) const
{
    if (auto it = iceberg_initial_defaults.find(path); it != iceberg_initial_defaults.end())
        return projectIcebergInitialDefault(it->second.value, it->second.type, type);

    const String field = getIcebergFieldPath(path);
    auto it = field.empty() || field == path ? iceberg_initial_defaults.end() : iceberg_initial_defaults.find(field);
    if (it == iceberg_initial_defaults.end())
        return {};
    const auto & [value, field_type] = it->second;
    const String subcolumn_name = path.substr(field.size() + 1);
    auto subcolumn_type = field_type->tryGetSubcolumnType(subcolumn_name);
    if (!subcolumn_type)
        return {};
    auto column = field_type->createColumn();
    column->insert(value);
    auto subcolumn = field_type->getSubcolumn(subcolumn_name, std::move(column));
    return projectIcebergInitialDefault((*subcolumn)[0], subcolumn_type, type);
}

std::pair<ColumnNameMap, ColumnNameMap> ColumnMapper::makeMapping(
    const FieldIdToColumnName & format_encoding) const
{
    ColumnNameMap clickhouse_to_parquet_names;
    ColumnNameMap parquet_names_to_clickhouse;
    for (const auto & [column_name, field_id] : storage_encoding)
    {
        if (auto it = format_encoding.find(field_id); it != format_encoding.end())
        {
            clickhouse_to_parquet_names[column_name] = it->second;
            parquet_names_to_clickhouse[it->second] = column_name;
        }
        else
        {
            clickhouse_to_parquet_names[column_name] = column_name;
            parquet_names_to_clickhouse[column_name] = column_name;
        }
    }
    return {clickhouse_to_parquet_names, parquet_names_to_clickhouse};
}

FormatFilterInfo::FormatFilterInfo(
    std::shared_ptr<const ActionsDAG> filter_actions_dag_,
    const ContextPtr & context_,
    ColumnMapperPtr column_mapper_,
    FilterDAGInfoPtr row_level_filter_,
    PrewhereInfoPtr prewhere_info_)
    : filter_actions_dag(filter_actions_dag_)
    , context(context_)
    , row_level_filter(std::move(row_level_filter_))
    , prewhere_info(std::move(prewhere_info_))
    , column_mapper(column_mapper_)
{
    if (filter_actions_dag && context_->getSettingsRef()[Setting::use_query_condition_cache])
        condition_hash = computeConditionHash(*filter_actions_dag, prewhere_info, context_);
}

std::optional<size_t> FormatFilterInfo::computeConditionHash(
    const ActionsDAG & filter_actions_dag,
    const PrewhereInfoPtr & prewhere_info,
    const ContextPtr & context)
{
    /// PREWHERE runs before the row count this hash describes, so a row group PREWHERE empties looks
    /// like one the hashed condition rejected. A PREWHERE collected with the read's filters is inside
    /// `filter_actions_dag`, so the hash covers it; a join runtime filter is moved in afterwards.
    if (prewhere_info)
    {
        const auto * prewhere_node = prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name);
        if (!prewhere_node || !VirtualColumnUtils::isDeterministic(prewhere_node))
            return {};
    }

    const auto & outputs = filter_actions_dag.getOutputs();
    if (outputs.size() != 1 || !VirtualColumnUtils::isDeterministic(outputs[0]))
        return {};

    const auto & settings = context->getSettingsRef();
    return queryConditionCacheHash(
        filter_actions_dag.getHash(),
        queryConditionCacheSettingsSalt(settings) ^ fileFormatPruningSettingsSalt(settings));
}

FormatFilterInfo::FormatFilterInfo() = default;


bool FormatFilterInfo::hasFilter() const
{
    return filter_actions_dag || row_level_filter || prewhere_info || top_k_filter;
}

namespace
{
    /// True if `base` already has a column that covers `name` - either `name` itself, or an
    /// ancestor of it (e.g. `t` covers subcolumn `t.a`). Requesting both the ancestor and the
    /// subcolumn from a format reader is redundant and some readers (e.g. Parquet's
    /// SchemaConverter) reject it as COLUMN_QUERIED_MORE_THAN_ONCE.
    bool isColumnCovered(const Block & base, const String & name)
    {
        if (base.has(name))
            return true;
        for (size_t pos = name.find('.'); pos != String::npos; pos = name.find('.', pos + 1))
            if (base.has(name.substr(0, pos)))
                return true;
        return false;
    }
}

Block FormatFilterInfo::buildKeyConditionInputs(
    Block base,
    const PrewhereInfoPtr & prewhere_info,
    const FilterDAGInfoPtr & row_level_filter)
{
    auto add_required = [&](const ActionsDAG & dag)
    {
        for (const auto & col : dag.getRequiredColumns())
            if (!isColumnCovered(base, col.name))
                base.insert({col.type->createColumn(), col.type, col.name});
    };
    if (row_level_filter)
        add_required(row_level_filter->actions);
    if (prewhere_info)
        add_required(prewhere_info->prewhere_actions);
    return base;
}

void FormatFilterInfo::initKeyConditionOnce(const Block & keys)
{
    std::call_once(
        init_flag,
        [&]
        {
            if (init_exception)
                std::rethrow_exception(copyMutableException(init_exception));

            try
            {
                if (!filter_actions_dag)
                    return;

                auto ctx = context.lock();
                if (!ctx)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Context has expired");

                Block all_inputs = buildKeyConditionInputs(keys, prewhere_info, row_level_filter);
                /// `row_level_filter`/`prewhere_info` are usually derived from `filter_actions_dag`
                /// (the WHERE clause) and so normally reference a superset of its columns, but that's
                /// not guaranteed - e.g. in the data lake schema-changed path they may be null while
                /// `filter_actions_dag` alone still drives spatial/row-group pruning. Make sure its
                /// required columns (e.g. the geometry column) end up in `additional_columns` too, or
                /// pruning code that looks the column up in the sample block silently no-ops.
                for (const auto & col : filter_actions_dag->getRequiredColumns())
                    if (!isColumnCovered(all_inputs, col.name))
                        all_inputs.insert({col.type->createColumn(), col.type, col.name});
                for (const auto & col : all_inputs)
                    if (!keys.has(col.name))
                        additional_columns.insert(col);

                ColumnsWithTypeAndName columns = all_inputs.getColumnsWithTypeAndName();
                Names names;
                names.reserve(columns.size());
                for (const auto & col : columns)
                    names.push_back(col.name);

                ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag->getOutputs().front(), ctx, /* boolean_context */ true);
                key_condition = std::make_shared<const KeyCondition>(
                    inverted_dag, ctx, names,
                    std::make_shared<ExpressionActions>(ActionsDAG(columns)));
            }
            catch (...)
            {
                /// Store the original as an immutable template that is only ever read
                /// (copied), and hand this caller a private copy too. Otherwise this
                /// thread would keep mutating the stored object (via `addMessage` up the
                /// stack) while a concurrent caller copies it here - a data race.
                init_exception = std::current_exception();
                std::rethrow_exception(copyMutableException(init_exception));
            }
        });
}

std::optional<std::shared_ptr<const KeyCondition>> FormatFilterInfo::tryGetJoinRuntimeFilterKeyCondition(const Block & keys)
{
    std::lock_guard lock(join_runtime_filter_mutex);
    if (join_runtime_filter_key_condition_built)
        return join_runtime_filter_key_condition;
    if (!join_runtime_filter_predicate)
        return nullptr;

    auto predicate = join_runtime_filter_predicate->tryGet();
    if (!predicate)
        return std::nullopt;

    join_runtime_filter_key_condition_built = true;
    auto ctx = context.lock();
    if (!*predicate || !ctx)
        return nullptr;

    ColumnsWithTypeAndName columns = keys.getColumnsWithTypeAndName();
    for (const auto & column : additional_columns)
        columns.push_back(column);

    /// The predicate reads each key as the type of the read step's output; KeyCondition would compare the statistics of
    /// a column of another type wrongly. Renamed, the column is unknown to the predicate, which then ignores that key.
    for (const auto & descriptor : join_runtime_filter_predicate->getDescriptors())
    {
        auto it = std::ranges::find(columns, descriptor.key_column_name, &ColumnWithTypeAndName::name);
        if (it != columns.end() && !it->type->equals(*descriptor.key_column_type))
            it->name = "__join_runtime_filter_key_of_another_type_" + it->name;
    }

    Names names;
    names.reserve(columns.size());
    for (const auto & column : columns)
        names.push_back(column.name);

    ActionsDAGWithInversionPushDown inverted_dag((*predicate)->getOutputs().front(), ctx, /* boolean_context */ true);
    join_runtime_filter_key_condition = std::make_shared<const KeyCondition>(
        inverted_dag, ctx, names, std::make_shared<ExpressionActions>(ActionsDAG(columns)));
    return join_runtime_filter_key_condition;
}

}
