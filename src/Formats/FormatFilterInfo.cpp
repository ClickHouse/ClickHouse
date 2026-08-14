#include <Formats/FormatFilterInfo.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/ExpressionActions.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/IColumn.h>
#include <Core/TypeId.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace Setting
{
    extern const SettingsBool use_query_condition_cache;
}

void ColumnMapper::setStorageColumnEncoding(std::unordered_map<String, Int64> && storage_encoding_)
{
    chassert(storage_encoding.empty());
    storage_encoding = std::move(storage_encoding_);
    for (const auto & [column_name, field_id] : storage_encoding)
        if (!field_id_to_clickhouse_name.emplace(field_id, column_name).second)
            throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Duplicate field id {}", field_id);
}

std::pair<std::unordered_map<String, String>, std::unordered_map<String, String>> ColumnMapper::makeMapping(
    const std::unordered_map<Int64, String> & format_encoding) const
{
    std::unordered_map<String, String> clickhouse_to_parquet_names;
    std::unordered_map<String, String> parquet_names_to_clickhouse;
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
    bool use_query_condition_cache = context_->getSettingsRef()[Setting::use_query_condition_cache];
    if (use_query_condition_cache && filter_actions_dag)
    {
        const auto & outputs = filter_actions_dag->getOutputs();
        if (outputs.size() == 1 && VirtualColumnUtils::isDeterministic(outputs[0]))
            condition_hash = filter_actions_dag->getHash();
    }
}

FormatFilterInfo::FormatFilterInfo() = default;


bool FormatFilterInfo::hasFilter() const
{
    return filter_actions_dag != nullptr;
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
                std::rethrow_exception(init_exception);

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
                init_exception = std::current_exception();
                throw;
            }
        });
}

}
