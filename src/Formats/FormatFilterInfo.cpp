#include <Formats/FormatFilterInfo.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/ExpressionActions.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/IColumn.h>
#include <Core/TypeId.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
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
    const std::unordered_map<Int64, String> & format_encoding)
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
}

FormatFilterInfo::FormatFilterInfo() = default;


bool FormatFilterInfo::hasFilter() const
{
    return filter_actions_dag != nullptr;
}


void FormatFilterInfo::initKeyCondition(const Block & keys)
{
    if (!filter_actions_dag)
        return;

    auto ctx = context.lock();
    if (!ctx)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Context has expired");

    if (prewhere_info || row_level_filter)
    {
        auto add_columns = [&](const ActionsDAG & dag)
        {
            for (const auto & col : dag.getRequiredColumns())
            {
                if (!keys.has(col.name) && !additional_columns.has(col.name))
                    additional_columns.insert({col.type->createColumn(), col.type, col.name});
            }
        };

        if (row_level_filter)
            add_columns(row_level_filter->actions);
        if (prewhere_info)
            add_columns(prewhere_info->prewhere_actions);
    }

    ColumnsWithTypeAndName columns = keys.getColumnsWithTypeAndName();
    for (const auto & col : additional_columns)
        columns.push_back(col);
    Names names;
    names.reserve(columns.size());
    for (const auto & col : columns)
        names.push_back(col.name);

    ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag->getOutputs().front(), ctx);
    key_condition = std::make_shared<const KeyCondition>(
        inverted_dag, ctx, names,
        std::make_shared<ExpressionActions>(ActionsDAG(columns)));
}

void FormatFilterInfo::initOnce(std::function<void()> f)
{
    std::call_once(
        init_flag,
        [&]
        {
            if (init_exception)
                std::rethrow_exception(init_exception);

            try
            {
                f();
            }
            catch (...)
            {
                init_exception = std::current_exception();
                throw;
            }
        });
}

}
