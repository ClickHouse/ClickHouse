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
}

void ColumnMapper::setStorageColumnEncoding(std::unordered_map<String, Int64> && storage_encoding_)
{
    storage_encoding = std::move(storage_encoding_);
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

    FormatFilterInfo::FormatFilterInfo(std::shared_ptr<const ActionsDAG> filter_actions_dag_, const ContextPtr & context_, ColumnMapperPtr column_mapper_)
        : filter_actions_dag(filter_actions_dag_)
        , context(context_)
        , column_mapper(column_mapper_)
    {
    }

    FormatFilterInfo::FormatFilterInfo()
        : filter_actions_dag(nullptr)
        , context(static_cast<const ContextPtr &>(nullptr))
        , column_mapper(nullptr)
    {
    }


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

    ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag->getOutputs().front(), ctx);
    key_condition = std::make_shared<const KeyCondition>(
        inverted_dag, ctx, keys.getNames(), std::make_shared<ExpressionActions>(ActionsDAG(keys.getColumnsWithTypeAndName())));
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
