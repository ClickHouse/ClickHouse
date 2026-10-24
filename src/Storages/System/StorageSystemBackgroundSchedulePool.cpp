#include <Storages/System/StorageSystemBackgroundSchedulePool.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Core/BackgroundSchedulePool.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

ColumnsDescription StorageSystemBackgroundSchedulePool::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"pool", std::make_shared<DataTypeString>(), "Pool name."},
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"table_uuid", std::make_shared<DataTypeUUID>(), "Table UUID."},
        {"query_id", std::make_shared<DataTypeString>(), "Query ID (if executing now)."},
        {"elapsed_ms", std::make_shared<DataTypeUInt64>(), "Task execution time (if executing now)."},
        {"log_name", std::make_shared<DataTypeString>(), "Log name for the task."},
        {"deactivated", std::make_shared<DataTypeUInt8>(), "Whether the task is deactivated (always false, since deactivated tasks removed from the pool)."},
        {"scheduled", std::make_shared<DataTypeUInt8>(), "Whether the task is scheduled for execution."},
        {"delayed", std::make_shared<DataTypeUInt8>(), "Whether the task is scheduled with delay."},
        {"executing", std::make_shared<DataTypeUInt8>(), "Whether the task is currently executing."},
    };
}

void StorageSystemBackgroundSchedulePool::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto fill_from_pool = [&](BackgroundSchedulePool & pool, const String & pool_name)
    {
        auto tasks = pool.getTasks();
        for (const auto & task_info : tasks)
        {
            size_t i = 0;
            res_columns[i++]->insert(pool_name);
            res_columns[i++]->insert(task_info.storage.database_name);
            res_columns[i++]->insert(task_info.storage.table_name);
            res_columns[i++]->insert(task_info.storage.uuid);
            res_columns[i++]->insert(task_info.query_id);
            res_columns[i++]->insert(task_info.elapsed_ms);
            res_columns[i++]->insert(task_info.log_name);
            res_columns[i++]->insert(task_info.deactivated);
            res_columns[i++]->insert(task_info.scheduled);
            res_columns[i++]->insert(task_info.delayed);
            res_columns[i++]->insert(task_info.executing);
        }
    };

    fill_from_pool(context->getSchedulePool(), "schedule");
    fill_from_pool(context->getBufferFlushSchedulePool(), "buffer_flush");
    fill_from_pool(context->getDistributedSchedulePool(), "distributed");
    fill_from_pool(context->getMessageBrokerSchedulePool(), "message_broker");
}

}
