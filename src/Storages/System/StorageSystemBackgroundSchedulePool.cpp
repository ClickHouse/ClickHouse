#include <Storages/System/StorageSystemBackgroundSchedulePool.h>
#include <Storages/System/SystemTableSourceRegistry.h>
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
        {"pool", std::make_shared<DataTypeString>(), "Pool name. Possible values: `schedule` (general purpose schedule pool), `buffer_flush` (pool for flushing Buffer table data), `distributed` (pool for distributed table operations), `message_broker` (pool for message broker operations), `streaming` (pool for streaming queries background jobs), `iceberg` (pool for Iceberg table metadata refresh)."},
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"table_uuid", std::make_shared<DataTypeUUID>(), "Table UUID."},
        {"query_id", std::make_shared<DataTypeString>(), "Query ID (if executing now). Note: this is not a real query, but a randomly generated ID for matching logs in `system.text_log`."},
        {"elapsed_ms", std::make_shared<DataTypeUInt64>(), "Task execution time (if executing now)."},
        {"log_name", std::make_shared<DataTypeString>(), "Log name for the task."},
        {"deactivated", std::make_shared<DataTypeUInt8>(), "Whether the task is deactivated (always false, since deactivated tasks are removed from the pool)."},
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

    /// Report only pools that already exist. The schedule pools are created lazily, and reading
    /// this table must not create one as a side effect (a read-only SELECT should have no effect
    /// on server state).
    auto fill_if_exists = [&](BackgroundSchedulePool * pool, const String & pool_name)
    {
        if (pool)
            fill_from_pool(*pool, pool_name);
    };

    fill_if_exists(context->getSchedulePoolIfExists(), "schedule");
    fill_if_exists(context->getBufferFlushSchedulePoolIfExists(), "buffer_flush");
    fill_if_exists(context->getDistributedSchedulePoolIfExists(), "distributed");
    fill_if_exists(context->getMessageBrokerSchedulePoolIfExists(), "message_broker");
    fill_if_exists(context->getStreamingSchedulePoolIfExists(), "streaming");
    fill_if_exists(context->getIcebergSchedulePoolIfExists(), "iceberg");
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemBackgroundSchedulePool) }
