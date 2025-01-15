#include <Storages/System/StorageSystemViewRefreshes.h>

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/MaterializedView/RefreshTask.h>


namespace DB
{

ColumnsDescription StorageSystemViewRefreshes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "The name of the database the table is in."},
        {"view", std::make_shared<DataTypeString>(), "Table name."},
        {"uuid", std::make_shared<DataTypeUUID>(), "Table uuid (Atomic database)."},
        {"status", std::make_shared<DataTypeString>(), "Current state of the refresh."},
        {"last_success_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()),
            "Time when the latest successful refresh started. NULL if no successful refreshes happened since server startup or table creation."},
        {"last_success_duration_ms", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "How long the latest refresh took."},
        {"last_refresh_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()),
            "Time when the latest refresh attempt finished (if known) or started (if unknown or still running). NULL if no refresh attempts happened since server startup or table creation."},
        {"last_refresh_replica", std::make_shared<DataTypeString>(), "If coordination is enabled, name of the replica that made the current (if running) or previous (if not running) refresh attempt."},
        {"next_refresh_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time at which the next refresh is scheduled to start, if status = Scheduled."},
        {"exception", std::make_shared<DataTypeString>(), "Error message from previous attempt if it failed."},
        {"retry", std::make_shared<DataTypeUInt64>(), "How many failed attempts there were so far, for the current refresh. Not available if status is `RunningOnAnotherReplica`."},
        {"progress", std::make_shared<DataTypeFloat64>(), "Progress of the current refresh, between 0 and 1. Not available if status is RunningOnAnotherReplica."},
        {"read_rows", std::make_shared<DataTypeUInt64>(), "Number of rows read by the current refresh so far. Not available if status is RunningOnAnotherReplica."},
        {"read_bytes", std::make_shared<DataTypeUInt64>(), "Number of bytes read during the current refresh. Not available if status is `RunningOnAnotherReplica`."},
        {"total_rows", std::make_shared<DataTypeUInt64>(), "Estimated total number of rows that need to be read by the current refresh. Not available if status is RunningOnAnotherReplica."},
        {"written_rows", std::make_shared<DataTypeUInt64>(), "Number of rows written during the current refresh. Not available if status is `RunningOnAnotherReplica`."},
        {"written_bytes", std::make_shared<DataTypeUInt64>(), "Number rof bytes written during the current refresh. Not available if status is `RunningOnAnotherReplica`."},
    };
}

void StorageSystemViewRefreshes::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto access = context->getAccess();
    auto valid_access = AccessType::SHOW_TABLES;
    bool check_access_for_tables = !access->isGranted(valid_access);

    for (const RefreshTaskPtr & task : context->getRefreshSet().getTasks())
    {
        RefreshTask::Info refresh = task->getInfo();

        if (check_access_for_tables && !access->isGranted(valid_access, refresh.view_id.getDatabaseName(), refresh.view_id.getTableName()))
            continue;

        std::size_t i = 0;
        res_columns[i++]->insert(refresh.view_id.getDatabaseName());
        res_columns[i++]->insert(refresh.view_id.getTableName());
        res_columns[i++]->insert(refresh.view_id.uuid);
        res_columns[i++]->insert(toString(refresh.state));

        if (refresh.znode.last_success_time.time_since_epoch().count() == 0)
        {
            res_columns[i++]->insertDefault(); // NULL
            res_columns[i++]->insertDefault();
        }
        else
        {
            res_columns[i++]->insert(refresh.znode.last_success_time.time_since_epoch().count());
            res_columns[i++]->insert(refresh.znode.last_success_duration.count());
        }

        if (refresh.znode.last_attempt_time.time_since_epoch().count() == 0)
            res_columns[i++]->insertDefault();
        else
            res_columns[i++]->insert(refresh.znode.last_attempt_time.time_since_epoch().count());
        res_columns[i++]->insert(refresh.znode.last_attempt_replica);

        res_columns[i++]->insert(std::chrono::duration_cast<std::chrono::seconds>(refresh.next_refresh_time.time_since_epoch()).count());

        if (refresh.znode.last_attempt_succeeded || refresh.znode.last_attempt_time.time_since_epoch().count() == 0)
            res_columns[i++]->insertDefault();
        else if (refresh.refresh_running)
            res_columns[i++]->insert(refresh.znode.previous_attempt_error);
        else if (refresh.znode.last_attempt_error.empty())
            res_columns[i++]->insert("Replica went away");
        else
            res_columns[i++]->insert(refresh.znode.last_attempt_error);

        Int64 retries = refresh.znode.attempt_number;
        if (refresh.refresh_running && retries)
            retries -= 1;
        res_columns[i++]->insert(retries);

        res_columns[i++]->insert(Float64(refresh.progress.read_rows) / std::max(refresh.progress.total_rows_to_read, UInt64(1)));
        res_columns[i++]->insert(refresh.progress.read_rows);
        res_columns[i++]->insert(refresh.progress.read_bytes);
        res_columns[i++]->insert(refresh.progress.total_rows_to_read);
        res_columns[i++]->insert(refresh.progress.written_rows);
        res_columns[i++]->insert(refresh.progress.written_bytes);
    }
}

}
