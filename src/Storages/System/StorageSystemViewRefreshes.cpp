#include <Storages/System/StorageSystemViewRefreshes.h>

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/MaterializedView/RefreshSet.h>


namespace DB
{

ColumnsDescription StorageSystemViewRefreshes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "The name of the database the table is in."},
        {"view", std::make_shared<DataTypeString>(), "Table name."},
        {"status", std::make_shared<DataTypeString>(), "Current state of the refresh."},
        {"last_refresh_result", std::make_shared<DataTypeString>(), "Outcome of the latest refresh attempt."},
        {"last_refresh_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()),
            "Time of the last refresh attempt. NULL if no refresh attempts happened since server startup or table creation."},
        {"last_success_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()),
            "Time of the last successful refresh. NULL if no successful refreshes happened since server startup or table creation."},
        {"duration_ms", std::make_shared<DataTypeUInt64>(), "How long the last refresh attempt took."},
        {"next_refresh_time", std::make_shared<DataTypeDateTime>(), "Time at which the next refresh is scheduled to start."},
        {"remaining_dependencies", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "If the view has refresh dependencies, this array contains the subset of those dependencies that are not satisfied for the current refresh yet. "
            "If status = 'WaitingForDependencies', a refresh is ready to start as soon as these dependencies are fulfilled."
        },
        {"exception", std::make_shared<DataTypeString>(),
            "if last_refresh_result = 'Exception', i.e. the last refresh attempt failed, this column contains the corresponding error message and stack trace."
        },
        {"refresh_count", std::make_shared<DataTypeUInt64>(), "Number of successful refreshes since last server restart or table creation."},
        {"progress", std::make_shared<DataTypeFloat64>(), "Progress of the current refresh, between 0 and 1."},
        {"elapsed", std::make_shared<DataTypeFloat64>(), "The amount of nanoseconds the current refresh took."},
        {"read_rows", std::make_shared<DataTypeUInt64>(), "Number of rows read during the current refresh."},
        {"read_bytes", std::make_shared<DataTypeUInt64>(), "Number of bytes read during the current refresh."},
        {"total_rows", std::make_shared<DataTypeUInt64>(), "Estimated total number of rows that need to be read by the current refresh."},
        {"total_bytes", std::make_shared<DataTypeUInt64>(), "Estimated total number of bytes that need to be read by the current refresh."},
        {"written_rows", std::make_shared<DataTypeUInt64>(), "Number of rows written during the current refresh."},
        {"written_bytes", std::make_shared<DataTypeUInt64>(), "Number rof bytes written during the current refresh."},
        {"result_rows", std::make_shared<DataTypeUInt64>(), "Estimated total number of rows in the result set of the SELECT query."},
        {"result_bytes", std::make_shared<DataTypeUInt64>(), "Estimated total number of bytes in the result set of the SELECT query."},
    };
}

void StorageSystemViewRefreshes::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto access = context->getAccess();
    auto valid_access = AccessType::SHOW_TABLES;
    bool check_access_for_tables = !access->isGranted(valid_access);

    for (const auto & refresh : context->getRefreshSet().getInfo())
    {
        if (check_access_for_tables && !access->isGranted(valid_access, refresh.view_id.getDatabaseName(), refresh.view_id.getTableName()))
            continue;

        std::size_t i = 0;
        res_columns[i++]->insert(refresh.view_id.getDatabaseName());
        res_columns[i++]->insert(refresh.view_id.getTableName());
        res_columns[i++]->insert(toString(refresh.state));
        res_columns[i++]->insert(toString(refresh.last_refresh_result));

        if (refresh.last_attempt_time.has_value())
            res_columns[i++]->insert(refresh.last_attempt_time.value());
        else
            res_columns[i++]->insertDefault(); // NULL

        if (refresh.last_success_time.has_value())
            res_columns[i++]->insert(refresh.last_success_time.value());
        else
            res_columns[i++]->insertDefault(); // NULL

        res_columns[i++]->insert(refresh.last_attempt_duration_ms);
        res_columns[i++]->insert(refresh.next_refresh_time);

        Array deps;
        for (const StorageID & id : refresh.remaining_dependencies)
            deps.push_back(id.getFullTableName());
        res_columns[i++]->insert(Array(deps));

        res_columns[i++]->insert(refresh.exception_message);
        res_columns[i++]->insert(refresh.refresh_count);
        res_columns[i++]->insert(Float64(refresh.progress.read_rows) / refresh.progress.total_rows_to_read);
        res_columns[i++]->insert(refresh.progress.elapsed_ns / 1e9);
        res_columns[i++]->insert(refresh.progress.read_rows);
        res_columns[i++]->insert(refresh.progress.read_bytes);
        res_columns[i++]->insert(refresh.progress.total_rows_to_read);
        res_columns[i++]->insert(refresh.progress.total_bytes_to_read);
        res_columns[i++]->insert(refresh.progress.written_rows);
        res_columns[i++]->insert(refresh.progress.written_bytes);
        res_columns[i++]->insert(refresh.progress.result_rows);
        res_columns[i++]->insert(refresh.progress.result_bytes);
    }
}

}
