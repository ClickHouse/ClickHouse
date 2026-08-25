#include <Interpreters/BackgroundSchedulePoolLog.h>
#include <Common/SystemTableDocumentation.h>

#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>


namespace DB
{

ColumnsDescription BackgroundSchedulePoolLogElement::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"hostname", low_cardinality_string, "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds precision."},

        {"query_id", std::make_shared<DataTypeString>(), "Identifier of the query associated with the background task."},
        {"database", low_cardinality_string, "Name of the database."},
        {"table", low_cardinality_string, "Name of the table."},
        {"table_uuid", std::make_shared<DataTypeUUID>(), "UUID of the table the background task belongs to."},
        {"log_name", low_cardinality_string, "Name of the background task."},

        {"duration_ms", std::make_shared<DataTypeUInt64>(), "Duration of the task execution in milliseconds."},

        {"error", std::make_shared<DataTypeUInt16>(), "The error code of the occurred exception."},
        {"exception", std::make_shared<DataTypeString>(), "Text message of the occurred error."},
    };
}

NamesAndAliases BackgroundSchedulePoolLogElement::getNamesAndAliases()
{
    return {};
}

void BackgroundSchedulePoolLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insert(query_id);
    columns[i++]->insert(database_name);
    columns[i++]->insert(table_name);
    columns[i++]->insert(table_uuid);
    columns[i++]->insert(log_name);

    columns[i++]->insert(duration_ms);

    columns[i++]->insert(error);
    columns[i++]->insert(exception);
}

}

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "background_schedule_pool_log",
    .description = R"DOCS_MD(
The `system.background_schedule_pool_log` table is created only if the [background_schedule_pool_log](/reference/settings/server-settings/settings/background-schedule#background_schedule_pool_log) server setting is specified.

This table contains the history of background schedule pool task executions. Background schedule pools are used for executing periodic tasks such as distributed sends, buffer flushes, and message broker operations.
)DOCS_MD",
    .get_columns = BackgroundSchedulePoolLogElement::getColumnsDescription,
    .columns_notes = R"DOCS_MD(
The `system.background_schedule_pool_log` table is created after the first background task execution.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.background_schedule_pool_log LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-12-18
event_time:              2025-12-18 10:30:15
event_time_microseconds: 2025-12-18 10:30:15.123456
query_id:
database:                default
table:                   data
table_uuid:              00000000-0000-0000-0000-000000000000
log_name:                default.data
duration_ms:             42
error:                   0
exception:
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [system.background_schedule_pool](/reference/system-tables/background_schedule_pool) — Contains information about currently scheduled tasks in background schedule pools.
)DOCS_MD")

}
