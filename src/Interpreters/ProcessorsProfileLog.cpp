#include <Interpreters/ProcessorsProfileLog.h>

#include <Common/ClickHouseRevision.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <base/logger_useful.h>

#include <array>

namespace DB
{

NamesAndTypesList ProcessorProfileLogElement::getNamesAndTypes()
{
    return
    {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},

        {"query_id", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"elapsed_us", std::make_shared<DataTypeUInt64>()},
        {"need_data_elapsed_us", std::make_shared<DataTypeUInt64>()},
        {"port_full_elapsed_us", std::make_shared<DataTypeUInt64>()},
    };
}

void ProcessorProfileLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insertData(processor_name.data(), processor_name.size());
    columns[i++]->insert(elapsed_us);
    columns[i++]->insert(need_data_elapsed_us);
    columns[i++]->insert(port_full_elapsed_us);
}

ProcessorsProfileLog::ProcessorsProfileLog(ContextPtr context_, const String & database_name_,
        const String & table_name_, const String & storage_def_,
        size_t flush_interval_milliseconds_)
  : SystemLog<ProcessorProfileLogElement>(context_, database_name_, table_name_,
        storage_def_, flush_interval_milliseconds_)
{
}

}
