#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>


namespace DB
{

ColumnsDescription FilesystemReadPrefetchesLogElement::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"path", std::make_shared<DataTypeString>()},
        {"offset", std::make_shared<DataTypeUInt64>()},
        {"size", std::make_shared<DataTypeInt64>()},
        {"prefetch_submit_time", std::make_shared<DataTypeDateTime64>(6)},
        {"priority", std::make_shared<DataTypeInt64>()},
        {"prefetch_execution_start_time", std::make_shared<DataTypeDateTime64>(6)},
        {"prefetch_execution_end_time", std::make_shared<DataTypeDateTime64>(6)},
        {"prefetch_execution_time_us", std::make_shared<DataTypeUInt64>()},
        {"state", std::make_shared<DataTypeString>()}, /// Was this prefetch used or we downloaded it in vain?
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"reader_id", std::make_shared<DataTypeString>()},
    };
}

void FilesystemReadPrefetchesLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(query_id);
    columns[i++]->insert(path);
    columns[i++]->insert(offset);
    columns[i++]->insert(size);
    columns[i++]->insert(std::chrono::duration_cast<std::chrono::microseconds>(prefetch_submit_time.time_since_epoch()).count());
    columns[i++]->insert(priority.value);
    if (execution_watch)
    {
        columns[i++]->insert(execution_watch->getStart() / 1000);
        columns[i++]->insert(execution_watch->getEnd() / 1000);
        columns[i++]->insert(execution_watch->elapsedMicroseconds());
    }
    else
    {
        columns[i++]->insertDefault();
        columns[i++]->insertDefault();
        columns[i++]->insertDefault();
    }
    columns[i++]->insert(magic_enum::enum_name(state));
    columns[i++]->insert(thread_id);
    columns[i++]->insert(reader_id);
}

}
