#include <base/getFQDNOrHostName.h>
#include <Interpreters/MemoryDumpLog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/ClickHouseRevision.h>


namespace DB
{

NamesAndTypesList MemoryDumpLogElement::getNamesAndTypes()
{
    return
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"revision", std::make_shared<DataTypeUInt32>()},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"size", std::make_shared<DataTypeInt64>()},
        {"ptr", std::make_shared<DataTypeUInt64>()},
    };
}

void MemoryDumpLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(ClickHouseRevision::getVersionRevision());
    columns[i++]->insert(thread_id);
    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insert(trace);
    columns[i++]->insert(size);
    columns[i++]->insert(ptr);
}

}
