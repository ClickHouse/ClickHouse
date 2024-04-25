#include <base/getFQDNOrHostName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <Interpreters/ProfileEventsExt.h>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/S3QueueLog.h>


namespace DB
{

ColumnsDescription S3QueueLogElement::getColumnsDescription()
{
    auto status_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Processed", static_cast<Int8>(S3QueueLogElement::S3QueueStatus::Processed)},
            {"Failed", static_cast<Int8>(S3QueueLogElement::S3QueueStatus::Failed)},
        });

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeString>()},
        {"file_name", std::make_shared<DataTypeString>()},
        {"rows_processed", std::make_shared<DataTypeUInt64>()},
        {"status", status_datatype},
        {"processing_start_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())},
        {"processing_end_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())},
        {"ProfileEvents", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>())},
        {"exception", std::make_shared<DataTypeString>()},
    };
}

void S3QueueLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(database);
    columns[i++]->insert(table);
    columns[i++]->insert(uuid);
    columns[i++]->insert(file_name);
    columns[i++]->insert(rows_processed);
    columns[i++]->insert(status);

    if (processing_start_time)
        columns[i++]->insert(processing_start_time);
    else
        columns[i++]->insertDefault();

    if (processing_end_time)
        columns[i++]->insert(processing_end_time);
    else
        columns[i++]->insertDefault();

    ProfileEvents::dumpToMapColumn(counters_snapshot, columns[i++].get(), true);

    columns[i++]->insert(exception);
}

}
