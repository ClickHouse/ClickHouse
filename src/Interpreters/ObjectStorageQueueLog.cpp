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
#include <Interpreters/ObjectStorageQueueLog.h>


namespace DB
{

ColumnsDescription ObjectStorageQueueLogElement::getColumnsDescription()
{
    auto status_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Processed", static_cast<Int8>(ObjectStorageQueueLogElement::ObjectStorageQueueStatus::Processed)},
            {"Failed", static_cast<Int8>(ObjectStorageQueueLogElement::ObjectStorageQueueStatus::Failed)},
        });

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname"},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date of writing this log row"},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time of writing this log row"},
        {"database", std::make_shared<DataTypeString>(), "The name of a database where current S3Queue table lives."},
        {"table", std::make_shared<DataTypeString>(), "The name of S3Queue table."},
        {"uuid", std::make_shared<DataTypeString>(), "The UUID of S3Queue table"},
        {"file_name", std::make_shared<DataTypeString>(), "File name of the processing file"},
        {"rows_processed", std::make_shared<DataTypeUInt64>(), "Number of processed rows"},
        {"status", status_datatype, "Status of the processing file"},
        {"processing_start_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time of the start of processing the file"},
        {"processing_end_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time of the end of processing the file"},
        {"exception", std::make_shared<DataTypeString>(), "Exception message if happened"},
    };
}

void ObjectStorageQueueLogElement::appendToBlock(MutableColumns & columns) const
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

    columns[i++]->insert(exception);
}

}
