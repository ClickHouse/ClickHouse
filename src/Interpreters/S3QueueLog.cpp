#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <Interpreters/ProfileEventsExt.h>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/S3QueueLog.h>


namespace DB
{

NamesAndTypesList S3QueueLogElement::getNamesAndTypes()
{
    auto status_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Processed", static_cast<Int8>(S3QueueLogElement::S3QueueStatus::Processed)},
            {"Failed", static_cast<Int8>(S3QueueLogElement::S3QueueStatus::Failed)},
        });
    return {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"table_uuid", std::make_shared<DataTypeString>()},
        {"file_name", std::make_shared<DataTypeString>()},
        {"rows_processed", std::make_shared<DataTypeUInt64>()},
        {"status", status_datatype},
    };
}

void S3QueueLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(table_uuid);
    columns[i++]->insert(file_name);
    columns[i++]->insert(rows_processed);
    columns[i++]->insert(magic_enum::enum_name(status));
}

}
