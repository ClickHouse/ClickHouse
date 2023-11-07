#include <Interpreters/BlobStorageLog.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeDate.h>


namespace DB
{

NamesAndTypesList BlobStorageLogElement::getNamesAndTypes()
{
    auto event_enum_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values{
            {"Upload", static_cast<Int8>(EventType::Upload)},
            {"Delete", static_cast<Int8>(EventType::Delete)},
            {"MultiPartUploadCreate", static_cast<Int8>(EventType::MultiPartUploadCreate)},
            {"MultiPartUploadWrite", static_cast<Int8>(EventType::MultiPartUploadWrite)},
            {"MultiPartUploadComplete", static_cast<Int8>(EventType::MultiPartUploadComplete)},
            {"MultiPartUploadAbort", static_cast<Int8>(EventType::MultiPartUploadAbort)},
        });

    return {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},

        {"event_type", event_enum_type},

        {"query_id", std::make_shared<DataTypeString>()},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"thread_name", std::make_shared<DataTypeString>()},

        {"disk_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"bucket", std::make_shared<DataTypeString>()},
        {"remote_path", std::make_shared<DataTypeString>()},
        {"local_path", std::make_shared<DataTypeString>()},
        {"data_size", std::make_shared<DataTypeUInt64>()},

        {"error", std::make_shared<DataTypeString>()},
    };
}

void BlobStorageLogElement::appendToBlock(MutableColumns & columns) const
{
#ifndef NDEBUG
    auto coulumn_names = BlobStorageLogElement::getNamesAndTypes().getNames();
#endif

    size_t i = 0;

    auto event_time_seconds = timeInSeconds(event_time);
    assert(coulumn_names.at(i) == "event_date");
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time_seconds).toUnderType());
    assert(coulumn_names.at(i) == "event_time");
    columns[i++]->insert(event_time_seconds);
    assert(coulumn_names.at(i) == "event_time_microseconds");
    columns[i++]->insert(Decimal64(timeInMicroseconds(event_time)));

    assert(coulumn_names.at(i) == "event_type");
    columns[i++]->insert(static_cast<Int8>(event_type));

    assert(coulumn_names.at(i) == "query_id");
    columns[i++]->insert(query_id);
    assert(coulumn_names.at(i) == "thread_id");
    columns[i++]->insert(thread_id);
    assert(coulumn_names.at(i) == "thread_name");
    columns[i++]->insert(thread_name);

    assert(coulumn_names.at(i) == "disk_name");
    columns[i++]->insert(disk_name);
    assert(coulumn_names.at(i) == "bucket");
    columns[i++]->insert(bucket);
    assert(coulumn_names.at(i) == "remote_path");
    columns[i++]->insert(remote_path);
    assert(coulumn_names.at(i) == "local_path");
    columns[i++]->insert(local_path);
    assert(coulumn_names.at(i) == "data_size");
    columns[i++]->insert(data_size);

    assert(coulumn_names.at(i) == "error");
    columns[i++]->insert(error_message);

    assert(i == coulumn_names.size() && columns.size() == coulumn_names.size());
}

}

