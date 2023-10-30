#include <Interpreters/BlobStorageLog.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeDate.h>
#include <base/getThreadId.h>
#include <IO/S3/Client.h>

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

        {"disk_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"bucket", std::make_shared<DataTypeString>()},
        {"remote_path", std::make_shared<DataTypeString>()},
        {"local_path", std::make_shared<DataTypeString>()},
        {"data_size", std::make_shared<DataTypeUInt32>()},

        {"error_msg", std::make_shared<DataTypeString>()},
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
    columns[i++]->insert(timeInMicroseconds(event_time));

    assert(coulumn_names.at(i) == "event_type");
    columns[i++]->insert(static_cast<Int8>(event_type));

    assert(coulumn_names.at(i) == "query_id");
    columns[i++]->insert(query_id);
    assert(coulumn_names.at(i) == "thread_id");
    columns[i++]->insert(thread_id);

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

    assert(coulumn_names.at(i) == "error_msg");
    columns[i++]->insert(error_msg);

    assert(i == coulumn_names.size() && columns.size() == coulumn_names.size());
}


void BlobStorageLogWriter::addEvent(
    BlobStorageLogElement::EventType event_type,
    const String & bucket,
    const String & remote_path,
    const String & local_path_,
    size_t data_size,
    const Aws::S3::S3Error * error,
    BlobStorageLogElement::EvenTime time_now)
{
    if (!log)
        return;

    if (!time_now.time_since_epoch().count())
        time_now = std::chrono::system_clock::now();

    BlobStorageLogElement element;

    element.event_type = event_type;

    element.query_id = query_id;
    element.thread_id = getThreadId();

    element.disk_name = disk_name;
    element.bucket = bucket;
    element.remote_path = remote_path;
    element.local_path = local_path_.empty() ? local_path : local_path_;

    if (data_size > std::numeric_limits<decltype(element.data_size)>::max())
        element.data_size = std::numeric_limits<decltype(element.data_size)>::max();
    else
        element.data_size = static_cast<decltype(element.data_size)>(data_size);

    if (error)
    {
        element.error_code = static_cast<Int32>(error->GetErrorType());
        element.error_msg = error->GetMessage();
    }

    element.event_time = time_now;

    log->add(element);
}

bool BlobStorageLogWriter::operator==(const BlobStorageLogWriter & other) const
{
    return log.get() == other.log.get()
        && disk_name == other.disk_name
        && query_id == other.query_id
        && local_path == other.local_path;
}

}
