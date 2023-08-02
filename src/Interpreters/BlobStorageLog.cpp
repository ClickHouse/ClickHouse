#include <Interpreters/BlobStorageLog.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
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
        {"event_type", event_enum_type},

        {"query_id", std::make_shared<DataTypeString>()},
        {"thread_id", std::make_shared<DataTypeUInt64>()},

        {"disk_name", std::make_shared<DataTypeString>()},
        {"bucket", std::make_shared<DataTypeString>()},
        {"remote_path", std::make_shared<DataTypeString>()},
        {"referring_local_path", std::make_shared<DataTypeString>()},

        {"error_msg", std::make_shared<DataTypeString>()},

        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
    };
}

void BlobStorageLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(static_cast<Int8>(event_type));

    columns[i++]->insert(query_id);
    columns[i++]->insert(thread_id);

    columns[i++]->insert(disk_name);
    columns[i++]->insert(bucket);
    columns[i++]->insert(remote_path);
    columns[i++]->insert(referring_local_path);

    columns[i++]->insert(error_msg);

    auto event_time_seconds = timeInSeconds(event_time);
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time_seconds).toUnderType());
    columns[i++]->insert(event_time_seconds);
    columns[i++]->insert(timeInMicroseconds(event_time));

    assert([&i]()
    {
        size_t total_colums = BlobStorageLogElement::getNamesAndTypes().size();
        return i == total_colums;
    }());
}


void BlobStorageLogWriter::addEvent(
    BlobStorageLogElement::EventType event_type,
    const String & bucket,
    const String & remote_path,
    const String & local_path,
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
    element.referring_local_path = local_path.empty() ? referring_local_path : local_path;

    if (error)
    {
        element.error_code = static_cast<Int32>(error->GetErrorType());
        element.error_msg = error->GetMessage();
    }

    element.event_time = time_now;

    log->add(element);
}

}
