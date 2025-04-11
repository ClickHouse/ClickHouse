#include <IO/S3/BlobStorageLogWriter.h>

#if USE_AWS_S3

#include <base/getThreadId.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <IO/S3/Client.h>
#include <Interpreters/Context.h>

namespace DB
{

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

    if (log->shouldIgnorePath(local_path_.empty() ? local_path : local_path_))
        return;

    if (!time_now.time_since_epoch().count())
        time_now = std::chrono::system_clock::now();

    BlobStorageLogElement element;

    element.event_type = event_type;

    element.query_id = query_id;
    element.thread_id = getThreadId();
    element.thread_name = getThreadName();

    element.disk_name = disk_name;
    element.bucket = bucket;
    element.remote_path = remote_path;
    element.local_path = local_path_.empty() ? local_path : local_path_;
    element.data_size = data_size;

    if (error)
    {
        element.error_code = static_cast<Int32>(error->GetErrorType());
        element.error_message = error->GetMessage();
    }

    element.event_time = time_now;

    log->add(element);
}

BlobStorageLogWriterPtr BlobStorageLogWriter::create(const String & disk_name)
{
    if (auto blob_storage_log = Context::getGlobalContextInstance()->getBlobStorageLog())
    {
        auto log_writer = std::make_shared<BlobStorageLogWriter>(std::move(blob_storage_log));

        log_writer->disk_name = disk_name;
        if (CurrentThread::isInitialized() && CurrentThread::get().getQueryContext())
            log_writer->query_id = CurrentThread::getQueryId();

        return log_writer;
    }
    return {};
}

}

#endif
