#include <IO/S3/BlobStorageLogWriter.h>

#include <base/getThreadId.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>


namespace DB
{

void BlobStorageLogWriter::addEvent(
    BlobStorageLogElement::EventType event_type,
    const String & bucket,
    const String & remote_path,
    const String & local_path_,
    size_t data_size,
    size_t elapsed_microseconds,
    Int32 error_code,
    const String & error_message,
    BlobStorageLogElement::EvenTime time_now)
{
    if (!log)
    {
        LOG_TEST(getLogger("BlobStorageLogWriter"), "No log, skipping {}", remote_path);
        return;
    }

    if (log->shouldIgnorePath(local_path_.empty() ? local_path : local_path_))
    {
        LOG_TRACE(getLogger("BlobStorageLogWriter"), "No log, skipping {}, because should ignore", remote_path);
        return;
    }

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
    element.elapsed_microseconds = elapsed_microseconds;
    element.error_code = error_code;
    element.error_message = error_message;

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
