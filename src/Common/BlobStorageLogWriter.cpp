#include <Common/BlobStorageLogWriter.h>

#include <Common/HTTPConnectionInfo.h>

#include <base/getThreadId.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
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
    /// Which connection carried the request we are about to log. Taken here, before anything can
    /// return early, so that the slot is always emptied: it is filled by every pooled HTTP request,
    /// including the ones whose event is never logged, and a value left behind would be picked up
    /// by whatever this thread logs next - attributing an unrelated socket to it.
    const auto connection = takeCurrentHTTPConnectionInfo();

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

    log->add([&](BlobStorageLogElement & element)
    {
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

        if (connection.has_value)
        {
            element.connection_id = connection.id;
            element.connection_local_port = connection.local_port;
            element.connection_socket_inode = connection.socket_inode;
            element.connection_requests = connection.requests_served;
            element.connection_age_microseconds = connection.age_microseconds;
            element.connection_idle_microseconds = connection.idle_microseconds;
        }

        element.error_code = error_code;
        element.error_message = error_message;

        element.event_time = time_now;
    });
}

BlobStorageLogWriterPtr BlobStorageLogWriter::create(const String & disk_name)
{
    /// Prefer the current query context so that per-query settings such as `enable_blob_storage_log`
    /// are honoured. Fall back to the global context for background operations that have no
    /// associated query.
    ContextPtr context = CurrentThread::tryGetQueryContext();
    if (!context)
        context = Context::getGlobalContextInstance();

    if (auto blob_storage_log = context->getBlobStorageLog())
    {
        auto log_writer = std::make_shared<BlobStorageLogWriter>(std::move(blob_storage_log));

        log_writer->disk_name = disk_name;
        if (CurrentThread::isInitialized() && CurrentThread::get().tryGetQueryContext())
            log_writer->query_id = CurrentThread::getQueryId();

        return log_writer;
    }
    return {};
}

}
