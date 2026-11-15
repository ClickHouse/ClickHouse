#pragma once

#include <Interpreters/BlobStorageLog.h>

namespace DB
{

using BlobStorageLogPtr = std::shared_ptr<BlobStorageLog>;

class BlobStorageLogWriter;
using BlobStorageLogWriterPtr = std::shared_ptr<BlobStorageLogWriter>;

/// Helper class to write events to BlobStorageLog
/// Can additionally hold some context information
class BlobStorageLogWriter : private boost::noncopyable
{
public:
    BlobStorageLogWriter() = default;

    explicit BlobStorageLogWriter(BlobStorageLogPtr log_)
        : log(std::move(log_))
    {
    }

    void addEvent(
        BlobStorageLogElement::EventType event_type,
        const String & bucket,
        const String & remote_path,
        const String & local_path,
        size_t data_size,
        size_t elapsed_microseconds,
        Int32 error_code,
        const String & error_message,
        BlobStorageLogElement::EvenTime time_now = {});

    /// A server-side copy of `source_bucket`/`source_remote_path` to `bucket`/`remote_path`.
    void addCopyEvent(
        const String & source_bucket,
        const String & source_remote_path,
        const String & bucket,
        const String & remote_path,
        size_t data_size,
        size_t elapsed_microseconds,
        Int32 error_code,
        const String & error_message);

    bool isInitialized() const { return log != nullptr; }

    /// Optional context information
    String disk_name;
    String query_id;
    String local_path;

    static BlobStorageLogWriterPtr create(const String & disk_name = "");

private:
    void addEventImpl(
        BlobStorageLogElement::EventType event_type,
        const String & bucket,
        const String & remote_path,
        const String & local_path_,
        const String & source_bucket,
        const String & source_remote_path,
        size_t data_size,
        size_t elapsed_microseconds,
        Int32 error_code,
        const String & error_message,
        BlobStorageLogElement::EvenTime time_now);

    BlobStorageLogPtr log;
};

}
