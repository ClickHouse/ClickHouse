#pragma once

#include <Common/QuillLogger_fwd.h>
#include <Common/ThreadPool_fwd.h>

#include <quill/core/Common.h>
#include <quill/Logger.h>
#include <quill/core/LogLevel.h>
#include <quill/sinks/FileSink.h>
#include <quill/sinks/StreamSink.h>
#include <quill/Frontend.h>

#include <deque>
#include <filesystem>
#include <future>
#include <optional>
#include <string_view>

using FreeThreadPool = ThreadPoolImpl<std::thread>;

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

struct QuillFrontendOptions
{
    static constexpr quill::QueueType queue_type = quill::QueueType::UnboundedBlocking;

    /**
   * Initial capacity of the queue. Used for UnboundedBlocking, UnboundedDropping, and
   * UnboundedUnlimited. Also serves as the capacity for BoundedBlocking and BoundedDropping.
   */
    static inline uint32_t initial_queue_capacity = 1024u;

    /**
   * Interval for retrying when using BoundedBlocking or UnboundedBlocking.
   * Applicable only when using BoundedBlocking or UnboundedBlocking.
   */
    static constexpr uint32_t blocking_queue_retry_interval_ns = 800;

    /**
   * Maximum capacity for unbounded queues (UnboundedBlocking, UnboundedDropping).
   * This defines the maximum size to which the queue can grow before blocking or dropping messages.
   */
    static inline size_t unbounded_queue_max_capacity = 8 * 1024u * 1024u; // 8 MiB

    /**
   * Enables huge pages on the frontend queues to reduce TLB misses. Available only for Linux.
   */
    static constexpr quill::HugePagesPolicy huge_pages_policy = quill::HugePagesPolicy::Never;

    /**
   * Define allocator for frontend queue.
   */
    static constexpr quill::AllocationPolicy allocation_policy = quill::AllocationPolicy::Global;
};

using QuillLogger = quill::LoggerImpl<QuillFrontendOptions>;
using QuillLoggerPtr = QuillLogger *;

void startQuillBackend(const Poco::Util::AbstractConfiguration * config = nullptr);

quill::LogLevel parseQuillLogLevel(std::string_view level);

class ConsoleSink : public quill::StreamSink
{
public:
    enum class Stream
    {
        STDOUT,
        STDERR,
    };

    constexpr std::string_view streamToString(Stream stream)
    {
        switch (stream)
        {
            case Stream::STDOUT:
                return "stdout";
            case Stream::STDERR:
                return "stderr";
        }
    }

    explicit ConsoleSink(Stream stream, bool enable_colors_ = false);

    ~ConsoleSink() override = default;

    void write_log(
        quill::MacroMetadata const * log_metadata,
        uint64_t log_timestamp,
        std::string_view thread_id,
        std::string_view thread_name,
        std::string const & process_id,
        std::string_view logger_name,
        quill::LogLevel log_level,
        std::string_view log_level_description,
        std::string_view log_level_short_code,
        std::vector<std::pair<std::string, std::string>> const * named_args,
        std::string_view log_message,
        std::string_view log_statement) override;

private:
    bool enable_colors = false;
};

class RotatingSinkConfiguration : public quill::FileSinkConfig
{
public:
    size_t max_file_size = 0;
    size_t max_backup_files = 0;
    bool compress = false;
    bool rotate_on_open = false;
};


class RotatingFileSink : public quill::FileSink
{
    using Base = quill::FileSink;

public:
    RotatingFileSink(const std::filesystem::path & filename, const RotatingSinkConfiguration & config_);

    void write_log(
        quill::MacroMetadata const * log_metadata,
        uint64_t log_timestamp,
        std::string_view thread_id,
        std::string_view thread_name,
        std::string const & process_id,
        std::string_view logger_name,
        quill::LogLevel log_level,
        std::string_view log_level_description,
        std::string_view log_level_short_code,
        std::vector<std::pair<std::string, std::string>> const * named_args,
        std::string_view log_message,
        std::string_view log_statement) override;

    void flush_sink() override;

    void closeFile();

private:
    void recoverFiles();

    void rotateFiles();

    struct FileInfo
    {
        FileInfo(std::filesystem::path base_filename_, uint32_t index_)
            : base_filename{std::move(base_filename_)}
            , index{index_}
        {
        }

        std::filesystem::path base_filename;
        uint32_t index;
    };

    std::deque<FileInfo> created_files;
    size_t current_log_size{0};
    RotatingSinkConfiguration config;

    std::unique_ptr<FreeThreadPool> pool;
    std::optional<std::future<bool>> compression_result;

    std::atomic<bool> close_current_file{false};
};

}
