#pragma once

#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/StreamLocalLimits.h>

namespace DB
{

struct Settings;
class ReadProgressCallback;

struct FormatParserSharedResources;
using FormatParserSharedResourcesPtr = std::shared_ptr<FormatParserSharedResources>;

/// When reading many files in one query, e.g. `SELECT ... FROM file('part{00..99}.parquet')`,
/// we want the file readers to share some resource limits, e.g. number of threads.
/// This struct is shared among such group of readers (IInputFormat instances).
///
/// Parsing resources are lazily initialized by the `IInputFormat` implementation. The executor
/// supplies the scan-progress callback before any readers start.
struct FormatParserSharedResources
{
    const size_t max_parsing_threads = 0;
    const size_t max_io_threads = 0;

    std::atomic<size_t> num_streams{0};
    ThreadPoolCallbackRunnerFast parsing_runner;
    ThreadPoolCallbackRunnerFast io_runner;

    /// IInputFormat implementation may put arbitrary state here.
    std::shared_ptr<void> opaque;

    FormatParserSharedResources(const Settings & settings, size_t num_streams_);

    static FormatParserSharedResourcesPtr singleThreaded(const Settings & settings);

    void finishStream();

    size_t getParsingThreadsPerReader() const;
    size_t getIOThreadsPerReader() const;

    void initOnce(std::function<void()> f);

    /// Formats that filter internally report decoded rows and bytes before filtering, instead of
    /// letting the enclosing storage source account for the returned chunks a second time.
    void setReadProgressCallback(const std::shared_ptr<ReadProgressCallback> & callback, std::shared_ptr<const StorageLimitsList> storage_limits_);
    bool hasReadProgressCallback() const { return bool(read_progress_callback); }
    bool reportReadProgress(size_t rows, size_t bytes);
    bool isReadStopped() const { return read_stopped.load(std::memory_order_relaxed); }

private:
    std::once_flag progress_init_flag;
    std::shared_ptr<ReadProgressCallback> read_progress_callback;
    std::shared_ptr<const StorageLimitsList> storage_limits;
    std::atomic<bool> read_stopped{false};

    /// For lazily initializing the fields above.
    std::once_flag init_flag;
    std::exception_ptr init_exception;
};
}
