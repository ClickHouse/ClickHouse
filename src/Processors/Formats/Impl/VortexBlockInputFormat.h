#pragma once

#include "config.h"

#if USE_VORTEX

#include <Core/BlockMissingValues.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <array>
#include <condition_variable>
#include <map>
#include <mutex>
#include <optional>

namespace arrow { class Schema; }
namespace arrow::io { class RandomAccessFile; }

struct VortexFFIRuntime;
struct VortexFFIReader;
struct VortexFFIScan;
enum class VortexFFIQueue : int32_t;

/// The Arrow C Data Interface struct the scan hands its chunks over in (`arrow/c/abi.h`).
struct ArrowArray;

namespace DB
{

class ArrowColumnToCHColumn;
class ShutdownHelper;
struct VortexReadContext;

/// Reads Vortex files (https://github.com/vortex-data/vortex, https://docs.vortex.dev/) through
/// the Rust `vortex` library, see `rust/workspace/vortex`. The library reads the file through a
/// callback backed by a seekable ClickHouse read buffer (or by an in-memory copy of the file if
/// the buffer is not seekable), and returns decoded chunks over the Arrow C Data Interface,
/// which are then converted to ClickHouse columns the same way as in the Arrow format.
///
/// Threading. The library owns no threads: it turns the work of a scan into tasks in two queues
/// (CPU: decoding, filtering, Arrow export; I/O: the read callback) and calls `onNotify` whenever a
/// task becomes runnable. ClickHouse then runs those tasks the same way it runs any other work: a
/// driver task on `parser_shared_resources->parsing_runner` (or `io_runner` for the I/O queue,
/// which is the same split as in `ParquetV3BlockInputFormat`) calls `vortex_ffi_runtime_run` until
/// the queue is empty. The number of drivers per queue is capped by this reader's share of
/// `max_parsing_threads` and `max_download_threads`.
///
/// Chunks are pushed, not pulled: the task that produced a chunk calls `onChunk`, which converts it
/// to a `Chunk` on that same thread (so the conversion is parallel too) and puts it into the
/// delivery queue; `read` takes chunks from there and returns the capacity to the scan, which
/// bounds how far ahead of the query the scan may run. The scan reports its end, or its first
/// error, to `onScanFinish`.
///
/// With `max_parsing_threads <= 1` there is no thread pool at all: `read` runs the tasks itself,
/// on the calling thread, as the Parquet reader does in the same case.
class VortexBlockInputFormat final : public IInputFormat
{
public:
    VortexBlockInputFormat(
        ReadBuffer & in_,
        SharedHeader header_,
        const FormatSettings & format_settings_,
        FormatParserSharedResourcesPtr parser_shared_resources_,
        FormatFilterInfoPtr format_filter_info_,
        bool is_remote_fs_);
    ~VortexBlockInputFormat() override;

    String getName() const override { return "VortexBlockInputFormat"; }

    void resetParser() override;

    void resetReadBuffer() override;

    const BlockMissingValues * getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

    /// Called by the library (through a C callback) when a task of the given queue became runnable.
    /// Any thread, including one that is already running tasks of this reader.
    void onNotify(VortexFFIQueue queue) noexcept;

    /// Called by a scan task with the chunk it produced (a null `array` means the split matched no
    /// rows) and the index of its row split. Returns 0 on success, 1 to stop the scan.
    int32_t onChunk(::ArrowArray * array, UInt64 split_index) noexcept;

    /// Called by the scan when it ends: `error` is null on success. (Not `onFinish`: that is the
    /// hook `ISource` calls when the source is done, see `IInputFormat::onFinish`.)
    void onScanFinish(const char * error) noexcept;

private:
    /// A chunk of the scan, converted and waiting to be returned from `read`.
    struct DeliveredChunk
    {
        Chunk chunk;
        BlockMissingValues missing_values;
        /// Whether the split produced no rows: nothing to return, but the index has to be consumed
        /// to keep the file order.
        bool empty = false;
        /// Whether the scan is holding capacity for this chunk until `read` returns it.
        bool holds_permit = false;
    };

    static constexpr size_t NUM_QUEUES = 2;

    Chunk read() override;

    void onCancel() noexcept override;

    void prepareReader();
    void closeReader();

    /// Produces chunks for queries that need no columns from the file (e.g. `SELECT count()`),
    /// where only the number of rows matters.
    Chunk readWithoutColumns();

    /// Runs the tasks of one queue until it is empty. The body of a driver task.
    void driveQueue(VortexFFIQueue queue, std::shared_ptr<ShutdownHelper> shutdown_) noexcept;

    /// The maximum number of drivers of the given queue this reader may have running.
    size_t maxDrivers(VortexFFIQueue queue) const;

    /// The runner the drivers of the given queue are scheduled on.
    ThreadPoolCallbackRunnerFast & runnerFor(VortexFFIQueue queue) const;

    /// Whether the reads have a pool of their own (`max_download_threads`), or share the parsing one.
    bool hasSeparateIORunner() const;

    /// The queue whose drivers run the tasks of `queue`: with one shared runner, the drivers of the
    /// CPU queue run both, so that the pool is not oversubscribed.
    VortexFFIQueue driverQueue(VortexFFIQueue queue) const;

    /// Cancels the scan (thread-safe): the tasks stop as soon as they can.
    void cancelScan() noexcept;

    /// Cancels the scan and waits for the drivers, so that no task of this reader runs afterwards.
    void stopTasks();

    /// Records the first error of a background task and wakes up `read`. Also cancels the scan, as
    /// the rest of it is not needed anymore, unless `cancel_scan` is false: cancelling enters the
    /// library, which `onNotify` must not do (see there).
    void setBackgroundException(std::exception_ptr exception, bool cancel_scan = true) noexcept;

    std::unique_ptr<ArrowColumnToCHColumn> createConverter() const;

    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    std::unique_ptr<VortexReadContext> read_context;
    VortexFFIRuntime * runtime = nullptr;
    VortexFFIReader * reader = nullptr;
    std::shared_ptr<arrow::Schema> file_schema;

    /// The scan and the schema of the chunks it produces. `onCancel` may run concurrently with
    /// `read`, so the creation, cancellation and destruction of the scan are serialized by the mutex.
    std::mutex scan_mutex;
    VortexFFIScan * scan = nullptr;
    std::shared_ptr<arrow::Schema> scan_schema;

    /// Everything up to `converters_mutex` is guarded by `delivery_mutex`.
    std::mutex delivery_mutex;
    /// Notified when a chunk is delivered, when the scan ends, and on cancellation.
    std::condition_variable delivery_cv;
    /// Converted chunks by split index.
    std::map<UInt64, DeliveredChunk> delivered;
    /// The split index the next chunk must have when `preserve_order` is set.
    UInt64 next_split_index = 0;
    /// The scan reported its end (or its cancellation).
    bool scan_finished = false;
    /// The first error of a background task or of the scan; rethrown by `read`.
    std::exception_ptr background_exception;

    /// The converters of the threads that deliver chunks (`ArrowColumnToCHColumn` caches
    /// dictionaries and is not thread-safe, so a thread takes one for the duration of a conversion).
    std::mutex converters_mutex;
    std::vector<std::unique_ptr<ArrowColumnToCHColumn>> converters;

    /// Drivers running or scheduled per queue. Not under `delivery_mutex`: `onNotify` is called
    /// from the library, including from inside a task, and must not take a lock that a delivering
    /// thread may hold.
    std::array<std::atomic<size_t>, NUM_QUEUES> running_drivers{};

    /// Guards `this` against the driver tasks: they hold it shared while running, and `stopTasks`
    /// waits for them. Recreated for every scan.
    std::shared_ptr<ShutdownHelper> tasks_shutdown;
    /// Set while the reader is being closed: no new drivers are scheduled.
    std::atomic<bool> closing{false};

    /// The number of rows left to return for queries that read no columns from the file.
    UInt64 pending_rows_without_columns = 0;
    bool count_returned = false;

    /// The missing values and the read bytes of the last chunk returned from `read`.
    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;
    size_t previous_approx_bytes_read = 0;

    const FormatSettings format_settings;
    FormatParserSharedResourcesPtr parser_shared_resources;
    FormatFilterInfoPtr format_filter_info;
    const bool is_remote_fs;

    std::atomic<int> is_stopped{0};
};

class VortexSchemaReader final : public ISchemaReader
{
public:
    VortexSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);
    ~VortexSchemaReader() override;

    NamesAndTypesList readSchema() override;

    std::optional<size_t> readNumberOrRows() override;

private:
    void initializeIfNeeded();

    const FormatSettings format_settings;

    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    std::unique_ptr<VortexReadContext> read_context;
    /// A runtime of its own, driven by the calling thread: reading the schema needs no parallelism.
    VortexFFIRuntime * runtime = nullptr;
    VortexFFIReader * reader = nullptr;
    std::shared_ptr<arrow::Schema> file_schema;

    /// Never set; the file wrapper created by asArrowFile keeps a reference to it.
    std::atomic<int> is_stopped{0};
};

}

#endif
