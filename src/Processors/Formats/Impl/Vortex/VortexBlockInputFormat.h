#pragma once

#include "config.h"

#if USE_VORTEX

#include <Core/BlockMissingValues.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <base/defines.h>
#include <Common/Logger.h>

#include <array>
#include <condition_variable>
#include <map>
#include <mutex>
#include <optional>

namespace arrow
{
class Schema;
}
namespace arrow::io
{
class RandomAccessFile;
}

struct FFI_VortexRuntime;
struct FFI_VortexReader;
struct FFI_VortexScan;
enum class FFI_VortexTaskQueue : int32_t;

struct ArrowArray;

namespace DB::Vortex
{
struct VortexReadContext;
}

namespace DB
{

class ArrowColumnToCHColumn;
class ShutdownHelper;

/// Reads Vortex files (https://docs.vortex.dev/) through the Rust bindings in
/// `rust/workspace/vortex`. The bindings own no threads: a scan is split into tasks that wait in
/// two queues, one for decoding and one for reads, and ClickHouse decides when and on which thread
/// each of them runs. The library calls `onNotify` when it queues a task, driver tasks on the
/// parsing and download pools run them in batches, and a decoded chunk arrives at `onChunk` on the
/// thread that decoded it, so the conversion to ClickHouse columns is parallel too. `read` takes
/// finished chunks from `delivered`; returning one lets the scan start another split, which is what
/// keeps it from running far ahead of the query.
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

    /// Schedules a driver task to run the work the library has just queued for `queue`, unless this
    /// reader already has as many drivers running as its share of the thread pool allows. Called by
    /// the library from the thread that queued the work, which may be one already running tasks of
    /// this reader.
    void onNotify(FFI_VortexTaskQueue queue) noexcept;

    /// Converts the Arrow array a split task decoded into a `Chunk` and puts it in the delivery
    /// queue under `split_index`, where `read` picks it up. Runs on the thread that decoded the
    /// split. A null `array` means the split matched no rows, so there is nothing to convert.
    /// Returns 0, or non-zero to make the library stop the scan.
    int32_t onChunk(::ArrowArray * array, UInt64 split_index) noexcept;

    /// Records the outcome of the scan and wakes `read` so that it stops waiting for more chunks.
    /// `error` is null when the scan read the file to the end.
    void onScanFinish(const char * error) noexcept;

private:
    struct DeliveredChunk
    {
        Chunk chunk;
        BlockMissingValues missing_values;
        /// There is nothing to return, but the index still has to be consumed to keep file order.
        bool empty = false;
        /// The scan counts this chunk against its in-flight limit until `read` releases it.
        bool holds_permit = false;
    };

    static constexpr size_t NUM_QUEUES = 2;

    /// Unlocks and re-locks `delivery_mutex` inside its wait loop, which the thread-safety
    /// analysis cannot follow.
    Chunk read() override TSA_NO_THREAD_SAFETY_ANALYSIS;

    void onCancel() noexcept override;

    void prepareReader();
    void closeReader();

    /// The chunks of a query that needs no column of the file, such as `SELECT count()`: only
    /// their number of rows matters.
    Chunk readWithoutColumns();

    /// Runs the queued tasks of `queue`, and of the other queue too when both share a thread pool,
    /// until nothing is left to run. This is the body of a driver task. `shutdown_` is passed by
    /// value because the task may still be waiting in the pool when the reader is destroyed.
    void driveQueue(FFI_VortexTaskQueue queue, std::shared_ptr<ShutdownHelper> shutdown_) noexcept;

    /// How many drivers of `queue` this reader may have running at once.
    size_t maxDrivers(FFI_VortexTaskQueue queue) const;

    ThreadPoolCallbackRunnerFast & runnerFor(FFI_VortexTaskQueue queue) const;

    /// Whether the reads have a pool of their own (`max_download_threads`) or share the parsing one.
    bool hasSeparateIORunner() const;

    /// The queue whose drivers are responsible for running the tasks of `queue`. Without a separate
    /// download pool that is the CPU queue for both, so that one pool does not get two sets of
    /// drivers competing for it.
    FFI_VortexTaskQueue driverQueueFor(FFI_VortexTaskQueue queue) const;

    /// Tells the scan to stop; safe to call from any thread.
    void cancelScan() noexcept;

    /// Cancels the scan and waits for the drivers, so that no task of this reader runs afterwards.
    void stopTasks();

    /// Stores the first failure, so that `read` rethrows it, and wakes `read` up. Also stops the
    /// scan, unless `cancel_scan` is false - which is needed where stopping it would re-enter this
    /// reader through the notification callback.
    void setBackgroundException(std::exception_ptr exception, bool cancel_scan = true) noexcept;

    std::unique_ptr<ArrowColumnToCHColumn> createConverter() const;

    /// Takes a converter out of the pool, creating one if the pool is empty, and returns it there
    /// afterwards. They are pooled because each caches dictionaries and cannot be used by two
    /// threads at once.
    std::unique_ptr<ArrowColumnToCHColumn> takeConverter();
    void returnConverter(std::unique_ptr<ArrowColumnToCHColumn> converter);

    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    std::unique_ptr<Vortex::VortexReadContext> read_context;
    FFI_VortexRuntime * runtime = nullptr;
    FFI_VortexReader * reader = nullptr;
    std::shared_ptr<arrow::Schema> file_schema;

    /// Cancellation can arrive on any thread, so the lifetime of the scan handle is serialized.
    std::mutex scan_mutex;
    FFI_VortexScan * scan TSA_GUARDED_BY(scan_mutex) = nullptr;
    /// Set before the first task exists and cleared after the last one has stopped, so the tasks
    /// can read it without holding a lock.
    std::shared_ptr<arrow::Schema> scan_schema;

    std::mutex delivery_mutex;
    std::condition_variable delivery_cv;
    /// Finished chunks waiting for `read`, keyed by the position of their split in the file.
    std::map<UInt64, DeliveredChunk> delivered TSA_GUARDED_BY(delivery_mutex);
    /// Which split `read` hands out next while `input_format_vortex_preserve_order` is on.
    UInt64 next_split_index TSA_GUARDED_BY(delivery_mutex) = 0;
    /// The scan reported that it reached the end. Stays false when it was cancelled instead.
    bool scan_finished TSA_GUARDED_BY(delivery_mutex) = false;
    /// Only the first failure is kept; `read` rethrows it.
    std::exception_ptr background_exception TSA_GUARDED_BY(delivery_mutex);

    std::mutex converters_mutex;
    std::vector<std::unique_ptr<ArrowColumnToCHColumn>> converters TSA_GUARDED_BY(converters_mutex);

    /// Atomic rather than guarded by `delivery_mutex`: the library reports new tasks from inside a
    /// running task, and that thread may already be holding the mutex.
    std::array<std::atomic<size_t>, NUM_QUEUES> running_drivers{};

    /// Every running driver holds a shared lock on it, and closing the reader waits for them all to
    /// let go. Replaced by a fresh one whenever a scan starts.
    std::shared_ptr<ShutdownHelper> tasks_shutdown;
    /// Set while the reader is being closed, so that no new driver is scheduled.
    std::atomic<bool> closing{false};

    /// For queries that touch no column of the file at all and need only its number of rows.
    UInt64 pending_rows_without_columns = 0;
    bool count_returned = false;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;
    size_t previous_approx_bytes_read = 0;

    const FormatSettings format_settings;
    FormatParserSharedResourcesPtr parser_shared_resources;
    FormatFilterInfoPtr format_filter_info;
    const bool is_remote_fs;
    const LoggerPtr log = getLogger("VortexBlockInputFormat");

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
    std::unique_ptr<Vortex::VortexReadContext> read_context;
    /// Reading a footer takes a few small reads, so the calling thread runs this runtime itself.
    FFI_VortexRuntime * runtime = nullptr;
    FFI_VortexReader * reader = nullptr;
    std::shared_ptr<arrow::Schema> file_schema;

    /// Never set: it only exists because the Arrow file wrapper takes a cancellation flag.
    std::atomic<int> is_stopped{0};
};

}

#endif
