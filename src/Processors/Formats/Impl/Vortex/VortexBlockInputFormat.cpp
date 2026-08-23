#include <Processors/Formats/Impl/Vortex/VortexBlockInputFormat.h>

#if USE_VORTEX

#include <Core/Defines.h>
#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadBuffer.h>
#include <IO/SharedThreadPools.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/Vortex/VortexFFIHelpers.h>
#include <Processors/Formats/Impl/Vortex/VortexScanPlanner.h>
#include <Processors/Port.h>
#include <base/scope_guard.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/result.h>

#include <algorithm>
#include <chrono>
#include <shared_mutex>

#include <vortex_ffi.h>

namespace ProfileEvents
{
extern const Event VortexScanSplits;
extern const Event VortexScanEmptySplits;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

using namespace Vortex;

/// `read` waits on a condition variable, but also wakes up this often to check that the scan is
/// still making progress. Finding it idle this many times in a row means it never will again.
static constexpr auto PROGRESS_CHECK_PERIOD = std::chrono::seconds(1);
static constexpr size_t IDLE_CHECKS_BEFORE_STUCK = 3;

/// The C entry points the library calls. An exception escaping one of them would unwind into Rust,
/// so everything they reach is `noexcept`.

extern "C" void vortexFFINotifyCallback(void * context, FFI_VortexTaskQueue queue);
extern "C" void vortexFFINotifyCallback(void * context, FFI_VortexTaskQueue queue)
{
    static_cast<VortexBlockInputFormat *>(context)->onNotify(queue);
}

extern "C" int32_t vortexFFIChunkCallback(void * context, ::ArrowArray * array, uint64_t split_index);
extern "C" int32_t vortexFFIChunkCallback(void * context, ::ArrowArray * array, uint64_t split_index)
{
    return static_cast<VortexBlockInputFormat *>(context)->onChunk(array, split_index);
}

extern "C" void vortexFFIFinishCallback(void * context, const char * error);
extern "C" void vortexFFIFinishCallback(void * context, const char * error)
{
    static_cast<VortexBlockInputFormat *>(context)->onScanFinish(error);
}

VortexBlockInputFormat::VortexBlockInputFormat(
    ReadBuffer & in_,
    SharedHeader header_,
    const FormatSettings & format_settings_,
    FormatParserSharedResourcesPtr parser_shared_resources_,
    FormatFilterInfoPtr format_filter_info_,
    bool is_remote_fs_)
    : IInputFormat(header_, &in_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
    , parser_shared_resources(std::move(parser_shared_resources_))
    , format_filter_info(std::move(format_filter_info_))
    , is_remote_fs(is_remote_fs_)
{
}

VortexBlockInputFormat::~VortexBlockInputFormat()
{
    closeReader();
}

bool VortexBlockInputFormat::hasSeparateIORunner() const
{
    return parser_shared_resources && !parser_shared_resources->io_runner.isDisabled();
}

ThreadPoolCallbackRunnerFast & VortexBlockInputFormat::runnerFor(FFI_VortexTaskQueue queue) const
{
    chassert(parser_shared_resources);
    if (queue == FFI_VortexTaskQueue::IO && hasSeparateIORunner())
        return parser_shared_resources->io_runner;
    return parser_shared_resources->parsing_runner;
}

FFI_VortexTaskQueue VortexBlockInputFormat::driverQueueFor(FFI_VortexTaskQueue queue) const
{
    return hasSeparateIORunner() ? queue : FFI_VortexTaskQueue::CPU;
}

size_t VortexBlockInputFormat::maxDrivers(FFI_VortexTaskQueue queue) const
{
    if (!parser_shared_resources)
        return 0;
    queue = driverQueueFor(queue);
    const auto & runner = runnerFor(queue);
    if (runner.getMode() != ThreadPoolCallbackRunnerFast::Mode::ThreadPool)
        return 0;
    /// Files read in parallel divide the pool between them, and the share grows back as they
    /// finish, so this is recomputed rather than cached.
    size_t threads = queue == FFI_VortexTaskQueue::IO ? parser_shared_resources->getIOThreadsPerReader()
                                                      : parser_shared_resources->getParsingThreadsPerReader();
    return std::max<size_t>(threads, 1);
}

void VortexBlockInputFormat::onNotify(FFI_VortexTaskQueue queue) noexcept
{
    if (closing.load(std::memory_order_acquire) || is_stopped)
        return;

    queue = driverQueueFor(queue);
    const size_t max_drivers = maxDrivers(queue);
    if (max_drivers == 0)
        return; /// There is no thread pool; `read` runs the tasks itself.

    /// The task was queued before this call, and a driver decrements the counter before it takes
    /// its last look at the queue. Both are sequentially consistent, so at least one of the two
    /// sides sees the other: either this call sees the driver leave, or the driver sees the task.
    size_t running = running_drivers[static_cast<size_t>(queue)].load(std::memory_order_seq_cst);
    while (running < max_drivers)
    {
        if (!running_drivers[static_cast<size_t>(queue)].compare_exchange_weak(running, running + 1, std::memory_order_relaxed))
            continue;

        try
        {
            runnerFor(queue)([this, queue, shutdown = tasks_shutdown] { driveQueue(queue, shutdown); });
        }
        catch (...)
        {
            /// A task the runner refused to accept will never run, so undo the increment.
            running_drivers[static_cast<size_t>(queue)].fetch_sub(1, std::memory_order_relaxed);
            /// Stopping the scan here would notify this reader back on this same thread, and a
            /// second refusal would then lock `scan_mutex` recursively. `read` rethrows anyway.
            setBackgroundException(std::current_exception(), /* cancel_scan */ false);
        }
        return;
    }
}

void VortexBlockInputFormat::driveQueue(FFI_VortexTaskQueue queue, std::shared_ptr<ShutdownHelper> shutdown_) noexcept
{
    /// After a long wait in the pool the reader may already have been destroyed.
    std::shared_lock shutdown_lock(*shutdown_, std::try_to_lock);
    if (!shutdown_lock.owns_lock())
    {
        /// `running_drivers` is left alone: nothing reads it after the shutdown.
        return;
    }

    /// With one shared pool the CPU drivers run the reads as well; otherwise a file that got no
    /// I/O drivers of its own would wait for bytes nobody is fetching.
    const bool drive_io = queue == FFI_VortexTaskQueue::IO || !hasSeparateIORunner();
    const bool drive_cpu = queue == FFI_VortexTaskQueue::CPU;

    /// A driver that runs both queues has to alternate between them: draining the decoding queue
    /// first would leave the next splits waiting for reads that nobody has started.
    while (true)
    {
        char * error = nullptr;
        int64_t tasks = 0;
        if (drive_cpu)
            tasks = vortex_ffi_runtime_run(runtime, FFI_VortexTaskQueue::CPU, /* max_tasks */ 16, &error);
        if (tasks >= 0 && drive_io)
        {
            int64_t io_tasks = vortex_ffi_runtime_run(runtime, FFI_VortexTaskQueue::IO, /* max_tasks */ 16, &error);
            tasks = io_tasks < 0 ? io_tasks : tasks + io_tasks;
        }
        if (tasks < 0)
        {
            setBackgroundException(makeVortexException(takeVortexError(error), read_context->getException()));
            break;
        }
        if (tasks == 0)
            break;
    }

    /// The other side of the handshake in `onNotify`: a task queued between the last run and this
    /// line is picked up either by the check below or by its own notification. Relaxing either side
    /// lets both read stale values, which does happen on AArch64.
    running_drivers[static_cast<size_t>(queue)].fetch_sub(1, std::memory_order_seq_cst);

    if ((drive_cpu && vortex_ffi_runtime_pending(runtime, FFI_VortexTaskQueue::CPU) > 0)
        || (drive_io && vortex_ffi_runtime_pending(runtime, FFI_VortexTaskQueue::IO) > 0))
        onNotify(queue);

    /// `read` watches the same counter to tell a scan that is working from one that is stuck, so it
    /// has to be woken when a driver leaves. The mutex is taken first, or the wake-up could fall
    /// into the gap between its check and its wait.
    {
        std::lock_guard lock(delivery_mutex);
    }
    delivery_cv.notify_all();
}

int32_t VortexBlockInputFormat::onChunk(::ArrowArray * array, UInt64 split_index) noexcept
{
    try
    {
        ProfileEvents::increment(ProfileEvents::VortexScanSplits);
        /// The library reports a fully filtered-out split as a null array.
        if (!array)
            ProfileEvents::increment(ProfileEvents::VortexScanEmptySplits);

        DeliveredChunk delivered_chunk;
        delivered_chunk.empty = array == nullptr;
        delivered_chunk.holds_permit = array != nullptr;

        if (array)
        {
            /// The array is owned by this callback; importing it passes that ownership to Arrow.
            auto batch = arrow::ImportRecordBatch(array, scan_schema);
            throwFromArrowStatusIfFailed(batch.status());

            ArrowColumnToCHColumn::checkRecordBatchValidityBitmaps(**batch);

            auto table = arrow::Table::FromRecordBatches({*batch});
            throwFromArrowStatusIfFailed(table.status());

            auto converter = takeConverter();
            SCOPE_EXIT({ returnConverter(std::move(converter)); });

            delivered_chunk.missing_values = BlockMissingValues(getPort().getHeader().columns());
            BlockMissingValues * missing_values_ptr
                = format_settings.defaults_for_omitted_fields ? &delivered_chunk.missing_values : nullptr;
            delivered_chunk.chunk = converter->arrowTableToCHChunk(*table, (*table)->num_rows(), nullptr, missing_values_ptr);
        }

        {
            std::lock_guard lock(delivery_mutex);
            if (!delivered.emplace(split_index, std::move(delivered_chunk)).second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Vortex scan delivered split {} twice", split_index);
        }
        delivery_cv.notify_all();
        return 0;
    }
    catch (...)
    {
        setBackgroundException(std::current_exception());
        return 1;
    }
}

void VortexBlockInputFormat::onScanFinish(const char * error) noexcept
{
    {
        std::lock_guard lock(delivery_mutex);
        if (error && !background_exception)
            background_exception = makeVortexException(error, read_context->getException());
        scan_finished = true;
    }
    delivery_cv.notify_all();
}

void VortexBlockInputFormat::setBackgroundException(std::exception_ptr exception, bool cancel_scan) noexcept
{
    {
        std::lock_guard lock(delivery_mutex);
        if (!background_exception)
            background_exception = std::move(exception);
    }
    delivery_cv.notify_all();
    if (cancel_scan)
        cancelScan();
}

void VortexBlockInputFormat::cancelScan() noexcept
{
    std::lock_guard lock(scan_mutex);
    if (scan)
        vortex_ffi_scan_cancel(scan);
}

void VortexBlockInputFormat::onCancel() noexcept
{
    is_stopped = 1;
    cancelScan();
    /// As above: the mutex keeps the wake-up out of the gap between the check and the wait.
    {
        std::lock_guard lock(delivery_mutex);
    }
    delivery_cv.notify_all();
}

void VortexBlockInputFormat::stopTasks()
{
    closing.store(true, std::memory_order_release);
    cancelScan();
    /// Cancelling drains the queues, which is what makes the running drivers stop; the ones still
    /// waiting in the pool find the shutdown and return without touching the reader.
    if (tasks_shutdown)
        tasks_shutdown->shutdown();
}

void VortexBlockInputFormat::closeReader()
{
    stopTasks();
    {
        std::lock_guard lock(scan_mutex);
        if (scan)
        {
            vortex_ffi_scan_free(scan);
            scan = nullptr;
        }
        scan_schema.reset();
    }
    {
        std::lock_guard lock(delivery_mutex);
        delivered.clear();
        next_split_index = 0;
        scan_finished = false;
        background_exception = nullptr;
    }
    {
        std::lock_guard lock(converters_mutex);
        converters.clear();
    }
    for (auto & running : running_drivers)
        running.store(0, std::memory_order_relaxed);
    /// `tasks_shutdown` is deliberately not reset: a notification running right now may be copying
    /// it into a task it is scheduling, and that task will find the shutdown and return. The next
    /// scan gets a fresh one from `prepareReader`.
    if (reader)
    {
        vortex_ffi_reader_free(reader);
        reader = nullptr;
    }
    if (runtime)
    {
        vortex_ffi_runtime_free(runtime);
        runtime = nullptr;
    }
    file_schema.reset();
    arrow_file.reset();
    read_context.reset();
    closing.store(false, std::memory_order_release);
}

std::unique_ptr<ArrowColumnToCHColumn> VortexBlockInputFormat::takeConverter()
{
    {
        std::lock_guard lock(converters_mutex);
        if (!converters.empty())
        {
            auto converter = std::move(converters.back());
            converters.pop_back();
            return converter;
        }
    }
    return createConverter();
}

void VortexBlockInputFormat::returnConverter(std::unique_ptr<ArrowColumnToCHColumn> converter)
{
    std::lock_guard lock(converters_mutex);
    converters.push_back(std::move(converter));
}

std::unique_ptr<ArrowColumnToCHColumn> VortexBlockInputFormat::createConverter() const
{
    return std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "Vortex",
        format_settings,
        std::nullopt,
        std::nullopt,
        /* allow_missing_columns */ true,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.parquet.allow_geoparquet_parser);
}

void VortexBlockInputFormat::prepareReader()
{
    if (parser_shared_resources)
    {
        parser_shared_resources->initOnce(
            [&]
            {
                /// A single parsing thread means no thread pool at all: the tasks then run inside
                /// `read`, on the thread of the query pipeline.
                if (parser_shared_resources->max_parsing_threads <= 1)
                    parser_shared_resources->parsing_runner.initManual();
                else
                    parser_shared_resources->parsing_runner.initThreadPool(
                        getFormatParsingThreadPool().get(),
                        parser_shared_resources->max_parsing_threads,
                        ThreadName::VORTEX_DECODER,
                        CurrentThread::getGroup());

                if (parser_shared_resources->max_parsing_threads > 1 && parser_shared_resources->max_io_threads > 0)
                    parser_shared_resources->io_runner.initThreadPool(
                        getFormatParsingThreadPool().get(),
                        parser_shared_resources->max_io_threads,
                        ThreadName::VORTEX_READER,
                        CurrentThread::getGroup());
            });
    }

    tasks_shutdown = std::make_shared<ShutdownHelper>();
    /// Everything that can put a task on this runtime is destroyed before the runtime itself.
    runtime = vortex_ffi_runtime_new(this, parser_shared_resources ? vortexFFINotifyCallback : nullptr);

    const size_t cpu_threads = std::max<size_t>(maxDrivers(FFI_VortexTaskQueue::CPU), 1);
    const size_t io_threads = std::max<size_t>(maxDrivers(FFI_VortexTaskQueue::IO), 1);

    read_context = std::make_unique<VortexReadContext>();
    reader = openVortexReader(runtime, *in, format_settings, is_stopped, arrow_file, *read_context, file_schema, io_threads, is_remote_fs);
    if (!reader)
        return;

    if (need_only_count)
        return;

    const VortexScanPlan plan = planVortexScan(getPort().getHeader(), *file_schema, format_filter_info.get(), format_settings, log);

    if (plan.column_names.empty())
    {
        pending_rows_without_columns = vortex_ffi_reader_row_count(reader);
        return;
    }

    /// Points into `plan.column_names`, which must not be modified from here on.
    std::vector<const char *> column_name_pointers;
    column_name_pointers.reserve(plan.column_names.size());
    for (const auto & name : plan.column_names)
        column_name_pointers.push_back(name.c_str());

    FFI_VortexScanOptions options{};
    options.columns = column_name_pointers.data();
    options.num_columns = column_name_pointers.size();
    options.filter = plan.filter.get();
    /// Splits allowed in flight at once: being read, decoded, or queued for `read`. Two per
    /// decoding thread leaves each thread something to decode while the next split is still being
    /// read. The limit is low because it counts splits and not bytes, and the decoded chunks of a
    /// wide projection are large.
    options.max_splits_in_flight = static_cast<uint32_t>(std::clamp<size_t>(2 * cpu_threads, 4, 64));

    FFI_VortexScanCallbacks callbacks{};
    callbacks.context = this;
    callbacks.on_chunk = vortexFFIChunkCallback;
    callbacks.on_finish = vortexFFIFinishCallback;

    /// A chunk can reach `onChunk` before `vortex_ffi_scan_create` has returned, so the schema it
    /// will be imported with has to exist beforehand: the file schema projected to the requested
    /// columns, the same way the library projects it.
    arrow::FieldVector scan_fields;
    scan_fields.reserve(plan.column_names.size());
    for (const auto & name : plan.column_names)
        scan_fields.push_back(file_schema->GetFieldByName(name));
    scan_schema = arrow::schema(std::move(scan_fields));

    char * error = nullptr;
    auto * new_scan = vortex_ffi_scan_create(reader, &options, &callbacks, &error);
    if (!new_scan)
    {
        /// The scan is started last, after everything that can fail, so nothing was delivered yet;
        /// this only makes sure no driver of this reader is running when the exception propagates.
        stopTasks();
        String message = takeVortexError(error);
        /// The translator only builds filters the scan can bind, so a failure here is a bug in it;
        /// the expression is the piece of context that makes such a report actionable.
        if (plan.filter)
            message += fmt::format(" (the scan was created with the pushed-down filter: {})", vortexExpressionToString(plan.filter.get()));
        std::rethrow_exception(makeVortexException(message, read_context->getException()));
    }

    /// Debug-only cross-check that the library projects the schema the same way.
    ArrowSchema c_schema{};
    if (vortex_ffi_scan_schema(new_scan, &c_schema, &error) == 0)
    {
        auto library_schema = arrow::ImportSchema(&c_schema);
        chassert(library_schema.ok() && (*library_schema)->Equals(*scan_schema, /* check_metadata */ false));
    }
    else if (error)
    {
        vortex_ffi_free_string(error);
    }

    std::lock_guard scan_lock(scan_mutex);
    scan = new_scan;
}

Chunk VortexBlockInputFormat::readWithoutColumns()
{
    if (!pending_rows_without_columns)
        return {};

    size_t num_rows = std::min<UInt64>(pending_rows_without_columns, DEFAULT_BLOCK_SIZE);
    pending_rows_without_columns -= num_rows;

    auto batch = arrow::RecordBatch::Make(arrow::schema(arrow::FieldVector{}), num_rows, arrow::ArrayVector{});
    auto table = arrow::Table::FromRecordBatches({batch});
    throwFromArrowStatusIfFailed(table.status());

    auto converter = takeConverter();
    SCOPE_EXIT({ returnConverter(std::move(converter)); });

    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    return converter->arrowTableToCHChunk(*table, num_rows, nullptr, block_missing_values_ptr);
}

Chunk VortexBlockInputFormat::read()
{
    if (!reader && !count_returned)
        prepareReader();

    if (is_stopped)
        return {};

    if (need_only_count)
    {
        if (count_returned)
            return {};
        count_returned = true;
        return getChunkForCount(vortex_ffi_reader_row_count(reader));
    }

    block_missing_values.clear();

    if (!scan)
        return readWithoutColumns();

    /// Without a thread pool the tasks have to be run here, between the waits.
    const bool run_tasks_inline
        = !parser_shared_resources || parser_shared_resources->parsing_runner.getMode() != ThreadPoolCallbackRunnerFast::Mode::ThreadPool;

    /// Consecutive waits that found the scan making no progress.
    size_t idle_checks = 0;

    std::unique_lock lock(delivery_mutex);
    while (true)
    {
        if (background_exception)
            std::rethrow_exception(background_exception);
        if (is_stopped)
            return {};

        if (!delivered.empty())
        {
            auto it = delivered.begin();
            if (!format_settings.vortex.preserve_order || it->first == next_split_index)
            {
                const UInt64 split_index = it->first;
                DeliveredChunk delivered_chunk = std::move(it->second);
                delivered.erase(it);
                next_split_index = std::max(next_split_index, split_index) + 1;
                lock.unlock();

                if (delivered_chunk.holds_permit)
                {
                    /// Frees a slot, so the scan may read one split further ahead.
                    std::lock_guard scan_lock(scan_mutex);
                    if (scan)
                        vortex_ffi_scan_release(scan, 1);
                }

                if (delivered_chunk.empty)
                {
                    /// The split matched no rows; it only moves the file order forward.
                    lock.lock();
                    continue;
                }

                block_missing_values = std::move(delivered_chunk.missing_values);
                /// The bytes read since the previous chunk was returned are attributed to this one.
                size_t bytes_read = read_context->bytes_read.load(std::memory_order_relaxed);
                approx_bytes_read_for_chunk = bytes_read - previous_approx_bytes_read;
                previous_approx_bytes_read = bytes_read;
                return std::move(delivered_chunk.chunk);
            }

            /// The split due next in file order has not arrived yet. Splits are delivered in the
            /// order they finish, so that is normal - unless the scan is already over.
            if (scan_finished)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Vortex reader lost split {} of the scan (the next delivered one is {})",
                    next_split_index,
                    it->first);
        }
        else if (scan_finished)
        {
            return {};
        }

        if (run_tasks_inline)
        {
            lock.unlock();
            char * error = nullptr;
            int64_t cpu_tasks = vortex_ffi_runtime_run(runtime, FFI_VortexTaskQueue::CPU, /* max_tasks */ 8, &error);
            int64_t io_tasks = cpu_tasks < 0 ? 0 : vortex_ffi_runtime_run(runtime, FFI_VortexTaskQueue::IO, /* max_tasks */ 8, &error);
            if (cpu_tasks < 0 || io_tasks < 0)
                throwVortexError(error, read_context->getException());
            lock.lock();
            if (cpu_tasks == 0 && io_tasks == 0)
            {
                /// Nothing ran and nothing can run: no other thread will make this call progress.
                if (!scan_finished && !background_exception && !is_stopped)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Deadlock in the Vortex reader (single-threaded)");
            }
            continue;
        }

        /// The timeout is a safety net: the protocol between the notifications and the drivers
        /// should never leave a runnable task without a driver, but a bug there has to cost a stall
        /// rather than a query that never returns.
        if (delivery_cv.wait_for(lock, PROGRESS_CHECK_PERIOD) != std::cv_status::timeout)
            continue;

        const bool idle = running_drivers[static_cast<size_t>(FFI_VortexTaskQueue::CPU)].load(std::memory_order_relaxed) == 0
            && running_drivers[static_cast<size_t>(FFI_VortexTaskQueue::IO)].load(std::memory_order_relaxed) == 0;
        if (!idle || scan_finished || background_exception)
        {
            idle_checks = 0;
            continue;
        }

        lock.unlock();
        const bool has_tasks = vortex_ffi_runtime_pending(runtime, FFI_VortexTaskQueue::CPU) > 0
            || vortex_ffi_runtime_pending(runtime, FFI_VortexTaskQueue::IO) > 0;
        if (has_tasks)
        {
            /// Tasks are queued and nothing is running them, which should not be reachable.
            /// Schedule the drivers again rather than hang.
            onNotify(FFI_VortexTaskQueue::CPU);
            onNotify(FFI_VortexTaskQueue::IO);
            idle_checks = 0;
        }
        else
        {
            /// Nothing queued and nothing running. Counted rather than reported at once, so that a
            /// task on its way into the queue is not mistaken for a dead scan.
            ++idle_checks;
        }
        lock.lock();

        const bool nothing_deliverable
            = delivered.empty() || (format_settings.vortex.preserve_order && delivered.begin()->first != next_split_index);
        if (idle_checks >= IDLE_CHECKS_BEFORE_STUCK && nothing_deliverable && !scan_finished && !background_exception && !is_stopped)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Deadlock in the Vortex reader (thread pool)");
    }
}

void VortexBlockInputFormat::resetParser()
{
    /// The scan tasks read through the buffer the base class is about to drain.
    closeReader();
    IInputFormat::resetParser();

    pending_rows_without_columns = 0;
    count_returned = false;
    block_missing_values.clear();
    approx_bytes_read_for_chunk = 0;
    previous_approx_bytes_read = 0;
}

void VortexBlockInputFormat::resetReadBuffer()
{
    /// Same here: the buffer the scan tasks read through is about to be released.
    closeReader();
    IInputFormat::resetReadBuffer();
}

const BlockMissingValues * VortexBlockInputFormat::getMissingValues() const
{
    return &block_missing_values;
}

VortexSchemaReader::VortexSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_)
    , format_settings(format_settings_)
{
}

VortexSchemaReader::~VortexSchemaReader()
{
    if (reader)
        vortex_ffi_reader_free(reader);
    if (runtime)
        vortex_ffi_runtime_free(runtime);
}

void VortexSchemaReader::initializeIfNeeded()
{
    if (reader)
        return;

    /// No notification callback: this thread runs the tasks of the footer read itself.
    if (!runtime)
        runtime = vortex_ffi_runtime_new(nullptr, nullptr);
    read_context = std::make_unique<VortexReadContext>();
    reader = openVortexReader(
        runtime,
        in,
        format_settings,
        is_stopped,
        arrow_file,
        *read_context,
        file_schema,
        /* io_threads */ 1,
        /* is_remote_fs */ false);
}

NamesAndTypesList VortexSchemaReader::readSchema()
{
    initializeIfNeeded();

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *file_schema,
        nullptr,
        "Vortex",
        format_settings,
        /* skip_columns_with_unsupported_types */ false,
        /* allow_arrow_null_type */ true,
        format_settings.schema_inference_make_columns_nullable != 0,
        /* case_insensitive_matching */ false,
        format_settings.parquet.allow_geoparquet_parser);
    if (format_settings.schema_inference_make_columns_nullable == 1)
        return getNamesAndRecursivelyNullableTypes(header, format_settings);
    return header.getNamesAndTypesList();
}

std::optional<size_t> VortexSchemaReader::readNumberOrRows()
{
    initializeIfNeeded();
    return vortex_ffi_reader_row_count(reader);
}

void registerInputFormatVortex(FormatFactory & factory);
void registerInputFormatVortex(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "Vortex",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & /* read_settings */,
           bool is_remote_fs,
           FormatParserSharedResourcesPtr parser_shared_resources,
           FormatFilterInfoPtr format_filter_info) -> InputFormatPtr
        {
            return std::make_shared<VortexBlockInputFormat>(
                buf,
                std::make_shared<const Block>(sample),
                settings,
                std::move(parser_shared_resources),
                std::move(format_filter_info),
                is_remote_fs);
        });
    factory.markFormatSupportsSubsetOfColumns("Vortex");

    factory.setDocumentation("Vortex", Documentation{.description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Vortex](https://vortex.dev/) is an extensible columnar file format for compressed Apache Arrow-compatible data,
designed for fast scans and random access. ClickHouse supports reading and writing Vortex files.

## Data types matching {#data-types-matching-vortex}

The table below shows the Vortex data types and the corresponding ClickHouse [data types](/reference/data-types/index)
in `INSERT` and `SELECT` queries.

| Vortex data type (`INSERT`)         | ClickHouse data type                                          | Vortex data type (`SELECT`) |
|-------------------------------------|---------------------------------------------------------------|-----------------------------|
| `Bool`                              | [Bool](/reference/data-types/boolean)                         | `Bool`                      |
| `I8`, `U8`                          | [Int8/UInt8](/reference/data-types/int-uint)                  | `I8`, `U8`                  |
| `I16`, `U16`                        | [Int16/UInt16](/reference/data-types/int-uint)                | `I16`, `U16`                |
| `I32`, `U32`                        | [Int32/UInt32](/reference/data-types/int-uint)                | `I32`, `U32`                |
| `I64`, `U64`                        | [Int64/UInt64](/reference/data-types/int-uint)                | `I64`, `U64`                |
| `F32`                               | [Float32](/reference/data-types/float)                        | `F32`                       |
| `F64`                               | [Float64](/reference/data-types/float)                        | `F64`                       |
| `Utf8`, `Binary`                    | [String](/reference/data-types/string)                        | `Binary`                    |
| `Binary`                            | [FixedString](/reference/data-types/fixedstring)              | `Binary`                    |
| `Decimal`                           | [Decimal](/reference/data-types/decimal)                      | `Decimal`                   |
| `vortex.date`                       | [Date32](/reference/data-types/date32)                        | `vortex.date`               |
| `vortex.timestamp`                  | [DateTime](/reference/data-types/datetime)/[DateTime64](/reference/data-types/datetime64) | `vortex.timestamp` |
| `vortex.time`                       | [Time64](/reference/data-types/time64)                        | `vortex.time`               |
| `List`                              | [Array](/reference/data-types/array)                          | `List`                      |
| `Struct`                            | [Tuple](/reference/data-types/tuple)                          | `Struct`                    |
| `Null`                              | [Nullable(Nothing)](/reference/data-types/special-data-types/nothing) | `Null`             |

Other types are not supported. In particular, [Map](/reference/data-types/map),
[Int128/UInt128/Int256/UInt256](/reference/data-types/int-uint), [IPv6](/reference/data-types/ipv6)
and [Interval](/reference/data-types/special-data-types/interval) columns cannot be written to Vortex files.
[String](/reference/data-types/string) columns are written as `Binary` because ClickHouse strings are
arbitrary byte sequences, while Vortex requires `Utf8` values to be valid UTF-8. Vortex has no
fixed-size binary type, so [FixedString](/reference/data-types/fixedstring) is also written as `Binary`;
schema inference reads it back as [String](/reference/data-types/string).
[LowCardinality](/reference/data-types/lowcardinality) columns are written as their underlying type
(Vortex chooses dictionary and other encodings adaptively by itself).
[DateTime](/reference/data-types/datetime) columns are written as `vortex.timestamp` with second precision,
so they are read back as [DateTime64](/reference/data-types/datetime64) with scale 0.
[IPv4](/reference/data-types/ipv4) columns are written as `U32` because Vortex has no type for IP addresses,
so schema inference reads them back as [UInt32](/reference/data-types/int-uint). Specify the type explicitly
to read such a column back as `IPv4`: `SELECT * FROM file('data.vortex', Vortex, 'ip IPv4')`.

The data types of ClickHouse table columns do not have to match the corresponding Vortex data fields.
When inserting data, ClickHouse interprets data types according to the table above and then
[casts](/reference/functions/regular-functions/type-conversion-functions#CAST) the data to the data type set for the
ClickHouse table column.

## Example usage {#example-usage}

You can select data from a Vortex file:

```sql
SELECT * FROM file('data.vortex');
```

And write data to a Vortex file:

```sql
SELECT * FROM numbers(3) INTO OUTFILE 'numbers.vortex' FORMAT Vortex;
```

## Format settings {#format-settings}

| Setting                                  | Description                                                          | Default |
|------------------------------------------|----------------------------------------------------------------------|---------|
| `input_format_vortex_filter_push_down`   | Push translatable parts of the `WHERE` condition down into the scan, so that a selective query reads and decodes less data. Only the translated part of the condition is enforced inside the scan: a condition that is only partly translatable is widened to one that keeps at least every matching row, so rows that do not match can still be read and decoded, and ClickHouse reapplies the full filter afterwards. Pushdown currently supports top-level integer, floating-point, string/binary, `Bool`, `Date`, `Date32` and `DateTime64` columns. | `1` |
| `input_format_vortex_preserve_order`     | Return the rows in file order. By default the row splits of a file are decoded in parallel and returned as soon as they are ready, so the row order is not guaranteed; with the setting a slow split holds back the ones after it. | `0` |
| `max_parsing_threads`                    | The number of threads that decode Vortex files (shared by the files read in parallel by one query). `1` disables the thread pool: the file is then read inside `read`, on the thread of the query pipeline. | number of cores |
| `max_download_threads`                   | The number of threads that read the file. `0` makes the reads share the decoding threads. | `4` |

As in other columnar formats, only the columns used by the query are read from the file, and
columns missing in the file are filled with default values.

## Performance {#performance}

A file is read in parallel. The scan splits it into row ranges (aligned to the chunk boundaries of
the requested columns, at most 100 000 rows each) that are read, filtered and decoded concurrently:
the decoding runs on up to `max_parsing_threads` threads of the same pool the `Parquet` reader uses,
the reads on up to `max_download_threads` threads, and the conversion of a decoded chunk to
ClickHouse columns happens on the thread that decoded it. Chunks are returned as soon as they are
ready, so the row order is not guaranteed unless `input_format_vortex_preserve_order` is set. The
reads of segments that are close in the file are merged into one request (up to 4 MiB for local
files and 16 MiB for remote storage). Filter pushdown reduces the amount of data read and decoded
by selective queries.
)DOCS_MD"});
}

void registerVortexSchemaReader(FormatFactory & factory);
void registerVortexSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Vortex",
        [](ReadBuffer & buf, const FormatSettings & settings) -> SchemaReaderPtr
        { return std::make_shared<VortexSchemaReader>(buf, settings); });

    /// These settings determine the inferred types, so a cached schema is only valid for the same
    /// values of them.
    factory.registerAdditionalInfoForSchemaCacheGetter(
        "Vortex",
        [](const FormatSettings & settings)
        {
            return fmt::format(
                "schema_inference_make_columns_nullable={};schema_inference_make_json_columns_nullable={};"
                "schema_inference_allow_nullable_tuple_type={};"
                "allow_geoparquet_parser={}",
                settings.schema_inference_make_columns_nullable,
                settings.schema_inference_make_json_columns_nullable,
                settings.schema_inference_allow_nullable_tuple_type,
                settings.parquet.allow_geoparquet_parser);
        });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatVortex(FormatFactory &);
void registerInputFormatVortex(FormatFactory &)
{
}

void registerVortexSchemaReader(FormatFactory &);
void registerVortexSchemaReader(FormatFactory &)
{
}
}

#endif
