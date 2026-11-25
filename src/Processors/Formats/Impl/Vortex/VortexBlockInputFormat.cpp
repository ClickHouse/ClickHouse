#include <Processors/Formats/Impl/Vortex/VortexBlockInputFormat.h>

#if USE_VORTEX

#include <Core/Defines.h>
#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadBuffer.h>
#include <IO/SharedThreadPools.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowCData.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCSchemaReader.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Columns/ColumnsNumber.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Processors/Formats/Impl/Vortex/VortexFFIHelpers.h>
#include <Processors/Formats/Impl/Vortex/VortexScanPlanner.h>
#include <Processors/Port.h>
#include <base/scope_guard.h>
#include <base/unit.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>


#include <algorithm>
#include <chrono>
#include <shared_mutex>

#include <vortex_ffi.h>

namespace ProfileEvents
{
extern const Event VortexScanSplits;
extern const Event VortexScanEmptySplits;
extern const Event VortexConvertMicroseconds;
extern const Event VortexReadWaitMicroseconds;
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

/// `ChunkInfoRowNumbers` stores the row numbers of a chunk as an offset and a filter over every row
/// from there to the last one, selected or not, so it costs a byte per row of the file it spans.
/// Rows further apart than this go to different chunks: a selective read must not allocate memory
/// in proportion to the gaps between the rows it keeps. A run of consecutive rows needs no filter,
/// so it is never cut.
static constexpr size_t MAX_ROW_NUMBERS_FILTER_SIZE = 1_MiB;

/// How many of the first `size` of the sorted, unique `row_numbers` can share one chunk.
static size_t rowNumbersPieceLength(const UInt64 * row_numbers, size_t size)
{
    if (size == 0)
        return 0;
    size_t length = 1;
    while (length < size)
    {
        const UInt64 span = row_numbers[length] - row_numbers[0] + 1;
        if (span > MAX_ROW_NUMBERS_FILTER_SIZE && span != length + 1)
            break;
        ++length;
    }
    return length;
}

static std::shared_ptr<ChunkInfoRowNumbers> makeRowNumbers(const UInt64 * row_numbers, size_t size)
{
    if (size == 0)
        return std::make_shared<ChunkInfoRowNumbers>(0);

    const UInt64 first = row_numbers[0];
    const UInt64 last = row_numbers[size - 1];
    if (last - first + 1 == size)
        return std::make_shared<ChunkInfoRowNumbers>(first);

    chassert(last - first + 1 <= MAX_ROW_NUMBERS_FILTER_SIZE);
    auto info = std::make_shared<ChunkInfoRowNumbers>(first, IColumnFilter(last - first + 1, 0));
    for (size_t i = 0; i < size; ++i)
        (*info->applied_filter)[row_numbers[i] - first] = 1;
    return info;
}

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
    /// Without a parsing pool `read` runs every task inline, the reads included. The download pool
    /// may still exist, set up by a `Parquet` reader that initialized the shared resources first,
    /// but reads scheduled on it would be invisible to `read`, which would then take a read still
    /// in flight for a stuck scan.
    return parser_shared_resources && !parser_shared_resources->io_runner.isDisabled()
        && parser_shared_resources->parsing_runner.getMode() == ThreadPoolCallbackRunnerFast::Mode::ThreadPool;
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
            runnerFor(queue)([this, queue, shutdown = tasks_shutdown]
            {
                /// After a long wait in the pool the reader may already have been destroyed, so
                /// `this` is not touched until the shutdown is ruled out. The lock is held for the
                /// whole run, which is what `stopTasks` waits for.
                std::shared_lock shutdown_lock(*shutdown, std::try_to_lock);
                if (!shutdown_lock.owns_lock())
                {
                    /// `running_drivers` is left alone: nothing reads it after the shutdown.
                    return;
                }
                driveQueue(queue);
            });
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

void VortexBlockInputFormat::driveQueue(FFI_VortexTaskQueue queue) noexcept
{
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
            Stopwatch convert_watch;
            SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::VortexConvertMicroseconds, convert_watch.elapsedMicroseconds()); });

            /// The array is borrowed for the duration of this callback; importing it moves the
            /// data out and marks the struct released, which is how it is handed back.
            const size_t num_rows = static_cast<size_t>(array->length);
            auto decoded = ArrowIPC::importRecordBatchFromC(
                *array, *scan_schema, format_settings, &requested_types, &shadowed_scan_fields);

            ColumnPtr row_index;
            if (row_index_column)
            {
                row_index = decoded.front().column->convertToFullColumnIfConst();
                decoded.erase(decoded.begin());
            }

            /// One piece unless the row numbers are too far apart to share a chunk.
            size_t begin = 0;
            do
            {
                const size_t length = row_index
                    ? rowNumbersPieceLength(assert_cast<const ColumnUInt64 &>(*row_index).getData().data() + begin, num_rows - begin)
                    : num_rows - begin;

                /// The decoder returns constants for `Null` fields and bufferless structs, while `buildChunk`
                /// expects full columns, as the `Arrow` input format hands them over.
                ArrowIPC::RecordBatchDecoder::DecodedColumns piece_columns = decoded;
                for (auto & column : piece_columns)
                {
                    if (length != num_rows)
                        column.column = column.column->cut(begin, length);
                    column.column = column.column->convertToFullColumnIfConst();
                }

                DeliveredPiece & piece = delivered_chunk.pieces.emplace_back();
                piece.missing_values = BlockMissingValues(getPort().getHeader().columns());
                BlockMissingValues * missing_values_ptr = format_settings.defaults_for_omitted_fields ? &piece.missing_values : nullptr;
                piece.chunk = ArrowIPC::buildChunk(
                    getPort().getHeader(), piece_columns, length, format_settings, /* allow_missing_columns */ true, geo_columns, missing_values_ptr,
                    requested_types, visibility_only_structs);
                if (row_index)
                    piece.row_numbers = row_index->cut(begin, length);

                begin += length;
            } while (begin < num_rows);
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
        /// Returning non-zero is what stops the scan here, and it is reported back through
        /// `onScanFinish`. Cancelling instead would suppress that report, and the first chunks can
        /// arrive before `vortex_ffi_scan_create` has handed the scan over, so there is not even a
        /// handle to cancel with yet.
        setBackgroundException(std::current_exception(), /* cancel_scan */ false);
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
        visibility_only_structs.clear();
        shadowed_scan_fields.clear();
    }
    {
        std::lock_guard lock(delivery_mutex);
        delivered.clear();
        next_split_index = 0;
        scan_finished = false;
        background_exception = nullptr;
    }
    pending_pieces.clear();
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
    file.reset();
    read_context.reset();
    closing.store(false, std::memory_order_release);
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
    reader = openVortexReader(runtime, *in, format_settings, is_stopped, file, *read_context, file_schema, io_threads, is_remote_fs);
    if (!reader)
        return;

    if (format_settings.parquet.allow_geoparquet_parser)
    {
        auto geo_it = file_schema->custom_metadata.find("geo");
        geo_columns = parseGeoMetadataEncoding(geo_it != file_schema->custom_metadata.end() ? &geo_it->second : nullptr);
    }
    const bool case_insensitive = format_settings.arrow.case_insensitive_column_matching;
    ArrowIPC::SharedRequestedHeaderTypes shared_requested_header_types;
    ArrowIPC::collectRequestedTypes(getPort().getHeader(), case_insensitive, requested_types, shared_requested_header_types);

    const VortexScanPlan plan = planVortexScan(getPort().getHeader(), *file_schema, format_filter_info.get(), format_settings, log);

    if (need_only_count || plan.column_names.empty())
    {
        pending_rows_without_columns = plan.rows_to_read ? plan.rows_to_read->size() : vortex_ffi_reader_row_count(reader);
        /// A scanless read still owes its consumer the row numbers it asked for: `_row_number` and
        /// lazy materialization are not satisfied by a bare row count. `plan.rows_to_read` points
        /// into `format_filter_info`, which this format keeps alive.
        rows_without_columns_selection = plan.rows_to_read;
        row_index_column = format_filter_info && format_filter_info->need_row_numbers;
        return;
    }

    /// Points into `plan.column_names`, which must not be modified from here on.
    VectorWithMemoryTracking<const char *> column_name_pointers;
    column_name_pointers.reserve(plan.column_names.size());
    for (const auto & name : plan.column_names)
        column_name_pointers.push_back(name.c_str());

    FFI_VortexScanOptions options{};

    preserve_order = format_settings.vortex.preserve_order || plan.rows_to_read;
    /// A split the filter emptied only tells us that the file order moved on, and on a selective
    /// scan nearly every split is empty - so ask for them only when the order is what we restore.
    options.report_empty_splits = preserve_order;

    if (plan.rows_to_read)
    {
        options.row_selection_begin = plan.rows_to_read->begin();
        options.row_selection_len = plan.rows_to_read->size();
    }

    options.columns = column_name_pointers.data();
    options.num_columns = column_name_pointers.size();
    options.filter = plan.filter.get();

    row_index_column = format_filter_info && format_filter_info->need_row_numbers;
    options.row_index_column = row_index_column;

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
    auto new_scan_schema = std::make_shared<ArrowIPC::ArrowSchema>();
    new_scan_schema->fields.reserve(plan.column_names.size() + 1);
    if (row_index_column)
    {
        /// The library names it by this rule too, so that it cannot collide with a column of the file.
        String row_index_name = "_row_index";
        while (std::ranges::find(plan.column_names, row_index_name) != plan.column_names.end())
            row_index_name.insert(0, "_");
        ArrowIPC::ArrowField row_index_field;
        row_index_field.name = row_index_name;
        row_index_field.type.kind = ArrowIPC::TypeKind::Int;
        row_index_field.type.bit_width = 64;
        row_index_field.type.is_signed = false;
        new_scan_schema->fields.push_back(std::move(row_index_field));
    }
    for (const auto & name : plan.column_names)
    {
        auto it = std::ranges::find_if(file_schema->fields, [&](const auto & field) { return field.name == name; });
        chassert(it != file_schema->fields.end());
        new_scan_schema->fields.push_back(*it);
    }
    scan_schema = std::move(new_scan_schema);
    /// With case-insensitive matching, header columns of different types can share a name. They are all taken
    /// from the one decoded field, which is then decoded with the requirements of all of them, as in `Arrow`.
    ArrowIPC::resolveSharedRequestedTypes(
        *scan_schema, /*keep_top_level_fields=*/nullptr, case_insensitive, shared_requested_header_types, requested_types);
    {
        ArrowIPC::DictionaryRegistry registry;
        const ArrowIPC::RecordBatchDecoder decoder(*scan_schema, format_settings, registry);
        /// Of the fields with the same normalized name, the columns are taken from the last one (see `buildChunk`),
        /// which is the one the shared requested types are resolved against. The other ones are not decoded, as in
        /// `Arrow`: the requested types could not describe them. The row numbers are taken by position.
        shadowed_scan_fields = decoder.shadowedTopLevelFields();
        if (row_index_column)
            shadowed_scan_fields.front() = 0;
        /// A shadowed field is in the scan schema, but it is not decoded, so its structs are not looked up.
        visibility_only_structs = decoder.collectVisibilityOnlyStructs(/*keep_top_level_fields=*/nullptr, &requested_types);
    }

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

    /// The scan is owned from here on: what follows can throw (the import of the schema allocates),
    /// and `closeReader` cancels and frees only the scan it finds in the member.
    {
        std::lock_guard scan_lock(scan_mutex);
        scan = new_scan;
    }

    /// Cross-check that the library projects the schema the same way (the comparison itself is
    /// done in debug and sanitizer builds only).
    ::ArrowSchema c_schema{};
    if (vortex_ffi_scan_schema(new_scan, &c_schema, &error) == 0)
    {
        SCOPE_EXIT({
            if (c_schema.release)
                c_schema.release(&c_schema);
        });
        [[maybe_unused]] const auto library_schema = ArrowIPC::importSchemaFromC(c_schema);
        chassert(library_schema.fields.size() == scan_schema->fields.size());
        for ([[maybe_unused]] size_t i = 0; i < library_schema.fields.size(); ++i)
            chassert(library_schema.fields[i].name == scan_schema->fields[i].name
                && library_schema.fields[i].type.kind == scan_schema->fields[i].type.kind);
    }
    else if (error)
    {
        vortex_ffi_free_string(error);
    }
}

std::shared_ptr<ChunkInfoRowNumbers> VortexBlockInputFormat::rowNumbersWithoutColumns(UInt64 first_delivered, size_t num_rows) const
{
    /// No scan means no filter and no reordering: without a row selection the rows are the whole
    /// file in its own order, so their numbers are consecutive.
    if (!rows_without_columns_selection)
        return std::make_shared<ChunkInfoRowNumbers>(first_delivered);

    if (num_rows == 0)
        return std::make_shared<ChunkInfoRowNumbers>(0);

    return makeRowNumbers(rows_without_columns_selection->data() + first_delivered, num_rows);
}

Chunk VortexBlockInputFormat::takePendingPiece()
{
    DeliveredPiece piece = std::move(pending_pieces.front());
    pending_pieces.pop_front();
    block_missing_values = std::move(piece.missing_values);
    if (piece.row_numbers)
    {
        const auto & row_numbers = assert_cast<const ColumnUInt64 &>(*piece.row_numbers).getData();
        piece.chunk.getChunkInfos().add(makeRowNumbers(row_numbers.data(), row_numbers.size()));
    }
    return std::move(piece.chunk);
}

Chunk VortexBlockInputFormat::readWithoutColumns()
{
    if (!pending_rows_without_columns)
        return {};

    size_t num_rows = std::min<UInt64>(pending_rows_without_columns, DEFAULT_BLOCK_SIZE);
    /// Selected rows that are too far apart to share a chunk go to the next one.
    if (row_index_column && rows_without_columns_selection)
        num_rows = rowNumbersPieceLength(rows_without_columns_selection->data() + rows_without_columns_delivered, num_rows);
    pending_rows_without_columns -= num_rows;
    const UInt64 first_delivered = rows_without_columns_delivered;
    rows_without_columns_delivered += num_rows;

    /// No column is read from the file: every column of the header is filled with defaults.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    ArrowIPC::RecordBatchDecoder::DecodedColumns no_columns;
    auto chunk = ArrowIPC::buildChunk(
        getPort().getHeader(), no_columns, num_rows, format_settings, /* allow_missing_columns */ true, geo_columns, block_missing_values_ptr,
        requested_types, /* visibility_only_structs */ {});

    if (row_index_column)
        chunk.getChunkInfos().add(rowNumbersWithoutColumns(first_delivered, num_rows));

    return chunk;
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

    if (!pending_pieces.empty())
    {
        /// The bytes were attributed to the first piece of the chunk already.
        approx_bytes_read_for_chunk = 0;
        return takePendingPiece();
    }

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
            if (!preserve_order || it->first == next_split_index)
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

                for (auto & piece : delivered_chunk.pieces)
                    pending_pieces.push_back(std::move(piece));
                /// The bytes read since the previous chunk was returned are attributed to this one.
                size_t bytes_read = read_context->bytes_read.load(std::memory_order_relaxed);
                approx_bytes_read_for_chunk = bytes_read - previous_approx_bytes_read;
                previous_approx_bytes_read = bytes_read;
                return takePendingPiece();
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
        Stopwatch wait_watch;
        const auto wait_status = delivery_cv.wait_for(lock, PROGRESS_CHECK_PERIOD);
        ProfileEvents::increment(ProfileEvents::VortexReadWaitMicroseconds, wait_watch.elapsedMicroseconds());
        if (wait_status != std::cv_status::timeout)
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

        const bool nothing_deliverable = delivered.empty() || (preserve_order && delivered.begin()->first != next_split_index);
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
    rows_without_columns_delivered = 0;
    rows_without_columns_selection = nullptr;
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
        file,
        *read_context,
        file_schema,
        /* io_threads */ 1,
        /* is_remote_fs */ false);
}

NamesAndTypesList VortexSchemaReader::readSchema()
{
    initializeIfNeeded();

    /// As the Apache Arrow library reader inferred the types of Vortex files: a field is nullable as in the file unless
    /// `schema_inference_make_columns_nullable` is 0, which removes `Nullable` at every depth, and with 1 the types are
    /// then made nullable recursively, which stops at a type that is nullable already (so the elements of a nullable
    /// struct keep their nullability).
    const UInt64 make_columns_nullable = format_settings.schema_inference_make_columns_nullable;
    NamesAndTypesList result;
    for (const auto & field : file_schema->fields)
    {
        auto type = ArrowIPC::fieldToCHType(
            field, format_settings, make_columns_nullable != 0 && ArrowIPC::isNullableField(field), /* allow_null_type */ true);
        if (make_columns_nullable == 0)
            type = ArrowIPC::removeNullableForInference(type, format_settings);
        else if (make_columns_nullable == 1)
            type = makeNullableRecursively(type, format_settings);
        result.emplace_back(field.name, std::move(type));
    }
    return result;
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
        }, /*supports_reset_parser=*/ true);
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
| `Utf8`, `Binary`                    | [String](/reference/data-types/string)                        | `Utf8` (`Binary` with `output_format_vortex_string_as_string = 0`) |
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
[String](/reference/data-types/string) columns are written as `Utf8`, which Vortex compresses better
than `Binary`. Vortex requires `Utf8` values to be valid UTF-8 while ClickHouse strings are arbitrary
byte sequences, so writing a value that is not valid UTF-8 throws an exception; with
`output_format_vortex_string_as_string = 0` strings are written as `Binary` instead. Vortex has no
fixed-size binary type, so [FixedString](/reference/data-types/fixedstring) is always written as `Binary`;
schema inference reads it back as [String](/reference/data-types/string).
[LowCardinality](/reference/data-types/lowcardinality) columns are written as their underlying type
(Vortex chooses dictionary and other encodings adaptively by itself).
[DateTime](/reference/data-types/datetime) columns are written as `vortex.timestamp` with second precision,
so they are read back as [DateTime64](/reference/data-types/datetime64) with scale 0.
A `vortex.timestamp` carries a unit and not a scale, so only the scales that a unit spells exactly -
0, 3, 6 and 9 - survive schema inference; every other scale is written in the next wider unit and
inferred as that unit's scale, so [DateTime64](/reference/data-types/datetime64)`(1)` and `(2)` come back
as `DateTime64(3)`, `(4)` and `(5)` as `DateTime64(6)`, and `(7)` and `(8)` as `DateTime64(9)`. The values
are rescaled and keep their meaning; only the declared scale widens, and naming the type explicitly reads
it back unchanged. The same holds for [Time64](/reference/data-types/time64).
`vortex.time` is a time of day, while [Time/Time64](/reference/data-types/time64) in ClickHouse also holds
negative values and values of 24 hours and more; writing such a value throws instead of writing a wrong one.
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
| `output_format_vortex_string_as_string`  | Write [String](/reference/data-types/string) columns as `Utf8` instead of `Binary`. A value that is not valid UTF-8 throws an exception; disable the setting to write such data as `Binary`. | `1` |
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
