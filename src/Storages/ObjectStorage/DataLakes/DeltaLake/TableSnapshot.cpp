#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Core/Field.h>
#include <Core/Settings.h>

#include <Columns/IColumn.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/escapeForFileName.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTracker.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

#include <Storages/ObjectStorage/DataLakes/DeltaLake/getSchemaFromSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/PartitionPruner.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/ExpressionVisitor.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/EnginePredicate.h>
#include <delta_kernel_ffi.hpp>
#include <base/scope_guard.h>
#include <fmt/ranges.h>
#include <roaring/roaring.hh>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DELTA_KERNEL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int LOGICAL_ERROR;
}

namespace DB::Setting
{
    extern const SettingsBool delta_lake_enable_expression_visitor_logging;
    extern const SettingsInt64 delta_lake_snapshot_version;
    extern const SettingsBool delta_lake_throw_on_engine_predicate_error;
    extern const SettingsBool delta_lake_enable_engine_predicate;
    extern const SettingsMilliseconds delta_lake_snapshot_load_timeout_ms;
}

namespace ProfileEvents
{
    extern const Event DeltaLakePartitionPrunedFiles;
    extern const Event DeltaLakeSnapshotInitializations;
    extern const Event DeltaLakeScannedFiles;
}

namespace DB::FailPoints
{
    extern const char delta_kernel_force_stale_token_error[];
    extern const char delta_kernel_snapshot_load_pause[];
}

namespace CurrentMetrics
{
    extern const Metric DeltaLakeSnapshotLoadsStuck;
}

namespace DeltaLake
{

class TableSnapshot::Iterator final : public DB::IObjectIterator
{
private:
    /// Struct to hold ObjectInfo along with FFI handles for lazy parsing
    struct ScannedDataFile
    {
        DB::ObjectInfoPtr object;
        KernelDvInfo dv_info_handle;
        std::optional<KernelExpression> transform_handle;

        ScannedDataFile(
            DB::ObjectInfoPtr object_,
            KernelDvInfo dv_info_,
            std::optional<KernelExpression> transform_)
            : object(std::move(object_))
            , dv_info_handle(std::move(dv_info_))
            , transform_handle(std::move(transform_))
        {}
    };

public:
    using UpdateStatsFunc = std::function<void(SnapshotStats &&)>;

    Iterator(
        std::shared_ptr<KernelSnapshotState> kernel_snapshot_state_,
        KernelHelperPtr helper_,
        const ReadSchema & read_schema_,
        const TableSchema & table_schema_,
        const DB::NameToNameMap & physical_names_map_,
        const DB::Names & partition_columns_,
        DB::ObjectStoragePtr object_storage_,
        const DB::ActionsDAG * filter_,
        DB::IDataLakeMetadata::FileProgressCallback callback_,
        size_t list_batch_size_,
        bool enable_expression_visitor_logging_,
        bool throw_on_engine_predicate_error_,
        bool enable_engine_predicate_,
        UpdateStatsFunc update_stats_func_,
        LoggerPtr log_)
        : kernel_snapshot_state(kernel_snapshot_state_)
        , captured_credentials_fingerprint(kernel_snapshot_state_->credentials_fingerprint)
        , helper(helper_)
        , read_schema(read_schema_)
        , expression_schema(table_schema_)
        , partition_columns(partition_columns_)
        , object_storage(object_storage_)
        , callback(callback_)
        , list_batch_size(list_batch_size_)
        , log(log_)
        , enable_expression_visitor_logging(enable_expression_visitor_logging_)
        , throw_on_engine_predicate_error(throw_on_engine_predicate_error_)
        , enable_engine_predicate(enable_engine_predicate_)
        , update_stats_func(update_stats_func_)
    {
        if (filter_)
        {
            filter = filter_->clone();
            pruner.emplace(
                *filter,
                table_schema_,
                partition_columns_,
                physical_names_map_,
                DB::Context::getGlobalContextInstance());

            LOG_TEST(log, "Using filter expression");
        }
        else
        {
            LOG_TEST(log, "No filter expression passed");
        }

        if (!physical_names_map_.empty())
        {
            for (auto & [name, value] : expression_schema)
                name = getPhysicalName(name, physical_names_map_);

            for (auto & name : partition_columns)
                name = getPhysicalName(name, physical_names_map_);
        }

        thread = ThreadFromGlobalPool(
            [&, thread_group = DB::CurrentThread::getGroup()]
            {
                /// Attach to current query thread group, to be able to
                /// have query id in logs and metrics from scanDataFunc.
                DB::ThreadGroupSwitcher switcher(thread_group, DB::ThreadName::DATALAKE_TABLE_SNAPSHOT);
                scanDataFunc();
            });
    }

    ~Iterator() override
    {
        shutdown.store(true);
        schedule_next_batch_cv.notify_one();
        if (thread.joinable())
            thread.join();
    }

    const std::string & getDataPath() const { return helper->getDataPath(); }

    const std::string & getTableLocation() const { return helper->getTableLocation(); }

    void setScanException()
    {
        if (!scan_exception)
        {
            scan_exception = std::current_exception();
            shutdown = true;
        }
    }

    void initScanState()
    {
        if (filter.has_value() && enable_engine_predicate)
        {
            auto predicate = getEnginePredicate(filter.value(), engine_predicate_exception, nullptr);
            scan = KernelUtils::unwrapResult(
                ffi::scan(
                    kernel_snapshot_state->snapshot.get(),
                    kernel_snapshot_state->engine.get(),
                    predicate.get(),
                    /* schema */nullptr),
                "scan");
        }
        else
        {
            scan = KernelUtils::unwrapResult(
                ffi::scan(
                    kernel_snapshot_state->snapshot.get(),
                    kernel_snapshot_state->engine.get(),
                    /* predicate */nullptr,
                    /* schema */nullptr),
                "scan");
        }

        scan_data_iterator = KernelUtils::unwrapResult(
            ffi::scan_metadata_iter_init(kernel_snapshot_state->engine.get(), scan.get()),
            "scan_metadata_iter_init");
    }

    /// One-shot retry on stale-credentials errors. Safe only when no files have been
    /// enqueued yet — replaying would emit duplicates to the consumer.
    bool tryRefreshAndRetryScanState(const DB::Exception & e)
    {
        if (e.code() != DB::ErrorCodes::DELTA_KERNEL_ERROR)
            return false;
        const auto & msg = e.message();
        const bool stale_credentials_error =
            msg.contains("ExpiredToken")
            || msg.contains("InvalidToken")
            || msg.contains("TokenRefreshRequired");
        if (!stale_credentials_error)
            return false;

        {
            std::lock_guard lock(next_mutex);
            if (total_data_files > 0)
            {
                LOG_INFO(
                    log,
                    "Cannot safely retry DeltaLake scan iteration after stale-credentials error: "
                    "{} data file(s) already enqueued for the consumer. Propagating exception.",
                    total_data_files);
                return false;
            }
        }

        /// Two paths can yield fresh credentials:
        ///   1. Catalog-vended providers (Glue / Unity / REST) — `refreshCredentials`
        ///      explicitly swaps the underlying S3 client.
        ///   2. Rotating S3 providers (assume-role, web-identity, IMDS) — the C++ SDK
        ///      auto-refreshes its cached session on `getCredentials` when the cached
        ///      session expiry has passed. In that case the fingerprint changes versus
        ///      what we captured at engine-build time, without any callback firing.
        const bool refreshed_via_callback = helper->refreshCredentials();
        const DB::UInt128 post_fingerprint = helper->getCredentialsFingerprint();
        const bool fingerprint_drifted = post_fingerprint != captured_credentials_fingerprint;

        if (!refreshed_via_callback && !fingerprint_drifted)
        {
            LOG_INFO(
                log,
                "Delta kernel reported stale credentials during scan iteration, but neither "
                "a catalog refresh callback nor SDK-side credential rotation produced fresh "
                "credentials. Propagating exception.");
            return false;
        }

        LOG_INFO(
            log,
            "Delta kernel reported stale credentials during scan iteration; rebuilding "
            "scan state (refreshed via callback: {}, fingerprint drifted: {}). Original error: {}",
            refreshed_via_callback, fingerprint_drifted, msg);

        /// Rebuild through the interruptible load (permit-bounded, `KILL QUERY` and the timeouts
        /// apply on this scan thread too) rather than blocking in the kernel synchronously.
        kernel_snapshot_state = loadKernelSnapshotState(
            helper, std::optional<size_t>(kernel_snapshot_state->snapshot_version), log);
        captured_credentials_fingerprint = kernel_snapshot_state->credentials_fingerprint;
        scan = KernelScan();
        scan_data_iterator = KernelScanDataIterator();
        return true;
    }

    void scanDataFunc()
    {
        bool retried = false;
        try
        {
            while (true)
            {
                try
                {
                    initScanState();

                    LOG_TEST(log, "Starting iterator loop (predicate exception: {})", bool(engine_predicate_exception));

                    while (!shutdown.load())
                    {
                        bool have_scan_data_res = KernelUtils::unwrapResult(
                            ffi::scan_metadata_next(scan_data_iterator.get(), this, visitData),
                            "scan_metadata_next");

                        if (have_scan_data_res)
                        {
                            std::unique_lock lock(next_mutex);
                            LOG_TEST(
                                log, "List batch size is {}/{}, shutdown: {}",
                                data_files.size(),
                                list_batch_size ? DB::toString(list_batch_size) : "Unlimitted",
                                shutdown.load());

                            if (!shutdown.load() && list_batch_size && data_files.size() >= list_batch_size)
                            {
                                schedule_next_batch_cv.wait(
                                    lock,
                                    [&]() { return (data_files.size() < list_batch_size) || shutdown.load(); });
                            }
                        }
                        else
                        {
                            {
                                std::lock_guard lock(next_mutex);
                                iterator_finished = true;
                                LOG_TEST(log, "Set finished");
                            }
                            data_files_cv.notify_all();

                            LOG_TRACE(
                                log, "All data files at version {} were listed "
                                "(scan exception: {}, total data files: {}, total rows: {}, total bytes: {})",
                                kernel_snapshot_state->snapshot_version,
                                bool(scan_exception),
                                total_data_files,
                                total_rows ? DB::toString(*total_rows) : "Unknown",
                                total_bytes);

                            if (update_stats_func
                                && !scan_exception
                                && (!filter.has_value() || !enable_engine_predicate))
                            {
                                update_stats_func(SnapshotStats{
                                    .total_bytes = total_bytes,
                                    /// total_rows is an optional statistic, but total_bytes is obligatory.
                                    .total_rows = total_rows
                                });
                            }
                            return;
                        }
                    }
                    return;
                }
                catch (const DB::Exception & e)
                {
                    if (retried || !tryRefreshAndRetryScanState(e))
                        throw;
                    retried = true;
                }
            }
        }
        catch (...) // Ok: exception saved via setScanException for later handling
        {
            setScanException();
            data_files_cv.notify_all();
            LOG_DEBUG(log, "Exception during scan_metadata_next");
        }
    }

    size_t estimatedKeysCount() override
    {
        /// For now do the same as StorageObjectStorageSource::GlobIterator.
        /// TODO: is it possible to do a precise estimation?
        return std::numeric_limits<size_t>::max();
    }

    std::optional<UInt64> getSnapshotVersion() const override
    {
        return kernel_snapshot_state->snapshot_version;
    }

    DB::ObjectInfoPtr next(size_t) override
    {
        while (true)
        {
            std::optional<ScannedDataFile> scan_item;
            {
                std::unique_lock lock(next_mutex);

                if (!iterator_finished && data_files.empty() && !shutdown)
                {
                    LOG_TEST(log, "Waiting for next data file");
                    schedule_next_batch_cv.notify_one();
                    data_files_cv.wait(lock, [&]() { return !data_files.empty() || iterator_finished || shutdown.load(); });
                }

                if (engine_predicate_exception && throw_on_engine_predicate_error)
                    std::rethrow_exception(engine_predicate_exception);

                if (scan_exception)
                    std::rethrow_exception(scan_exception);

                if (data_files.empty() || shutdown)
                {
                    LOG_TEST(log, "Data files: {}", data_files.size());
                    return nullptr;
                }

                LOG_TEST(log, "Current data files: {}", data_files.size());

                scan_item = std::move(data_files.front());
                data_files.pop_front();
            }

            schedule_next_batch_cv.notify_one();

            auto object = std::move(scan_item->object);

            /// Needed for partition values.
            parseTransformHandle(*scan_item, object);
            if (pruner.has_value() && pruner->canBePruned(*object))
            {
                ProfileEvents::increment(ProfileEvents::DeltaLakePartitionPrunedFiles);

                LOG_TEST(log, "Skipping file {} according to partition pruning", object->getPath());
                continue;
            }

            parseDVHandle(*scan_item, object);
            object->setObjectMetadata(object_storage->getObjectMetadata(object->getPath(), /*with_tags=*/ false));

            if (callback)
            {
                chassert(object->getObjectMetadata());
                callback(DB::FileProgress(0, object->getObjectMetadata()->size_bytes));
            }
            return object;
        }
    }

    void parseTransformHandle(ScannedDataFile & scan_item, DB::ObjectInfoPtr & object)
    {
        auto & metadata = object->data_lake_metadata;
        chassert(metadata.has_value());

        if (scan_item.transform_handle.has_value())
        {
            if (!partition_columns.empty())
            {
                metadata->schema_transform = visitScanCallbackExpression(
                    scan_item.transform_handle->get(),
                    read_schema,
                    expression_schema,
                    enable_expression_visitor_logging);

                LOG_TEST(
                    log,
                    "Parsed transform for file: {}, transform: {}",
                    object->getPath(),
                    metadata->schema_transform->dumpNames());
            }
        }
    }

    void parseDVHandle(ScannedDataFile & scan_item, DB::ObjectInfoPtr & object)
    {
        auto & metadata = object->data_lake_metadata;
        chassert(metadata.has_value());

        if (auto * dv_info_ptr = scan_item.dv_info_handle.get(); dv_info_ptr && ffi::dv_info_has_vector(dv_info_ptr))
        {
            /// `row_indexes_from_dv` returns a vector of row indexes
            /// that should be *removed* from the result set
            ffi::KernelRowIndexArray row_indexes = KernelUtils::unwrapResult(
                ffi::row_indexes_from_dv(
                    dv_info_ptr,
                    kernel_snapshot_state->engine.get(),
                    KernelUtils::toDeltaString(getTableLocation())),
                "row_indexes_from_dv");

            SCOPE_EXIT({
                ffi::free_row_indexes(row_indexes);
            });

            if (row_indexes.len > 0)
            {
                LOG_TEST(log, "Row indexes size {} for file {}", row_indexes.len, object->getPath());

                auto bitmap = std::make_shared<DB::DataLakeObjectMetadata::ExcludedRows>();
                for (size_t i = 0; i < row_indexes.len; ++i)
                {
                    bitmap->add(row_indexes.ptr[i]);
                }
                metadata->excluded_rows = std::move(bitmap);
            }
        }
    }

    static void visitData(
        void * engine_context,
        ffi::SharedScanMetadata * scan_metadata)
    {
        auto * iter = static_cast<Iterator *>(engine_context);
        /// Release the handle on all exit paths to avoid leaking it.
        SCOPE_EXIT({
            ffi::free_scan_metadata(scan_metadata);
        });
        /// Runs inside Rust's `scan_metadata_next`: a C++ exception must not cross the `extern "C"`
        /// frame, so store it (like `scanCallback`) and let `scanDataFunc` rethrow after it returns.
        try
        {
            KernelUtils::unwrapResult(
                ffi::visit_scan_metadata(
                    scan_metadata,
                    iter->kernel_snapshot_state->engine.get(),
                    engine_context,
                    Iterator::scanCallback),
                "visit_scan_metadata");
        }
        catch (...) // Ok: exception saved via setScanException, rethrown by scanDataFunc
        {
            iter->setScanException();
            iter->data_files_cv.notify_all();
        }
    }

    static bool scanCallback(
        ffi::NullableCvoid engine_context,
        struct ffi::KernelStringSlice path,
        int64_t size,
        int64_t /*mod_time*/,
        const ffi::Stats * stats,
        ffi::SharedDvInfo * dv_info,
        ffi::OptionalValue<ffi::SharedExpression *> transform,
        const struct ffi::CStringMap * deprecated)
    {
        try
        {
            return scanCallbackImpl(engine_context, path, size, stats, dv_info, transform, deprecated);
        }
        catch (...)
        {
            auto * context = static_cast<TableSnapshot::Iterator *>(engine_context);
            /// We cannot allow to throw exceptions from ScanCallback,
            /// otherwise delta-kernel will panic and call terminate.
            context->setScanException();
            context->data_files_cv.notify_all();

            return false;  /// Stop iteration on exception
        }
    }

    static bool scanCallbackImpl(
        ffi::NullableCvoid engine_context,
        struct ffi::KernelStringSlice path,
        int64_t size,
        const ffi::Stats * stats,
        ffi::SharedDvInfo * dv_info,
        ffi::OptionalValue<ffi::SharedExpression *> transform,
        const struct ffi::CStringMap * /* deprecated */)
    {
        /// Wrap handles in RAII immediately to ensure cleanup on any exit path
        KernelDvInfo dv_info_handle(dv_info);
        std::optional<KernelExpression> transform_handle;
        if (transform.tag == ffi::OptionalValue<ffi::SharedExpression *>::Tag::Some)
            transform_handle.emplace(transform.some._0);

        auto * context = static_cast<TableSnapshot::Iterator *>(engine_context);
        if (context->shutdown)
        {
            LOG_TEST(
                context->log, "Callback: shutdown detected at first check");

            context->data_files_cv.notify_all();
            return false; /// Break iteration
        }

        if (context->list_batch_size > 0)
        {
            std::unique_lock lock(context->next_mutex);
            if (context->data_files.size() >= context->list_batch_size
                && !context->shutdown.load())
            {
                LOG_TEST(
                    context->log, "Callback pausing: queue size {}/{}",
                    context->data_files.size(), context->list_batch_size);

                context->schedule_next_batch_cv.wait(lock, [&]()
                {
                    return (context->data_files.size() < context->list_batch_size)
                        || context->shutdown.load();
                });
            }

            if (context->shutdown.load())
            {
                LOG_TEST(
                    context->log,
                    "Callback: shutdown detected after queue wait");

                context->data_files_cv.notify_all();
                return false; /// Break iteration
            }
        }

        ProfileEvents::increment(ProfileEvents::DeltaLakeScannedFiles);

        std::string full_path = DB::resolvePathInsideTable(
            context->getDataPath(), DB::unescapeForFileName(KernelUtils::fromDeltaString(path)));
        auto object = std::make_shared<DB::ObjectInfo>(DB::RelativePathWithMetadata(std::move(full_path)));
        object->data_lake_metadata.emplace();

        LOG_TEST(
            context->log,
            "Scanned file: {}, size: {}, num records: {}",
            object->getPath(), size, stats ? DB::toString(stats->num_records) : "Unknown");

        {
            std::lock_guard lock(context->next_mutex);
            context->data_files.emplace_back(std::move(object), std::move(dv_info_handle), std::move(transform_handle));
        }

        context->total_data_files += 1;
        context->total_bytes += size;
        if (stats && context->total_rows.has_value())
            context->total_rows.value() += stats->num_records;
        else
            context->total_rows = std::nullopt;

        context->data_files_cv.notify_one();
        return true;  /// Continue iteration
    }

private:
    using KernelScan = KernelPointerWrapper<ffi::SharedScan, ffi::free_scan>;
    using KernelScanDataIterator = KernelPointerWrapper<ffi::SharedScanMetadataIterator, ffi::free_scan_metadata_iter>;

    std::shared_ptr<KernelSnapshotState> kernel_snapshot_state;
    /// Fingerprint of the credentials embedded in `kernel_snapshot_state` at build time.
    /// Used by the retry path to detect SDK-side auto-refresh of rotating S3 providers
    /// (assume-role, web-identity, IMDS) — those have no `credentials_refresh_callback`,
    /// but `helper->getCredentialsFingerprint()` re-reads from the live S3 client and
    /// reflects any refresh the SDK performed transparently between attempts.
    DB::UInt128 captured_credentials_fingerprint;
    KernelScan scan;
    KernelScanDataIterator scan_data_iterator;
    std::optional<PartitionPruner> pruner;
    std::optional<DB::ActionsDAG> filter;

    KernelHelperPtr helper;
    DB::NamesAndTypesList read_schema;
    DB::NamesAndTypesList expression_schema;
    DB::Names partition_columns;
    const DB::ObjectStoragePtr object_storage;
    const DB::IDataLakeMetadata::FileProgressCallback callback;
    const size_t list_batch_size;
    const LoggerPtr log;
    const bool enable_expression_visitor_logging;
    const bool throw_on_engine_predicate_error;
    const bool enable_engine_predicate;
    const UpdateStatsFunc update_stats_func;

    std::exception_ptr scan_exception;
    std::exception_ptr engine_predicate_exception;

    /// Whether scanDataFunc should stop scanning.
    /// Set in destructor.
    std::atomic<bool> shutdown = false;
    /// A CV to notify that new data_files are available.
    std::condition_variable data_files_cv;
    /// A flag meaning that all data files were scanned
    /// and data scanning thread is finished.
    bool iterator_finished = false;

    std::optional<size_t> total_rows = 0;
    size_t total_bytes = 0;
    size_t total_data_files = 0;

    /// A CV to notify data scanning thread to continue,
    /// as current data batch is fully read.
    std::condition_variable schedule_next_batch_cv;

    std::deque<ScannedDataFile> data_files;
    std::mutex next_mutex;

    /// A thread for async data scanning.
    ThreadFromGlobalPool thread;
};

TableSnapshot::TableSnapshot(
    std::optional<size_t> version_,
    KernelHelperPtr helper_,
    DB::ObjectStoragePtr object_storage_,
    LoggerPtr log_)
    : helper(helper_)
    , object_storage(object_storage_)
    , log(log_)
    , snapshot_version_to_read(version_)
{
    chassert(object_storage);
}

size_t TableSnapshot::getVersion() const
{
    initOrUpdateSnapshot();
    std::lock_guard lock(mutex);
    return getVersionUnlocked();
}

bool TableSnapshot::isAbandonedWithoutWaiters() const
{
    std::lock_guard lock(mutex);
    return !kernel_snapshot_state && inflight_load
        && inflight_load->state.load() == InflightSnapshotLoad::State::Abandoned
        && inflight_load->waiters.load() == 0;
}

bool TableSnapshot::isInitialized() const
{
    std::lock_guard lock(mutex);
    return kernel_snapshot_state != nullptr;
}

bool TableSnapshot::canShareInflightLoad(const KernelClientOptions & client_options) const
{
    std::lock_guard lock(mutex);
    if (inflight_load)
        return inflight_load->client_options == client_options;
    if (reserved_client_options)
        return *reserved_client_options == client_options;
    return true;
}

void TableSnapshot::reserveClientOptions(const KernelClientOptions & client_options)
{
    std::lock_guard lock(mutex);
    reserved_client_options = client_options;
}

size_t TableSnapshot::getVersionUnlocked() const
{
    return getKernelSnapshotState()->snapshot_version;
}

TableSnapshot::SnapshotStats TableSnapshot::getSnapshotStats() const
{
    for (size_t attempt = 0;; ++attempt)
    {
        /// Builds the kernel state (or rebuilds it after a credentials refresh) through the
        /// interruptible shared load, with the mutex released while waiting.
        initOrUpdateSnapshot();

        std::lock_guard lock(mutex);
        if (snapshot_stats.has_value())
            return snapshot_stats.value();

        const auto pre_fingerprint = kernel_state_credentials_fingerprint;
        try
        {
            snapshot_stats = getSnapshotStatsImpl();
        }
        catch (const DB::Exception & e)
        {
            if (attempt > 0 || !tryRefreshAfterStaleTokenError(e, pre_fingerprint, "stats scan"))
                throw;
            /// Fresh credentials: let the next `initOrUpdateSnapshot` rebuild the engine (still
            /// killable, mutex released) and re-run the stats scan against it.
            kernel_state_needs_rebuild = true;
            continue;
        }
        LOG_TEST(
            log, "Updated statistics for snapshot version {}",
            getVersionUnlocked());
        return snapshot_stats.value();
    }
}

TableSnapshot::SnapshotStats TableSnapshot::getSnapshotStatsImpl() const
{
    auto state = getKernelSnapshotState();

    KernelScan fallback_scan;
    fallback_scan = KernelUtils::unwrapResult(
        ffi::scan(
            state->snapshot.get(),
            state->engine.get(),
            /* predicate */nullptr,
            /* schema */nullptr),
        "scan");

    KernelScanMetadataIterator fallback_scan_data_iterator;
    fallback_scan_data_iterator = KernelUtils::unwrapResult(
        ffi::scan_metadata_iter_init(
            state->engine.get(), fallback_scan.get()),
        "scan_metadata_iter_init");

    struct StatsVisitor
    {
        explicit StatsVisitor(ffi::SharedExternEngine * engine_) : engine(engine_) {}

        ffi::SharedExternEngine * const engine;
        size_t total_data_files = 0;
        size_t total_bytes = 0;
        /// Not all writers add rows count to metadata
        std::optional<size_t> total_rows = 0;
        /// Set when `visitData` catches an exception; rethrown by the caller after
        /// `scan_metadata_next` returns (the callback runs inside a Rust `extern "C"` frame).
        std::exception_ptr exception;

        static bool visit(
            ffi::NullableCvoid engine_context,
            struct ffi::KernelStringSlice /* path */,
            int64_t size,
            int64_t /* mod_time */,
            const ffi::Stats * stats,
            ffi::SharedDvInfo * dv_info,
            ffi::OptionalValue<ffi::SharedExpression *> transform,
            const struct ffi::CStringMap * /* deprecated */)
        {
            /// Wrap handles in RAII immediately to ensure cleanup on any exit path
            /// TODO: Actually we do not need any transforms/dv_info to exist here,
            /// so it would be better to implement in delta-kernel scanCallback
            /// which will only collect stats.
            KernelDvInfo dv_info_handle(dv_info);
            std::optional<KernelExpression> transform_handle;
            if (transform.tag == ffi::OptionalValue<ffi::SharedExpression *>::Tag::Some)
                transform_handle.emplace(transform.some._0);

            auto * visitor = static_cast<StatsVisitor *>(engine_context);
            visitor->total_data_files += 1;
            visitor->total_bytes += static_cast<size_t>(size);
            if (stats && visitor->total_rows.has_value())
                visitor->total_rows.value() += stats->num_records;
            else
                visitor->total_rows = std::nullopt;
            return true;
        }

        static void visitData(void * engine_context, ffi::SharedScanMetadata * scan_metadata)
        {
            auto * visitor = static_cast<StatsVisitor *>(engine_context);
            /// Release the handle on all exit paths to avoid leaking it.
            SCOPE_EXIT({
                ffi::free_scan_metadata(scan_metadata);
            });
            /// Runs inside Rust's `scan_metadata_next`: a C++ exception must not cross the
            /// `extern "C"` frame, so store it and let the caller rethrow after the call returns.
            try
            {
                KernelUtils::unwrapResult(
                    ffi::visit_scan_metadata(
                        scan_metadata,
                        visitor->engine,
                        engine_context,
                        StatsVisitor::visit),
                    "visit_scan_metadata");
            }
            catch (...)
            {
                visitor->exception = std::current_exception();
            }
        }
    };

    StatsVisitor visitor(state->engine.get());

    while (true)
    {
        bool have_scan_data = KernelUtils::unwrapResult(
            ffi::scan_metadata_next(
                fallback_scan_data_iterator.get(),
                &visitor,
                StatsVisitor::visitData),
            "scan_metadata_next");

        /// Rethrow only now that `scan_metadata_next` has returned to C++ (see `visitData`).
        if (visitor.exception)
            std::rethrow_exception(visitor.exception);

        if (!have_scan_data)
            break;
    }

    LOG_TEST(
        log, "Snapshot at version {} data files: {}, total rows: {}, total bytes: {}",
        state->snapshot_version,
        visitor.total_data_files,
        visitor.total_rows ? DB::toString(*visitor.total_rows) : "Unknown",
        visitor.total_bytes);

    return SnapshotStats{
        .total_bytes = visitor.total_bytes,
        /// total_rows is an optional statistic, but total_bytes is obligatory.
        .total_rows = visitor.total_rows,
    };
}

std::optional<size_t> TableSnapshot::getTotalRows() const
{
    return getSnapshotStats().total_rows;
}

std::optional<size_t> TableSnapshot::getTotalBytes() const
{
    return getSnapshotStats().total_bytes;
}

bool TableSnapshot::tryRefreshAfterStaleTokenError(
    const DB::Exception & e,
    DB::UInt128 pre_fingerprint,
    const char * context_for_log) const
{
    if (e.code() != DB::ErrorCodes::DELTA_KERNEL_ERROR)
        return false;
    const auto & msg = e.message();
    const bool stale_credentials_error =
        msg.contains("ExpiredToken")
        || msg.contains("InvalidToken")
        || msg.contains("TokenRefreshRequired");
    if (!stale_credentials_error)
        return false;

    const bool refreshed_via_callback = helper->refreshCredentials();
    const auto post_fingerprint = helper->getCredentialsFingerprint();
    const bool fingerprint_drifted = post_fingerprint != pre_fingerprint;
    if (!refreshed_via_callback && !fingerprint_drifted)
        return false;

    LOG_INFO(
        log,
        "Delta kernel reported stale credentials during {}; rebuilding "
        "(refreshed via callback: {}, fingerprint drifted: {}). Original error: {}",
        context_for_log, refreshed_via_callback, fingerprint_drifted, msg);
    return true;
}

void TableSnapshot::initOrUpdateSnapshot() const
{
    std::unique_lock lock(mutex);

    /// Rebuild when credentials rotate so the engine never outlives its embedded STS token.
    const auto current_credentials_fingerprint = helper->getCredentialsFingerprint();
    if (kernel_snapshot_state && !kernel_state_needs_rebuild
        && current_credentials_fingerprint == kernel_state_credentials_fingerprint)
        return;

    /// Pin rebuilds to the already-resolved version so cached latest snapshots don't drift.
    const std::optional<size_t> version_to_build = kernel_snapshot_state
        ? std::optional<size_t>(kernel_snapshot_state->snapshot_version)
        : snapshot_version_to_read;

    ProfileEvents::increment(ProfileEvents::DeltaLakeSnapshotInitializations);

    LOG_TEST(
        log, "{}",
        kernel_snapshot_state ? "Rebuilding kernel snapshot state (credentials rotated)" : "Initializing snapshot");

    /// Resolved on the query thread; also the key deciding whether an in-flight build may be
    /// shared, since query-level S3 timeouts are forwarded into the kernel client.
    const auto client_options = helper->resolveClientOptions();

    for (size_t attempt = 0;; ++attempt)
    {
        /// One in-flight build is shared by every query which needs this snapshot: the first
        /// waiter starts it and later waiters adopt it — also when some waiter already gave up
        /// on it, as long as another one is still waiting (a short-timeout query must not divert
        /// healthy work). Only a load nobody waits for any more is considered dead: it may be
        /// stuck for good, so a fresh (permit-bounded) load is started instead and the table
        /// recovers once the object store does. That restart is only allowed for a pinned
        /// version (or a rebuild of the installed one), whose result is the same version. A first
        /// latest-version load is never repeated on this object, or it could resolve two
        /// different versions: the metadata layer starts a fresh object in that case.
        auto load = inflight_load;
        const bool given_up = load && load->state.load() == InflightSnapshotLoad::State::Abandoned
            && load->waiters.load() == 0;
        const bool other_options = load && !(load->client_options == client_options);
        /// For a first latest-version load the metadata layer already handed out a separate
        /// object when the options differ (see canShareInflightLoad), so here only pinned
        /// versions and rebuilds ever start a second load on the same object.
        if (!load || (version_to_build.has_value() && (given_up || other_options)))
        {
            /// The first load of an object reserved by the metadata layer runs with the reserved
            /// options (they equal this query's: other queries were turned away by then).
            const auto load_options = (!kernel_snapshot_state && reserved_client_options)
                ? *reserved_client_options
                : client_options;
            load = startKernelSnapshotLoad(helper, version_to_build, load_options);
            inflight_load = load;
        }

        /// Registered as a waiter while still holding the mutex — and unregistered under it
        /// again below — so that a query evaluating `given_up` above never observes a waiter
        /// which is already gone, nor misses one which is about to poll.
        load->waiters.fetch_add(1, std::memory_order_relaxed);

        /// Wait for the build OUTSIDE the mutex, so that every waiter sits in its own polling
        /// loop with its own cancellation checks. With the wait under the mutex, sibling
        /// queries slept inside a plain lock and could not be killed while a load was stuck.
        lock.unlock();
        try
        {
            /// This waiter may be cancelled or time out; if so it throws and the build keeps
            /// running, staying installed in `inflight_load` for the other waiters and for
            /// later queries.
            waitForSnapshotLoad(*load, *helper);
        }
        catch (...)
        {
            /// Unregistered — and, when this was the last waiter, marked as given up — in one
            /// critical section with the `given_up` check above, so that neither transition can
            /// be observed half-done by another query.
            lock.lock();
            if (load->waiters.fetch_sub(1, std::memory_order_relaxed) == 1)
                markAbandoned(*load, *helper, log);
            throw;
        }
        lock.lock();
        load->waiters.fetch_sub(1, std::memory_order_relaxed);

        /// The registered load installs its result. A superseded load (given up on by everyone,
        /// then replaced by a fresh one for the same pinned version) may still install it while
        /// nothing is installed yet, so that a waiter which stayed on healthy work completes from
        /// it. Once a state is installed only the registered load may replace it — a rebuild
        /// after a credentials refresh, pinned to the same version — so an object never changes
        /// its version.
        const bool is_current_load = (inflight_load == load);
        if (is_current_load)
            inflight_load = nullptr;

        std::shared_ptr<KernelSnapshotState> built;
        try
        {
            /// Rethrows the build error to every waiter if the build failed.
            built = load->future.get();
        }
        catch (const DB::Exception & e)
        {
            /// Judged against the credentials the failed build actually used (recorded by the
            /// worker), not against the helper's client as seen by whichever waiter handles the
            /// failure: the client may have rotated while the shared load was in flight.
            if (attempt > 0 || !tryRefreshAfterStaleTokenError(e, load->credentials_fingerprint, "snapshot init"))
                throw;
            continue;
        }
        if (!is_current_load && kernel_snapshot_state)
            break;  /// Already installed by the registered load (or a sibling waiter): keep it.
        kernel_snapshot_state = built;
        /// The fingerprint of the credentials inside `built`, not of the helper's current client:
        /// the client may have rotated while the load was in flight (or given up on).
        kernel_state_credentials_fingerprint = built->credentials_fingerprint;
        kernel_state_needs_rebuild = false;
        break;
    }

    LOG_TRACE(
        log, "Initialized snapshot. Snapshot version: {}",
        kernel_snapshot_state->snapshot_version);
}

namespace
{
    /// Bounds the number of snapshot-load worker threads that are alive at the same time,
    /// whether still serving waiters or already given up by them. The permit is reserved
    /// BEFORE the worker is launched (a post-facto counter would let any number of concurrent
    /// loads pass the check together) and released by the worker itself when the kernel call
    /// returns, so stuck workers can never exceed this cap.
    /// Each stuck worker occupies one `GlobalThreadPool` thread, so the cap is derived from the
    /// pool's actual size (`max_thread_pool_size`, configurable): a share of it, at least one
    /// worker and at most 128 (reached with the default pool of 10000), while always leaving
    /// two workers free for unrelated tasks and for shutdown. Only a pool too small even for
    /// that (`max_thread_pool_size <= 2`) gets a cap of 0, so that snapshot loads fail fast
    /// with a clear error instead of occupying the last worker. Builds are shared per table
    /// snapshot, so this stays far above realistic concurrency.
    Int64 maxSnapshotLoadWorkers()
    {
        const auto pool_size = static_cast<Int64>(GlobalThreadPool::instance().getMaxThreads());
        return std::clamp<Int64>(std::min<Int64>(std::max<Int64>(pool_size / 8, 1), pool_size - 2), 0, 128);
    }
    std::atomic<Int64> snapshot_load_worker_permits{0};
}

std::shared_ptr<TableSnapshot::InflightSnapshotLoad> TableSnapshot::startKernelSnapshotLoad(
    KernelHelperPtr kernel_helper, std::optional<size_t> version_to_build, const KernelClientOptions & client_options)
{
    /// The kernel FFI is synchronous and has no cancellation hook: `snapshot_builder_build` reads
    /// `_delta_log` through the kernel's own object store client and blocks on a channel fed by
    /// the kernel's background executor. If the object store never answers, the call never
    /// returns; a query stuck there could not be killed, was not bounded by any timeout and, when
    /// executed by `DDLWorker`, blocked the whole distributed DDL queue
    /// (https://github.com/ClickHouse/ClickHouse/issues/117280).
    /// So the kernel call runs on a worker thread which never touches `this` and outlives any
    /// waiter; it keeps the kernel handles alive and releases them when the kernel call returns.

    /// Reserve a permit before launching, so the bound holds under concurrency.
    const Int64 max_workers = maxSnapshotLoadWorkers();
    if (max_workers == 0)
        throw DB::Exception(
            DB::ErrorCodes::CANNOT_SCHEDULE_TASK,
            "Refusing to load the snapshot of the Delta Lake table at {}: the global thread pool is too small "
            "(max_thread_pool_size = {}) to run a snapshot-load worker without starving other tasks",
            kernel_helper->getTableLocation(), GlobalThreadPool::instance().getMaxThreads());
    if (snapshot_load_worker_permits.fetch_add(1, std::memory_order_relaxed) >= max_workers)
    {
        snapshot_load_worker_permits.fetch_sub(1, std::memory_order_relaxed);
        throw DB::Exception(
            DB::ErrorCodes::CANNOT_SCHEDULE_TASK,
            "Refusing to load the snapshot of the Delta Lake table at {}: {} snapshot-load workers are "
            "already running or stuck inside delta-kernel ({} of them were given up by their queries, "
            "see the DeltaLakeSnapshotLoadsStuck metric); not starting another one until a worker returns",
            kernel_helper->getTableLocation(), max_workers,
            CurrentMetrics::get(CurrentMetrics::DeltaLakeSnapshotLoadsStuck));
    }
    /// Everything below may throw (allocations, thread creation): release the permit unless it
    /// was handed over to the worker, which releases it when the kernel call returns.
    bool permit_transferred = false;
    SCOPE_EXIT({
        if (!permit_transferred)
            snapshot_load_worker_permits.fetch_sub(1, std::memory_order_relaxed);
    });

    /// `client_options` were captured by the caller on the query thread: the worker runs under
    /// a neutral thread group and has no query context to take settings from.
    auto load = std::make_shared<InflightSnapshotLoad>();
    load->client_options = client_options;
    auto task = std::make_shared<std::packaged_task<std::shared_ptr<KernelSnapshotState>()>>(
        [kernel_helper, version_to_build, client_options, load]
        {
            /// Simulates a kernel call which never returns (used by tests).
            DB::FailPointInjection::pauseFailPoint(DB::FailPoints::delta_kernel_snapshot_load_pause);
            return std::make_shared<KernelSnapshotState>(
                *kernel_helper, version_to_build, client_options, load->credentials_fingerprint);
        });
    load->future = task->get_future().share();

    /// The build is shared by unrelated queries, so it must not run under the starting query's
    /// own thread group: its `max_memory_usage` and accounting would silently apply to every
    /// waiter. The worker gets a group of its own instead: built from the starting query's
    /// context, so that the kernel's log lines (`DeltaKernelTracing` in `system.text_log`) stay
    /// attributed to the query which started the load, as they were when the load ran on the
    /// query thread — but through the raw constructor, which applies no per-query memory
    /// limits, and with the memory accounted to the server total rather than to that query.
    DB::ThreadGroupPtr thread_group;
    DB::ContextPtr group_context = DB::CurrentThread::tryGetQueryContext();
    if (!group_context)
        group_context = DB::Context::getGlobalContextInstance();
    if (group_context)
    {
        thread_group = std::make_shared<DB::ThreadGroup>(group_context, /* os_threads_nice_value */ 0);
        thread_group->memory_tracker.setDescription("Delta Lake snapshot load");
        thread_group->memory_tracker.setParent(&total_memory_tracker);
    }
    else
        thread_group = DB::CurrentThread::getGroup();

    ThreadFromGlobalPool thread(
        [task, load, thread_group]
        {
            DB::ThreadGroupSwitcher switcher(thread_group, DB::ThreadName::DATALAKE_TABLE_SNAPSHOT);
            (*task)();
            /// If every waiter already gave up, undoing the stuck-load accounting is ours to do.
            if (load->state.exchange(InflightSnapshotLoad::State::Finished) == InflightSnapshotLoad::State::Abandoned)
                CurrentMetrics::sub(CurrentMetrics::DeltaLakeSnapshotLoadsStuck);
            snapshot_load_worker_permits.fetch_sub(1, std::memory_order_relaxed);
        });
    /// Nobody joins the worker: waiters wait on the shared future with their own cancellation
    /// checks, and the captured shared state keeps everything the worker needs alive.
    thread.detach();
    permit_transferred = true;
    return load;
}

void TableSnapshot::waitForSnapshotLoad(InflightSnapshotLoad & load, const IKernelHelper & kernel_helper)
{
    DB::QueryStatusPtr process_list_element;
    UInt64 timeout_ms = 0;
    auto context = DB::CurrentThread::tryGetQueryContext();
    if (!context)
        context = DB::Context::getGlobalContextInstance();
    if (context)
    {
        process_list_element = context->getProcessListElementSafe();
        timeout_ms = context->getSettingsRef()[DB::Setting::delta_lake_snapshot_load_timeout_ms].totalMilliseconds();
    }

    Stopwatch watch;
    while (load.future.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready)
    {
        /// Throws if the query was killed or `max_execution_time` is exceeded. Giving up is
        /// accounted for by the caller (see markAbandoned), under the snapshot's mutex.
        if (process_list_element)
            process_list_element->checkTimeLimit();

        if (timeout_ms && watch.elapsedMilliseconds() >= timeout_ms)
            throw DB::Exception(
                DB::ErrorCodes::TIMEOUT_EXCEEDED,
                "Timeout exceeded while loading the snapshot of the Delta Lake table at {}: "
                "waited {} ms, delta_lake_snapshot_load_timeout_ms is {} ms",
                kernel_helper.getTableLocation(), watch.elapsedMilliseconds(), timeout_ms);
    }
}

void TableSnapshot::markAbandoned(InflightSnapshotLoad & load, const IKernelHelper & kernel_helper, const LoggerPtr & log)
{
    /// The build keeps running: the worker delivers or drops its result when the kernel call
    /// returns, and undoes the stuck-load count then. Callers which share the load call this
    /// under `TableSnapshot::mutex`, in one critical section with the waiter count.
    auto expected = InflightSnapshotLoad::State::Running;
    if (load.state.compare_exchange_strong(expected, InflightSnapshotLoad::State::Abandoned))
    {
        CurrentMetrics::add(CurrentMetrics::DeltaLakeSnapshotLoadsStuck);
        LOG_WARNING(
            log,
            "Giving up waiting for the delta-kernel snapshot load of the table at {}: no waiter is left; "
            "the worker thread keeps running until the kernel call returns "
            "(stuck loads: {}, snapshot-load workers are capped at {})",
            kernel_helper.getTableLocation(),
            CurrentMetrics::get(CurrentMetrics::DeltaLakeSnapshotLoadsStuck), maxSnapshotLoadWorkers());
    }
}

std::shared_ptr<TableSnapshot::KernelSnapshotState> TableSnapshot::loadKernelSnapshotState(
    KernelHelperPtr kernel_helper, std::optional<size_t> version_to_build, const LoggerPtr & log)
{
    auto load = startKernelSnapshotLoad(kernel_helper, version_to_build, kernel_helper->resolveClientOptions());
    try
    {
        waitForSnapshotLoad(*load, *kernel_helper);
    }
    catch (...)
    {
        /// A private, unshared load: nobody else waits, so give it up right here.
        markAbandoned(*load, *kernel_helper, log);
        throw;
    }
    /// Rethrows the build error, if any.
    return load->future.get();
}

std::shared_ptr<TableSnapshot::KernelSnapshotState> TableSnapshot::getKernelSnapshotState() const
{
    /// Built by `initOrUpdateSnapshot` at every public entry point before reaching here.
    if (!kernel_snapshot_state)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Kernel snapshot state is not initialized");
    return kernel_snapshot_state;
}

TableSnapshot::KernelSnapshotState::KernelSnapshotState(
    const IKernelHelper & helper_,
    std::optional<size_t> snapshot_version_,
    const KernelClientOptions & client_options_,
    DB::UInt128 & used_credentials_fingerprint)
{
    fiu_do_on(DB::FailPoints::delta_kernel_force_stale_token_error,
    {
        throw DB::Exception(
            DB::ErrorCodes::DELTA_KERNEL_ERROR,
            "ExpiredToken: forced by delta_kernel_force_stale_token_error failpoint");
    });

    /// The fingerprint is taken inside the helper from the same client snapshot the builder is
    /// filled with, so it describes the credentials this engine is actually built with. It is
    /// published to the load right away, before the kernel calls below which may throw.
    auto * engine_builder = helper_.createBuilderWithOptions(client_options_, credentials_fingerprint);
    used_credentials_fingerprint = credentials_fingerprint;
    engine = KernelUtils::unwrapResult(ffi::builder_build(engine_builder), "builder_build");

    using KernelSnapshotBuilder = KernelPointerWrapper<ffi::MutableFfiSnapshotBuilder, ffi::free_snapshot_builder>;
    KernelSnapshotBuilder snapshot_builder(KernelUtils::unwrapResult(
        ffi::get_snapshot_builder(
            KernelUtils::toDeltaString(helper_.getTableLocation()),
            engine.get()),
        "get_snapshot_builder"));
    if (snapshot_version_.has_value())
    {
        auto * builder_handle = snapshot_builder.get();
        ffi::snapshot_builder_set_version(&builder_handle, snapshot_version_.value());
    }
    /// `snapshot_builder_build` consumes the handle, so release() prevents the RAII destructor
    /// from double-freeing on success. The destructor still frees on early exception paths.
    snapshot = KernelUtils::unwrapResult(
        ffi::snapshot_builder_build(snapshot_builder.release()),
        "snapshot_builder_build");

    snapshot_version = ffi::version(snapshot.get());
    scan = KernelUtils::unwrapResult(
        ffi::scan(snapshot.get(), engine.get(), /* predicate */{}, /* engine_schema */nullptr),
        "scan");
}

DB::ObjectIterator TableSnapshot::iterate(
    const DB::ActionsDAG * filter_dag,
    DB::IDataLakeMetadata::FileProgressCallback callback,
    size_t list_batch_size,
    DB::ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    initOrUpdateSnapshot();
    std::lock_guard lock(mutex);
    initOrUpdateSchemaIfChanged();
    auto state = getKernelSnapshotState();
    auto update_stats_func = [self = shared_from_this(), version = state->snapshot_version, this]
        (SnapshotStats && stats)
        {
            std::unique_lock lk(mutex, std::defer_lock);
            if (lk.try_lock())
            {
                if (!snapshot_stats.has_value())
                {
                    snapshot_stats.emplace(std::move(stats));
                    LOG_TEST(
                        log, "Updated statistics from data files iterator for snapshot version {}",
                        version);
                }
            }
        };
    return std::make_shared<TableSnapshot::Iterator>(
        state,
        helper,
        schema->read_schema,
        schema->table_schema,
        schema->physical_names_map,
        schema->partition_columns,
        object_storage,
        filter_dag,
        callback,
        list_batch_size,
        settings[DB::Setting::delta_lake_enable_expression_visitor_logging],
        settings[DB::Setting::delta_lake_throw_on_engine_predicate_error],
        settings[DB::Setting::delta_lake_enable_engine_predicate],
        std::move(update_stats_func),
        log);
}

void TableSnapshot::initOrUpdateSchemaIfChanged() const
{
    if (!schema.has_value())
    {
        auto state = getKernelSnapshotState();
        auto [table_schema, physical_names_map] = getTableSchemaFromSnapshot(state->snapshot.get(), state->engine.get());

        if (table_schema.empty())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Table schema cannot be empty");

        auto read_schema = getReadSchemaFromSnapshot(state->scan.get(), state->engine.get());
        auto partition_columns = getPartitionColumnsFromSnapshot(state->snapshot.get());

        /// Both names are logical here; the rename to physical names happens later, on copies.
        for (const auto & column_name : partition_columns)
            if (!table_schema.tryGetByName(column_name))
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                    "Partition column {} is not present in the table schema", column_name);

        LOG_TRACE(
            log, "Table logical schema: {}, read schema: {}, "
            "partition columns: {}, physical names map size: {}",
            fmt::join(table_schema.getNames(), ", "),
            fmt::join(read_schema.getNames(), ", "),
            fmt::join(partition_columns, ", "),
            physical_names_map.size());

        schema.emplace(SchemaInfo{
            .table_schema = std::move(table_schema),
            .read_schema = std::move(read_schema),
            .physical_names_map = std::move(physical_names_map),
            .partition_columns = std::move(partition_columns),
        });
    }
}

const DB::NamesAndTypesList & TableSnapshot::getTableSchema() const
{
    initOrUpdateSnapshot();
    std::lock_guard lock(mutex);
    initOrUpdateSchemaIfChanged();
    return schema->table_schema;
}

const DB::NamesAndTypesList & TableSnapshot::getReadSchema() const
{
    initOrUpdateSnapshot();
    std::lock_guard lock(mutex);
    initOrUpdateSchemaIfChanged();
    return schema->read_schema;
}

const DB::Names & TableSnapshot::getPartitionColumns() const
{
    initOrUpdateSnapshot();
    std::lock_guard lock(mutex);
    initOrUpdateSchemaIfChanged();
    return schema->partition_columns;
}

const DB::NameToNameMap & TableSnapshot::getPhysicalNamesMap() const
{
    initOrUpdateSnapshot();
    std::lock_guard lock(mutex);
    initOrUpdateSchemaIfChanged();
    return schema->physical_names_map;
}

}

#endif
