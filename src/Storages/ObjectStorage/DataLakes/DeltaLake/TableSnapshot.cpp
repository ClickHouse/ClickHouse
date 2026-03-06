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
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/escapeForFileName.h>
#include <Common/setThreadName.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

#include <Storages/ObjectStorage/DataLakes/DeltaLake/getSchemaFromSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/PartitionPruner.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/ExpressionVisitor.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/EnginePredicate.h>
#include <delta_kernel_ffi.hpp>
#include <fmt/ranges.h>
#include <roaring/roaring.hh>


namespace fs = std::filesystem;

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

namespace DB::Setting
{
    extern const SettingsBool delta_lake_enable_expression_visitor_logging;
    extern const SettingsInt64 delta_lake_snapshot_version;
    extern const SettingsBool delta_lake_throw_on_engine_predicate_error;
    extern const SettingsBool delta_lake_enable_engine_predicate;
}

namespace ProfileEvents
{
    extern const Event DeltaLakePartitionPrunedFiles;
    extern const Event DeltaLakeScannedFiles;
}

namespace DB
{

Field parseFieldFromString(const String & value, DB::DataTypePtr data_type)
{
    try
    {
        ReadBufferFromString buffer(value);
        auto col = data_type->createColumn();
        auto serialization = data_type->getSerialization({ISerialization::Kind::DEFAULT}, {});
        serialization->deserializeWholeText(*col, buffer, FormatSettings{});
        return (*col)[0];
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot parse {} for data type {}: {}",
            value, data_type->getName(), getCurrentExceptionMessage(false));
    }
}

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

    void scanDataFunc()
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
        }
        catch (...)
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
        ffi::visit_scan_metadata(scan_metadata, engine_context, Iterator::scanCallback);
        ffi::free_scan_metadata(scan_metadata);
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

        std::string full_path = fs::path(context->getDataPath()) / DB::unescapeForFileName(KernelUtils::fromDeltaString(path));
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
    std::lock_guard lock(mutex);
    return getVersionUnlocked();
}

size_t TableSnapshot::getVersionUnlocked() const
{
    return getKernelSnapshotState()->snapshot_version;
}

TableSnapshot::SnapshotStats TableSnapshot::getSnapshotStats() const
{
    if (!snapshot_stats.has_value())
    {
        snapshot_stats = getSnapshotStatsImpl();
        LOG_TEST(
            log, "Updated statistics for snapshot version {}",
            getVersionUnlocked());
    }
    return snapshot_stats.value();
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
        size_t total_data_files = 0;
        size_t total_bytes = 0;
        /// Not all writers add rows count to metadata
        std::optional<size_t> total_rows = 0;

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
            ffi::visit_scan_metadata(scan_metadata, engine_context, StatsVisitor::visit);
            ffi::free_scan_metadata(scan_metadata);
        }
    };

    StatsVisitor visitor;

    while (true)
    {
        bool have_scan_data = KernelUtils::unwrapResult(
            ffi::scan_metadata_next(
                fallback_scan_data_iterator.get(),
                &visitor,
                StatsVisitor::visitData),
            "scan_metadata_next");

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
    std::lock_guard lock(mutex);
    return getSnapshotStats().total_rows;
}

std::optional<size_t> TableSnapshot::getTotalBytes() const
{
    std::lock_guard lock(mutex);
    return getSnapshotStats().total_bytes;
}

void TableSnapshot::initOrUpdateSnapshot() const
{
    if (kernel_snapshot_state)
        return;

    LOG_TEST(log, "Initializing snapshot");

    kernel_snapshot_state = std::make_shared<KernelSnapshotState>(*helper, snapshot_version_to_read);

    LOG_TRACE(
        log, "Initialized snapshot. Snapshot version: {}",
        kernel_snapshot_state->snapshot_version);
}

std::shared_ptr<TableSnapshot::KernelSnapshotState> TableSnapshot::getKernelSnapshotState() const
{
    initOrUpdateSnapshot();
    return kernel_snapshot_state;
}

TableSnapshot::KernelSnapshotState::KernelSnapshotState(const IKernelHelper & helper_, std::optional<size_t> snapshot_version_)
{
    auto * engine_builder = helper_.createBuilder();
    engine = KernelUtils::unwrapResult(ffi::builder_build(engine_builder), "builder_build");
    if (snapshot_version_.has_value())
    {
        snapshot = KernelUtils::unwrapResult(
            ffi::snapshot_at_version(
                KernelUtils::toDeltaString(helper_.getTableLocation()),
                engine.get(),
                snapshot_version_.value()),
            "snapshot");
    }
    else
    {
        snapshot = KernelUtils::unwrapResult(
            ffi::snapshot(
                KernelUtils::toDeltaString(helper_.getTableLocation()),
                engine.get()),
            "snapshot");
    }
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
        auto [table_schema, physical_names_map] = getTableSchemaFromSnapshot(state->snapshot.get());

        if (table_schema.empty())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Table schema cannot be empty");

        auto read_schema = getReadSchemaFromSnapshot(state->scan.get());
        auto partition_columns = getPartitionColumnsFromSnapshot(state->snapshot.get());

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
    std::lock_guard lock(mutex);
    initOrUpdateSchemaIfChanged();
    return schema->table_schema;
}

const DB::NamesAndTypesList & TableSnapshot::getReadSchema() const
{
    std::lock_guard lock(mutex);
    initOrUpdateSchemaIfChanged();
    return schema->read_schema;
}

const DB::Names & TableSnapshot::getPartitionColumns() const
{
    std::lock_guard lock(mutex);
    initOrUpdateSchemaIfChanged();
    return schema->partition_columns;
}

const DB::NameToNameMap & TableSnapshot::getPhysicalNamesMap() const
{
    std::lock_guard lock(mutex);
    initOrUpdateSchemaIfChanged();
    return schema->physical_names_map;
}

}

#endif
