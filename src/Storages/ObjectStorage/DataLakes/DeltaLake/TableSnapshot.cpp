#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>

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

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>

#include <Storages/ObjectStorage/DataLakes/DeltaLake/getSchemaFromSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/PartitionPruner.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/ExpressionVisitor.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/EnginePredicate.h>
#include <delta_kernel_ffi.hpp>
#include <fmt/ranges.h>


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
}

namespace DB
{

Field parseFieldFromString(const String & value, DB::DataTypePtr data_type)
{
    try
    {
        ReadBufferFromString buffer(value);
        auto col = data_type->createColumn();
        auto serialization = data_type->getSerialization({ISerialization::Kind::DEFAULT});
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
public:
    Iterator(
        std::shared_ptr<KernelSnapshotState> kernel_snapshot_state_,
        const std::string & data_prefix_,
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
        LoggerPtr log_)
        : kernel_snapshot_state(kernel_snapshot_state_)
        , data_prefix(data_prefix_)
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
                ffi::scan(kernel_snapshot_state->snapshot.get(), kernel_snapshot_state->engine.get(), predicate.get()),
                "scan");
        }
        else
        {
            scan = KernelUtils::unwrapResult(
                ffi::scan(kernel_snapshot_state->snapshot.get(), kernel_snapshot_state->engine.get(), nullptr),
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
                    if (!shutdown.load() && list_batch_size && data_files.size() >= list_batch_size)
                    {
                        LOG_TEST(log, "List batch size is {}/{}", data_files.size(), list_batch_size);

                        schedule_next_batch_cv.wait(
                            lock,
                            [&]() { return (data_files.size() < list_batch_size) || shutdown.load(); });
                    }
                }
                else
                {
                    LOG_TEST(log, "All data files were listed");
                    {
                        std::lock_guard lock(next_mutex);
                        iterator_finished = true;
                        LOG_TEST(log, "Set finished");
                    }
                    data_files_cv.notify_all();
                    LOG_TEST(log, "Notified");
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
            DB::ObjectInfoPtr object;
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

                object = data_files.front();
                data_files.pop_front();
                schedule_next_batch_cv.notify_one();
            }

            chassert(object);
            if (pruner.has_value() && pruner->canBePruned(*object))
            {
                ProfileEvents::increment(ProfileEvents::DeltaLakePartitionPrunedFiles);

                LOG_TEST(log, "Skipping file {} according to partition pruning", object->getPath());
                continue;
            }

            object->setObjectMetadata(object_storage->getObjectMetadata(object->getPath(), /*with_tags=*/ false));

            if (callback)
            {
                chassert(object->getObjectMetadata());
                callback(DB::FileProgress(0, object->getObjectMetadata()->size_bytes));
            }
            return object;
        }
    }

    static void visitData(
        void * engine_context,
        ffi::SharedScanMetadata * scan_metadata)
    {
        ffi::visit_scan_metadata(scan_metadata, engine_context, Iterator::scanCallback);
        ffi::free_scan_metadata(scan_metadata);
    }

    static void scanCallback(
        ffi::NullableCvoid engine_context,
        struct ffi::KernelStringSlice path,
        int64_t size,
        const ffi::Stats * stats,
        const ffi::CDvInfo * dv_info,
        const ffi::Expression * transform,
        const struct ffi::CStringMap * deprecated)
    {
        try
        {
            scanCallbackImpl(engine_context, path, size, stats, dv_info, transform, deprecated);
        }
        catch (...)
        {
            auto * context = static_cast<TableSnapshot::Iterator *>(engine_context);
            /// We cannot allow to throw exceptions from ScanCallback,
            /// otherwise delta-kernel will panic and call terminate.
            context->setScanException();
        }
    }

    static void scanCallbackImpl(
        ffi::NullableCvoid engine_context,
        struct ffi::KernelStringSlice path,
        int64_t size,
        const ffi::Stats * stats,
        const ffi::CDvInfo * /* dv_info */,
        const ffi::Expression * transform,
        const struct ffi::CStringMap * /* deprecated */)
    {
        auto * context = static_cast<TableSnapshot::Iterator *>(engine_context);
        if (context->shutdown)
        {
            context->data_files_cv.notify_all();
            return;
        }

        std::string full_path = fs::path(context->data_prefix) / DB::unescapeForFileName(KernelUtils::fromDeltaString(path));
        auto object = std::make_shared<DB::ObjectInfo>(DB::RelativePathWithMetadata(std::move(full_path)));

        if (transform && !context->partition_columns.empty())
        {
            auto parsed_transform = visitScanCallbackExpression(
                transform,
                context->read_schema,
                context->expression_schema,
                context->enable_expression_visitor_logging);

            object->data_lake_metadata = DB::DataLakeObjectMetadata{ .transform = parsed_transform };

            LOG_TEST(
                context->log,
                "Scanned file: {}, size: {}, num records: {}, transform: {}",
                object->getPath(), size, stats ? DB::toString(stats->num_records) : "Unknown",
                parsed_transform->dumpNames());
        }
        else
        {
            LOG_TEST(
                context->log,
                "Scanned file: {}, size: {}, num records: {}",
                object->getPath(), size, stats ? DB::toString(stats->num_records) : "Unknown");
        }

        {
            std::lock_guard lock(context->next_mutex);
            context->data_files.push_back(std::move(object));
        }
        context->data_files_cv.notify_one();
    }

private:
    using KernelScan = KernelPointerWrapper<ffi::SharedScan, ffi::free_scan>;
    using KernelScanDataIterator = KernelPointerWrapper<ffi::SharedScanMetadataIterator, ffi::free_scan_metadata_iter>;

    std::shared_ptr<KernelSnapshotState> kernel_snapshot_state;
    KernelScan scan;
    KernelScanDataIterator scan_data_iterator;
    std::optional<PartitionPruner> pruner;
    std::optional<DB::ActionsDAG> filter;

    const std::string data_prefix;
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

    /// A CV to notify data scanning thread to continue,
    /// as current data batch is fully read.
    std::condition_variable schedule_next_batch_cv;

    std::deque<DB::ObjectInfoPtr> data_files;
    std::mutex next_mutex;

    /// A thread for async data scanning.
    ThreadFromGlobalPool thread;
};

static constexpr auto LATEST_SNAPSHOT_VERSION = -1;

TableSnapshot::TableSnapshot(
    KernelHelperPtr helper_,
    DB::ObjectStoragePtr object_storage_,
    DB::ContextPtr context_,
    LoggerPtr log_)
    : helper(helper_)
    , object_storage(object_storage_)
    , log(log_)
{
    updateSettings(context_);
}

size_t TableSnapshot::getVersion() const
{
    initSnapshot();
    return kernel_snapshot_state->snapshot_version;
}

void TableSnapshot::updateSettings(const DB::ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    enable_expression_visitor_logging = settings[DB::Setting::delta_lake_enable_expression_visitor_logging];
    throw_on_engine_visitor_error = settings[DB::Setting::delta_lake_throw_on_engine_predicate_error];
    enable_engine_predicate = settings[DB::Setting::delta_lake_enable_engine_predicate];
    if (settings[DB::Setting::delta_lake_snapshot_version].value != LATEST_SNAPSHOT_VERSION)
    {
        if (settings[DB::Setting::delta_lake_snapshot_version].value < 0)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Snapshot version cannot be a negative value");

        snapshot_version_to_read = settings[DB::Setting::delta_lake_snapshot_version];
    }
}

void TableSnapshot::update(const DB::ContextPtr & context)
{
    updateSettings(context);
    if (!kernel_snapshot_state)
    {
        /// Snapshot is not yet created,
        /// so next attempt to create it would return the latest snapshot.
        return;
    }
    initSnapshotImpl();
}

void TableSnapshot::initSnapshot() const
{
    if (kernel_snapshot_state)
        return;
    initSnapshotImpl();
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
    scan = KernelUtils::unwrapResult(ffi::scan(snapshot.get(), engine.get(), /* predicate */{}), "scan");
}

void TableSnapshot::initSnapshotImpl() const
{
    LOG_TEST(log, "Initializing snapshot");

    kernel_snapshot_state = std::make_shared<KernelSnapshotState>(*helper, snapshot_version_to_read);

    LOG_TRACE(log, "Initialized scan state. Snapshot version: {}", kernel_snapshot_state->snapshot_version);

    std::tie(table_schema, physical_names_map) = getTableSchemaFromSnapshot(kernel_snapshot_state->snapshot.get());
    LOG_TRACE(
        log, "Table logical schema: {}, physical names map size: {}",
        fmt::join(table_schema.getNames(), ", "), physical_names_map.size());

    if (table_schema.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Table schema cannot be empty");

    read_schema = getReadSchemaFromSnapshot(kernel_snapshot_state->scan.get());
    LOG_TRACE(log, "Table read schema: {}", fmt::join(read_schema.getNames(), ", "));

    partition_columns = getPartitionColumnsFromSnapshot(kernel_snapshot_state->snapshot.get());
    LOG_TRACE(log, "Partition columns: {}", fmt::join(partition_columns, ", "));
}

DB::ObjectIterator TableSnapshot::iterate(
    const DB::ActionsDAG * filter_dag,
    DB::IDataLakeMetadata::FileProgressCallback callback,
    size_t list_batch_size)
{
    initSnapshot();
    return std::make_shared<TableSnapshot::Iterator>(
        kernel_snapshot_state,
        helper->getDataPath(),
        getReadSchema(),
        getTableSchema(),
        getPhysicalNamesMap(),
        getPartitionColumns(),
        object_storage,
        filter_dag,
        callback,
        list_batch_size,
        enable_expression_visitor_logging,
        throw_on_engine_visitor_error,
        enable_engine_predicate,
        log);
}

const DB::NamesAndTypesList & TableSnapshot::getTableSchema() const
{
    initSnapshot();
    return table_schema;
}

const DB::NamesAndTypesList & TableSnapshot::getReadSchema() const
{
    initSnapshot();
    return read_schema;
}

const DB::Names & TableSnapshot::getPartitionColumns() const
{
    initSnapshot();
    return partition_columns;
}

const DB::NameToNameMap & TableSnapshot::getPhysicalNamesMap() const
{
    initSnapshot();
    return physical_names_map;
}

}

#endif
