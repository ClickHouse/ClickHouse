#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Core/Field.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/escapeForFileName.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>

#include "getSchemaFromSnapshot.h"
#include "PartitionPruner.h"
#include "KernelUtils.h"
#include "ExpressionVisitor.h"
#include <delta_kernel_ffi.hpp>
#include <fmt/ranges.h>


namespace fs = std::filesystem;

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
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
        auto serialization = data_type->getSerialization(ISerialization::Kind::DEFAULT);
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
        const KernelExternEngine & engine_,
        const KernelSnapshot & snapshot_,
        KernelScan & scan_,
        const std::string & data_prefix_,
        const TableSchema & table_schema_,
        const DB::NameToNameMap & physical_names_map_,
        const DB::Names & partition_columns_,
        DB::ObjectStoragePtr object_storage_,
        const DB::ActionsDAG * filter_dag_,
        DB::IDataLakeMetadata::FileProgressCallback callback_,
        size_t list_batch_size_,
        LoggerPtr log_)
        : engine(engine_)
        , snapshot(snapshot_)
        , scan(scan_)
        , data_prefix(data_prefix_)
        , expression_schema(table_schema_)
        , partition_columns(partition_columns_)
        , object_storage(object_storage_)
        , callback(callback_)
        , list_batch_size(list_batch_size_)
        , log(log_)
        , thread([&, thread_group = DB::CurrentThread::getGroup()] {
            /// Attach to current query thread group, to be able to
            /// have query id in logs and metrics from scanDataFunc.
            DB::ThreadGroupSwitcher switcher(thread_group, "TableSnapshot");
            scanDataFunc();
        })
    {
        if (filter_dag_)
        {
            pruner.emplace(
                *filter_dag_,
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
    }

    ~Iterator() override
    {
        shutdown.store(true);
        schedule_next_batch_cv.notify_one();
        if (thread.joinable())
            thread.join();
    }

    void initScanState()
    {
        scan = KernelUtils::unwrapResult(ffi::scan(snapshot.get(), engine.get(), /* predicate */{}), "scan");
        scan_data_iterator = KernelUtils::unwrapResult(
            ffi::scan_metadata_iter_init(engine.get(), scan.get()),
            "scan_metadata_iter_init");
    }

    void scanDataFunc()
    {
        initScanState();
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
                }
                data_files_cv.notify_all();
                return;
            }
        }
    }

    size_t estimatedKeysCount() override
    {
        /// For now do the same as StorageObjectStorageSource::GlobIterator.
        /// TODO: is it possible to do a precise estimation?
        return std::numeric_limits<size_t>::max();
    }

    DB::ObjectInfoPtr next(size_t) override
    {
        while (true)
        {
            DB::ObjectInfoPtr object;
            {
                std::unique_lock lock(next_mutex);
                if (!iterator_finished && data_files.empty())
                {
                    LOG_TEST(log, "Waiting for next data file");
                    schedule_next_batch_cv.notify_one();
                    data_files_cv.wait(lock, [&]() { return !data_files.empty() || iterator_finished; });
                }

                if (scan_exception)
                    std::rethrow_exception(scan_exception);

                if (data_files.empty())
                    return nullptr;

                LOG_TEST(log, "Current data files: {}", data_files.size());

                object = data_files.front();
                data_files.pop_front();
                if (data_files.empty())
                    schedule_next_batch_cv.notify_one();
            }

            chassert(object);
            if (pruner.has_value() && pruner->canBePruned(*object))
            {
                ProfileEvents::increment(ProfileEvents::DeltaLakePartitionPrunedFiles);

                LOG_TEST(log, "Skipping file {} according to partition pruning", object->getPath());
                continue;
            }

            object->metadata = object_storage->getObjectMetadata(object->getPath());

            if (callback)
            {
                chassert(object->metadata);
                callback(DB::FileProgress(0, object->metadata->size_bytes));
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
        const ffi::DvInfo * dv_info,
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
            if (!context->scan_exception)
            {
                /// We cannot allow to throw exceptions from ScanCallback,
                /// otherwise delta-kernel will panic and call terminate.
                context->scan_exception = std::current_exception();
            }
        }
    }

    static void scanCallbackImpl(
        ffi::NullableCvoid engine_context,
        struct ffi::KernelStringSlice path,
        int64_t size,
        const ffi::Stats * stats,
        const ffi::DvInfo * /* dv_info */,
        const ffi::Expression * transform,
        const struct ffi::CStringMap * /* deprecated */)
    {
        auto * context = static_cast<TableSnapshot::Iterator *>(engine_context);
        std::string full_path = fs::path(context->data_prefix) / DB::unescapeForFileName(KernelUtils::fromDeltaString(path));
        auto object = std::make_shared<DB::ObjectInfo>(std::move(full_path));

        if (transform && !context->partition_columns.empty())
        {
            auto parsed_transform = visitScanCallbackExpression(transform, context->expression_schema);
            object->data_lake_metadata = DB::DataLakeObjectMetadata{ .transform = parsed_transform };

            LOG_TEST(
                context->log,
                "Scanned file: {}, size: {}, num records: {}, transform: {}",
                object->getPath(), size, stats ? DB::toString(stats->num_records) : "Unknown",
                parsed_transform->dumpNames());
        }
        else
            LOG_TEST(
                context->log,
                "Scanned file: {}, size: {}, num records: {}",
                object->getPath(), size, stats ? DB::toString(stats->num_records) : "Unknown");

        {
            std::lock_guard lock(context->next_mutex);
            context->data_files.push_back(std::move(object));
        }
        context->data_files_cv.notify_one();
    }

private:
    using KernelScan = KernelPointerWrapper<ffi::SharedScan, ffi::free_scan>;
    using KernelScanDataIterator = KernelPointerWrapper<ffi::SharedScanMetadataIterator, ffi::free_scan_metadata_iter>;

    const KernelExternEngine & engine;
    const KernelSnapshot & snapshot;
    KernelScan & scan;
    KernelScanDataIterator scan_data_iterator;
    std::optional<PartitionPruner> pruner;

    const std::string data_prefix;
    DB::NamesAndTypesList expression_schema;
    DB::Names partition_columns;
    const DB::ObjectStoragePtr object_storage;
    const DB::IDataLakeMetadata::FileProgressCallback callback;
    const size_t list_batch_size;
    const LoggerPtr log;

    std::exception_ptr scan_exception;

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


TableSnapshot::TableSnapshot(
    KernelHelperPtr helper_,
    DB::ObjectStoragePtr object_storage_,
    LoggerPtr log_)
    : helper(helper_)
    , object_storage(object_storage_)
    , log(log_)
{
}

size_t TableSnapshot::getVersion() const
{
    initSnapshot();
    return snapshot_version;
}

bool TableSnapshot::update()
{
    if (!snapshot.get())
    {
        /// Snapshot is not yet created,
        /// so next attempt to create it would return the latest snapshot.
        return false;
    }
    initSnapshotImpl();
    return true;
}

void TableSnapshot::initSnapshot() const
{
    if (snapshot.get())
        return;
    initSnapshotImpl();
}

void TableSnapshot::initSnapshotImpl() const
{
    LOG_TEST(log, "Initializing snapshot");

    auto * engine_builder = helper->createBuilder();
    engine = KernelUtils::unwrapResult(ffi::builder_build(engine_builder), "builder_build");
    snapshot = KernelUtils::unwrapResult(
        ffi::snapshot(KernelUtils::toDeltaString(helper->getTableLocation()), engine.get()), "snapshot");
    snapshot_version = ffi::version(snapshot.get());
    LOG_TRACE(log, "Snapshot version: {}", snapshot_version);

    scan = KernelUtils::unwrapResult(ffi::scan(snapshot.get(), engine.get(), /* predicate */{}), "scan");

    LOG_TRACE(log, "Initialized scan state");

    std::tie(table_schema, physical_names_map) = getTableSchemaFromSnapshot(snapshot.get());
    LOG_TRACE(log, "Table logical schema: {}", fmt::join(table_schema.getNames(), ", "));

    read_schema = getReadSchemaFromSnapshot(scan.get());
    LOG_TRACE(log, "Table read schema: {}", fmt::join(read_schema.getNames(), ", "));

    partition_columns = getPartitionColumnsFromSnapshot(snapshot.get());
    LOG_TRACE(log, "Partition columns: {}", fmt::join(partition_columns, ", "));
}

DB::ObjectIterator TableSnapshot::iterate(
    const DB::ActionsDAG * filter_dag,
    DB::IDataLakeMetadata::FileProgressCallback callback,
    size_t list_batch_size)
{
    initSnapshot();
    return std::make_shared<TableSnapshot::Iterator>(
        engine,
        snapshot,
        scan,
        helper->getDataPath(),
        getTableSchema(),
        getPhysicalNamesMap(),
        getPartitionColumns(),
        object_storage,
        filter_dag,
        callback,
        list_batch_size,
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
