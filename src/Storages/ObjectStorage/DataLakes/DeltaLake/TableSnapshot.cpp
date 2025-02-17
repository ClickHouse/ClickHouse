#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/ObjectInfoWithPartitionColumns.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/parseFieldFromString.h>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Core/Field.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include "getSchemaFromSnapshot.h"
#include "KernelUtils.h"

namespace fs = std::filesystem;

namespace DeltaLake
{

class TableSnapshot::Iterator final : public DB::IObjectIterator
{
public:
    Iterator(
        const KernelExternEngine & engine_,
        const KernelSnapshot & snapshot_,
        const std::string & data_prefix_,
        const DB::NamesAndTypesList & schema_,
        LoggerPtr log_)
        : scan(KernelUtils::unwrapResult(ffi::scan(snapshot_.get(), engine_.get(), /* predicate */{}), "scan"))
        , scan_data_iterator(KernelUtils::unwrapResult(
            ffi::kernel_scan_data_init(engine_.get(), scan.get()),
            "kernel_scan_data_init"))
        , data_prefix(data_prefix_)
        , schema(schema_)
        , log(log_)
    {
    }

    size_t estimatedKeysCount() override { return 0; } /// TODO

    DB::ObjectInfoPtr next(size_t) override
    {
        if (data_files.empty())
        {
            auto have_scan_data_res = KernelUtils::unwrapResult(
                ffi::kernel_scan_data_next(scan_data_iterator.get(), this, visitData),
                "kernel_scan_data_next");

            if (!have_scan_data_res)
                return nullptr;
        }

        chassert(!data_files.empty());
        auto object = data_files.front();
        data_files.pop_front();
        return object;
    }

    static void visitData(
        void * engine_context,
        ffi::ExclusiveEngineData * engine_data,
        const struct ffi::KernelBoolSlice selection_vec,
        const ffi::CTransforms *transforms)
    {
        ffi::visit_scan_data(engine_data, selection_vec, transforms, engine_context, Iterator::scanCallback);
    }

    static void scanCallback(
        ffi::NullableCvoid engine_context,
        struct ffi::KernelStringSlice path,
        int64_t size,
        const ffi::Stats * stats,
        const ffi::DvInfo * /* dv_info */,
        const struct ffi::CStringMap * partition_map)
    {
        auto * context = static_cast<TableSnapshot::Iterator *>(engine_context);

        std::string path_string = fs::path(context->data_prefix) / KernelUtils::fromDeltaString(path);

        DB::ObjectInfoWithParitionColumns::PartitionColumnsInfo partitions_info;
        for (const auto & name_and_type : context->schema)
        {
            auto * raw_value = ffi::get_from_string_map(partition_map, KernelUtils::toDeltaString(name_and_type.name), KernelUtils::allocateString);
            auto value = std::unique_ptr<std::string>(static_cast<std::string *>(raw_value));
            if (value)
                partitions_info.emplace_back(name_and_type, DB::parseFieldFromString(*value, name_and_type.type));
        }

        DB::ObjectInfoPtr object;
        if (partitions_info.empty())
            object = std::make_shared<DB::ObjectInfo>(path_string, size);
        else
            object = std::make_shared<DB::ObjectInfoWithParitionColumns>(std::move(partitions_info), path_string, size);

        context->data_files.push_back(object);
        LOG_TEST(
            context->log,
            "Scanned file: {}, size: {}, num records: {}, partition columns: {}",
            path_string, size, stats->num_records, partitions_info.size());
    }

private:
    using KernelScan = TemplatedKernelPointerWrapper<ffi::SharedScan, ffi::free_scan>;
    using KernelScanDataIterator = TemplatedKernelPointerWrapper<ffi::SharedScanDataIterator, ffi::free_kernel_scan_data>;

    const KernelScan scan;
    const KernelScanDataIterator scan_data_iterator;
    const std::string data_prefix;
    const DB::NamesAndTypesList & schema;
    const LoggerPtr log;

    std::deque<DB::ObjectInfoPtr> data_files;
};


TableSnapshot::TableSnapshot(KernelHelperPtr helper_, LoggerPtr log_)
    : helper(helper_)
    , log(log_)
{
}

void TableSnapshot::initSnapshot()
{
    if (snapshot.get())
        return;

    auto * engine_builder = helper->createBuilder();
    engine = KernelUtils::unwrapResult(ffi::builder_build(engine_builder), "builder_build");
    snapshot = KernelUtils::unwrapResult(ffi::snapshot(KernelUtils::toDeltaString(helper->getTablePath()), engine.get()), "snapshot");
    snapshot_version = ffi::version(snapshot.get());

    LOG_TEST(log, "Snapshot version: {}", snapshot_version);
}

ffi::SharedSnapshot * TableSnapshot::getSnapshot()
{
    if (!snapshot.get())
        initSnapshot();
    return snapshot.get();
}

DB::ObjectIterator TableSnapshot::iterate()
{
    initSnapshot();
    return std::make_shared<TableSnapshot::Iterator>(engine, snapshot, helper->getDataPath(), getReadSchema(), log);
}

const DB::NamesAndTypesList & TableSnapshot::getTableSchema()
{
    if (!table_schema.has_value())
    {
        table_schema = getTableSchemaFromSnapshot(getSnapshot());
        LOG_TEST(log, "Fetched table schema: {}", table_schema->toString());
    }
    return table_schema.value();
}

const DB::NamesAndTypesList & TableSnapshot::getReadSchema()
{
    if (!read_schema.has_value())
    {
        auto * current_snapshot = getSnapshot();
        chassert(engine.get());
        read_schema = getReadSchemaFromSnapshot(current_snapshot, engine.get());
        LOG_TEST(log, "Fetched read schema: {}", read_schema->toString());
    }
    return read_schema.value();
}

}

#endif
