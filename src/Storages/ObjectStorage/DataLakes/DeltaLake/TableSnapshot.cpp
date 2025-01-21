#include "config.h"

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

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_TEXT;
    extern const int DELTA_KERNEL_ERROR;
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
        const struct ffi::KernelBoolSlice selection_vec)
    {
        ffi::visit_scan_data(engine_data, selection_vec, engine_context, Iterator::scanCallback);
    }

    static void * allocateString(const struct ffi::KernelStringSlice slice)
    {
        return new std::string(slice.ptr, slice.len); ///TODO: not good
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
            const auto * value = static_cast<const std::string *>(
                ffi::get_from_map(partition_map, KernelUtils::toDeltaString(name_and_type.name), allocateString));
            if (value)
            {
                const std::string partition_value(*value);
                partitions_info.emplace_back(name_and_type, DB::parseFieldFromString(partition_value, name_and_type.type));

                // LOG_TEST(context->log, "Got partition value {}={}", name_and_type.name, partition_value);
                // delete value;
            }
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
    return std::make_shared<TableSnapshot::Iterator>(engine, snapshot, helper->getDataPath(), getSchema(), log);
}

const DB::NamesAndTypesList & TableSnapshot::getSchema()
{
    if (!schema.has_value())
    {
        schema = getSchemaFromSnapshot(getSnapshot());
        LOG_TEST(log, "Fetched schema: {}", schema->toString());
    }
    return schema.value();
}

}
