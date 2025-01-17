#include "config.h"

#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
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
        LoggerPtr log_)
        : scan(KernelUtils::unwrapResult(ffi::scan(snapshot_.get(), engine_.get(), {}), "scan"))
        , scan_data_iterator(KernelUtils::unwrapResult(
            ffi::kernel_scan_data_init(engine_.get(), scan.get()),
            "kernel_scan_data_init"))
        , data_prefix(data_prefix_)
        , log(log_)
    {
    }

    size_t estimatedKeysCount() override { return 0; }

    DB::ObjectInfoPtr next(size_t) override
    {
        auto have_scan_data_res = KernelUtils::unwrapResult(
            ffi::kernel_scan_data_next(scan_data_iterator.get(), this, visitData),
            "kernel_scan_data_next");

        if (!have_scan_data_res)
            return nullptr;

        return data_files.back();
    }

    static void visitData(
        void * engine_context,
        ffi::ExclusiveEngineData * engine_data,
        const struct ffi::KernelBoolSlice selection_vec)
    {
        ffi::visit_scan_data(engine_data, selection_vec, engine_context, Iterator::scanCallback);
    }

    static void scanCallback(
        ffi::NullableCvoid engine_context,
        struct ffi::KernelStringSlice path,
        int64_t size,
        const ffi::Stats * /* stats */,
        const ffi::DvInfo * /* dv_info */,
        const struct ffi::CStringMap * /* partition_map */)
    {
        auto * context = static_cast<TableSnapshot::Iterator *>(engine_context);

        std::string path_string = fs::path(context->data_prefix) / KernelUtils::fromDeltaString(path);
        context->data_files.push_back(std::make_shared<DB::ObjectInfo>(path_string, size));

        LOG_TEST(context->log, "Scanned file: {}, size: {}", path_string, size);
    }

private:
    using KernelScan = TemplatedKernelPointerWrapper<ffi::SharedScan, ffi::free_scan>;
    using KernelScanDataIterator = TemplatedKernelPointerWrapper<ffi::SharedScanDataIterator, ffi::free_kernel_scan_data>;
    // using KernelGlobalScanState = TemplatedKernelPointerWrapper<ffi::SharedGlobalScanState, ffi::free_global_scan_state>;

    const KernelScan scan;
    const KernelScanDataIterator scan_data_iterator;
    const std::string data_prefix;
    std::vector<DB::ObjectInfoPtr> data_files;
    LoggerPtr log;
};


TableSnapshot::TableSnapshot(KernelHelperPtr helper_, LoggerPtr log_)
    : helper(helper_)
    , log(log_)
{
    auto * engine_builder = helper->createBuilder();
    engine = KernelUtils::unwrapResult(ffi::builder_build(engine_builder), "builder_build");
    snapshot = KernelUtils::unwrapResult(ffi::snapshot(KernelUtils::toDeltaString(helper->getTablePath()), engine.get()), "snapshot");
    snapshot_version = ffi::version(snapshot.get());
}

DB::ObjectIterator TableSnapshot::iterate()
{
    return std::make_shared<TableSnapshot::Iterator>(engine, snapshot, helper->getDataPath(), log);
}

DB::NamesAndTypesList TableSnapshot::getSchema()
{
    return getSchemaFromSnapshot(snapshot.get());
}

}
