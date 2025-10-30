#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>
#include <mutex>
#include "delta_kernel_ffi.hpp"

namespace DeltaLake
{

class TableChanges
{
public:
    explicit TableChanges(const std::pair<size_t, size_t> & version_range_, KernelHelperPtr helper_);
    explicit TableChanges(size_t from_version_, KernelHelperPtr helper_);

    DB::Chunk next();

private:
    using KernelExternEngine = KernelPointerWrapper<ffi::SharedExternEngine, ffi::free_engine>;
    using KernelTableChanges = KernelPointerWrapper<ffi::ExclusiveTableChanges, ffi::free_table_changes>;
    using KernelTableChangesScan = KernelPointerWrapper<ffi::SharedTableChangesScan, ffi::free_table_changes_scan>;
    using KernelTableChangesScanIterator = KernelPointerWrapper<ffi::SharedScanTableChangesIterator, ffi::free_scan_table_changes_iter>;

    void initialize();

    const std::pair<size_t, std::optional<size_t>> version_range;
    const KernelHelperPtr helper;

    KernelExternEngine engine;
    KernelTableChanges table_changes;
    KernelTableChangesScan table_changes_scan;
    KernelTableChangesScanIterator table_changes_scan_iterator;

    bool initialized = false;
    std::mutex mutex;
};
using TableChangesPtr = std::shared_ptr<TableChanges>;

}

#endif
