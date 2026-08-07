#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>
#include <mutex>
#include "delta_kernel_ffi.hpp"

namespace DeltaLake
{

/// from_version or version_range.
using TableChangesVersionRange = std::pair<size_t, std::optional<size_t>>;

class TableChanges : private DB::WithContext
{
public:
    explicit TableChanges(
        const TableChangesVersionRange & version_range_,
        KernelHelperPtr helper_,
        const DB::Block & header_,
        const std::optional<DB::FormatSettings> & format_settings_,
        const std::string & format_name_,
        DB::ContextPtr context_);

    DB::Chunk next();

    DB::NamesAndTypesList getSchema() const;

    void setFilter(const DB::ActionsDAG * filter_) { filter = filter_->clone(); }

    static TableChangesVersionRange getVersionRange(int64_t start_version, int64_t end_version);

private:
    using KernelExternEngine = KernelPointerWrapper<ffi::SharedExternEngine, ffi::free_engine>;
    using KernelTableChanges = KernelPointerWrapper<ffi::ExclusiveTableChanges, ffi::free_table_changes>;
    using KernelTableChangesScan = KernelPointerWrapper<ffi::SharedTableChangesScan, ffi::free_table_changes_scan>;
    using KernelTableChangesScanIterator = KernelPointerWrapper<ffi::SharedScanTableChangesIterator, ffi::free_scan_table_changes_iter>;

    KernelTableChanges & getTableChanges() const;
    KernelTableChangesScanIterator & getTableChangesScanIterator() const;
    DB::NamesAndTypesList getSchemaUnlocked() const;

    const TableChangesVersionRange version_range;
    const KernelHelperPtr helper;
    const DB::Block header;
    const DB::FormatSettings format_settings;
    const std::string format_name;
    std::optional<DB::ActionsDAG> filter;
    const LoggerPtr log;

    mutable std::mutex mutex;
    mutable KernelExternEngine engine;
    mutable KernelTableChanges table_changes;
    mutable KernelTableChangesScan table_changes_scan;
    mutable KernelTableChangesScanIterator table_changes_scan_iterator;
    mutable std::optional<DB::NamesAndTypesList> schema;
};
using TableChangesPtr = std::shared_ptr<TableChanges>;

}

#endif
