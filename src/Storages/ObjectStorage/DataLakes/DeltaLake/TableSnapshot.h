#include <Core/Types.h>
#include <IO/S3/URI.h>
#include <Common/Logger.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include "KernelPointerWrapper.h"
#include "KernelHelper.h"
#include <boost/noncopyable.hpp>
#include "delta_kernel_ffi.hpp"

namespace DeltaLake
{

class TableSnapshot
{
public:
    using ConfigurationWeakPtr = DB::StorageObjectStorage::ConfigurationObserverPtr;

    explicit TableSnapshot(KernelHelperPtr helper_, LoggerPtr log_);

    DB::ObjectIterator iterate();

    const DB::NamesAndTypesList & getSchema();

private:
    class Iterator;
    using KernelExternEngine = TemplatedKernelPointerWrapper<ffi::SharedExternEngine, ffi::free_engine>;
    using KernelSnapshot = TemplatedKernelPointerWrapper<ffi::SharedSnapshot, ffi::free_snapshot>;

    const KernelHelperPtr helper;
    const LoggerPtr log;

    KernelExternEngine engine;
    KernelSnapshot snapshot;
    size_t snapshot_version;
    std::optional<DB::NamesAndTypesList> schema;

    void initSnapshot();
    ffi::SharedSnapshot * getSnapshot();
};

}
