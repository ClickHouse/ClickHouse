#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <base/types.h>

#include <optional>
#include <string>

namespace DeltaLake
{

class IKernelHelper;

struct DeletionVectorDescriptor
{
    char storage_type = 0;
    std::string path_or_inline_dv;
    std::optional<Int32> offset;
    Int32 size_in_bytes = 0;
    Int64 cardinality = 0;
};

std::string decodeZ85(std::string_view encoded);

std::string getDeletionVectorRelativePath(const DeletionVectorDescriptor & descriptor);

DB::DataLakeObjectMetadata::ExcludedRowsPtr readDeletionVector(
    const DeletionVectorDescriptor & descriptor,
    const IKernelHelper & helper,
    const DB::ObjectStoragePtr & object_storage,
    const DB::ContextPtr & context);

}

#endif
