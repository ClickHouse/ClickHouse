#pragma once

#include <Interpreters/Context_fwd.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <Storages/ObjectStorage/StorageObejctStorageConfiguration.h>
#include <Core/Types.h>

namespace DB
{

struct HudiMetadataParser
{
    Strings getFiles(
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration, ContextPtr context);
};

}
