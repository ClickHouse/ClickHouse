#pragma once

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/Configuration.h>

namespace DB
{

struct DeltaLakeMetadataParser
{
public:
    DeltaLakeMetadataParser();

    Strings getFiles(
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        ContextPtr context);

private:
    struct Impl;
    std::shared_ptr<Impl> impl;
};

}
