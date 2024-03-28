#pragma once
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

namespace DB
{

using ConfigurationPtr = StorageObjectStorageConfigurationPtr;
using ObjectInfo = RelativePathWithMetadata;
using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
using ObjectInfos = std::vector<ObjectInfoPtr>;

}
