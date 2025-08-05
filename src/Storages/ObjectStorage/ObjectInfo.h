#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB
{

struct ObjectInfo : RelativePathWithMetadata
{
    std::optional<DataLakeObjectMetadata> data_lake_metadata = std::nullopt;

    explicit ObjectInfo(String relative_path_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : RelativePathWithMetadata(std::move(relative_path_), std::move(metadata_))
    {}
};

using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
using ObjectInfos = std::vector<ObjectInfoPtr>;

}
