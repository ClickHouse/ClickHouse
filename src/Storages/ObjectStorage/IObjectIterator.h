#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB
{
using ObjectInfo = RelativePathWithMetadata;
using ObjectInfoPtr = std::shared_ptr<RelativePathWithMetadata>;

struct IObjectIterator
{
    virtual ~IObjectIterator() = default;
    virtual ObjectInfoPtr next(size_t) = 0;
    virtual size_t estimatedKeysCount() = 0;
};

using ObjectIterator = std::shared_ptr<IObjectIterator>;

}
