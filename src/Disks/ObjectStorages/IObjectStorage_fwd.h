#pragma once
#include <memory>

namespace DB
{

class IObjectStorage;
using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;

class IMetadataStorage;
using MetadataStoragePtr = std::shared_ptr<IMetadataStorage>;

}
