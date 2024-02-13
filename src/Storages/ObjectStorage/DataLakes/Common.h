#pragma once
#include <Core/Types.h>

namespace DB
{

class IObjectStorage;
class StorageObjectStorageConfiguration;

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, const String & suffix);

}
