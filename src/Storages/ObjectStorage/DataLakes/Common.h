#pragma once
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class IObjectStorage;

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const StorageObjectStorage::Configuration & configuration,
    const String & prefix, const String & suffix);

}
