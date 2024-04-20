#pragma once
#include <Core/Types.h>

namespace DB
{

class IObjectStorage;
class StorageObjectStorageConfiguration;
struct StorageObjectStorageSettings;

std::optional<std::string> checkAndGetNewFileOnInsertIfNeeded(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const StorageObjectStorageSettings & query_settings,
    const std::string & key,
    size_t sequence_number);

}
