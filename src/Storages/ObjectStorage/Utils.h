#pragma once
#include "StorageObjectStorage.h"

namespace DB
{

class IObjectStorage;

std::optional<std::string> checkAndGetNewFileOnInsertIfNeeded(
    const IObjectStorage & object_storage,
    const StorageObjectStorage::Configuration & configuration,
    const StorageObjectStorage::QuerySettings & settings,
    const std::string & key,
    size_t sequence_number);

void resolveSchemaAndFormat(
    ColumnsDescription & columns,
    std::string & format,
    ObjectStoragePtr object_storage,
    const StorageObjectStorage::ConfigurationPtr & configuration,
    std::optional<FormatSettings> format_settings,
    const ContextPtr & context);

}
