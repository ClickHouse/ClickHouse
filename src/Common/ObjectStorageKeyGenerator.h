#pragma once

#include <memory>
#include <optional>
#include "ObjectStorageKey.h"

namespace DB
{

class IObjectStorageKeysGenerator
{
public:
    virtual ~IObjectStorageKeysGenerator() = default;

    /// Generates an object storage key based on a path in the virtual filesystem.
    /// @param path         - Path in the virtual filesystem.
    /// @param is_directory - If the path in the virtual filesystem corresponds to a directory.
    /// @param key_prefix   - Optional key prefix for the generated object storage key. If provided, this prefix will be added to the beginning of the generated key.
    virtual ObjectStorageKey generate(const String & path, bool is_directory, const std::optional<String> & key_prefix) const = 0;
};

using ObjectStorageKeysGeneratorPtr = std::shared_ptr<IObjectStorageKeysGenerator>;

ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorAsIsWithPrefix(String key_prefix);
ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorByPrefix(String key_prefix);
ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorByTemplate(String key_template);

}
