#pragma once

#include <Common/ObjectStorageKey.h>

namespace DB
{

class IObjectStorageKeyGenerator
{
public:
    virtual ~IObjectStorageKeyGenerator() = default;

    /// Generates an object storage key based on a path in the virtual filesystem.
    /// @returns Keys that should be used to save data.
    virtual ObjectStorageKey generate() const = 0;
};

using ObjectStorageKeyGeneratorPtr = std::shared_ptr<IObjectStorageKeyGenerator>;

ObjectStorageKeyGeneratorPtr createObjectStorageKeyGeneratorByPrefix(String key_prefix);
ObjectStorageKeyGeneratorPtr createObjectStorageKeyGeneratorByTemplate(String key_template);

}
