#pragma once

#include <Common/ObjectStorageKey.h>

namespace DB
{

class IObjectStorageKeyGenerator
{
public:
    virtual ~IObjectStorageKeyGenerator() = default;

    /// Generates an object storage key based on a path in the virtual filesystem.
    /// @param path - Path in the virtual filesystem.
    /// @returns Keys that should be used to save data.
    virtual ObjectStorageKey generate(const String & path) const = 0;

    /// Returns whether this generator uses a pseudorandom number generator to generate object storage keys.
    virtual bool isRandom() const = 0;
};

using ObjectStorageKeyGeneratorPtr = std::shared_ptr<IObjectStorageKeyGenerator>;

ObjectStorageKeyGeneratorPtr createObjectStorageKeyGeneratorAsIsWithPrefix(String key_prefix);
ObjectStorageKeyGeneratorPtr createObjectStorageKeyGeneratorByPrefix(String key_prefix);
ObjectStorageKeyGeneratorPtr createObjectStorageKeyGeneratorByTemplate(String key_template);

}
