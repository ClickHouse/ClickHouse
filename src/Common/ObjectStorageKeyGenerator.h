#pragma once

#include <Common/ObjectStorageKey.h>

namespace DB
{

class IObjectStorageKeysGenerator
{
public:
    virtual ~IObjectStorageKeysGenerator() = default;

    /// Generates an object storage key based on a path in the virtual filesystem.
    /// @param path - Path in the virtual filesystem.
    /// @returns Keys that should be used to save data.
    virtual ObjectStorageKey generate(const String & path) const = 0;

    /// Returns whether this generator uses a pseudorandom number generator to generate object storage keys.
    virtual bool isRandom() const = 0;
};

using ObjectStorageKeysGeneratorPtr = std::shared_ptr<IObjectStorageKeysGenerator>;

ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorAsIsWithPrefix(String key_prefix);
ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorByPrefix(String key_prefix);
ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorByTemplate(String key_template);

}
