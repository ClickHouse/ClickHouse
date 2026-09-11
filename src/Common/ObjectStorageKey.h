#pragma once

#include <base/types.h>

#include <memory>

namespace DB
{
    /// Joins two parts of an object storage key with a single `/`.
///
/// Spelled out rather than delegated to `std::filesystem::path`, which is what this used to be:
/// an object storage key is not a filesystem path, and `path::operator/` appends the host's
/// preferred separator - a backslash on Windows, which is an ordinary character in a key.
String appendObjectStorageKeySegment(const String & prefix, const String & suffix);

struct ObjectStorageKey
    {
        ObjectStorageKey() = default;

        bool hasPrefix() const { return is_relative; }
        const String & getPrefix() const;
        const String & getSuffix() const;
        const String & serialize() const;

        static ObjectStorageKey createAsRelative(String prefix_, String suffix_);
        static ObjectStorageKey createAsRelative(String key_);
        static ObjectStorageKey createAsAbsolute(String key_);

    private:
        String prefix;
        String suffix;
        String key;
        bool is_relative = false;
    };

}
