#pragma once

#include <base/types.h>

#include <memory>

namespace DB
{
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
