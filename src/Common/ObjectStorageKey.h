#pragma once

#include <base/types.h>

#include <memory>

namespace DB
{
    struct ObjectStorageKey
    {
        ObjectStorageKey() = default;

        [[nodiscard]] bool hasPrefix() const { return is_relative; }
        [[nodiscard]] const String & getPrefix() const;
        [[nodiscard]] const String & getSuffix() const;
        [[nodiscard]] const String & serialize() const;

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
