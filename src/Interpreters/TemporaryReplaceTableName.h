#pragma once

#include <optional>
#include <base/types.h>

namespace DB
{
    struct TemporaryReplaceTableName
    {
        String name_hash;
        String random_suffix;

        String toString() const;

        static std::optional<TemporaryReplaceTableName> fromString(const String & str);
    };
}
