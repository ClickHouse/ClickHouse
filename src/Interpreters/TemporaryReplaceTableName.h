#pragma once

#include <base/types.h>

#include <optional>

namespace DB
{
    struct TemporaryReplaceTableName
    {
        String name_hash;
        String random_suffix;

        String toString() const;

        static std::optional<TemporaryReplaceTableName> fromString(const String & str);

        static String calculateHash(String database, String table);
    };
}
