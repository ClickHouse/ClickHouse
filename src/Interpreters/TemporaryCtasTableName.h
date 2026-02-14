#pragma once

#include <base/types.h>

#include <optional>

namespace DB
{
    struct TemporaryCtasTableName
    {
        String name_hash;
        String random_suffix;

        String toString() const;

        static std::optional<TemporaryCtasTableName> fromString(const String & str);

        static String calculateHash(String database, String table);
    };
}
