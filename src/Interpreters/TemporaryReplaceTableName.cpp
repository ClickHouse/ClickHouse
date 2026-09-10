#include <Interpreters/TemporaryReplaceTableName.h>

#include <Common/re2.h>
#include <Common/SipHash.h>

#include <base/hex.h>
#include <fmt/core.h>


namespace DB
{
    String TemporaryReplaceTableName::toString() const
    {
        return fmt::format("_tmp_replace_{}_{}", name_hash, random_suffix);
    }

    std::optional<TemporaryReplaceTableName> TemporaryReplaceTableName::fromString(const String & str)
    {
        static const re2::RE2 pattern(R"(^_tmp_replace_(\w+)_(\w+)$)");
        TemporaryReplaceTableName result;
        if (RE2::FullMatch(str, pattern, &result.name_hash, &result.random_suffix))
        {
            return result;
        }
        return std::nullopt;
    }

    String TemporaryReplaceTableName::calculateHash(String database, String table)
    {
        return getHexUIntLowercase(sipHash64(std::move(database) + std::move(table)));
    }
}
