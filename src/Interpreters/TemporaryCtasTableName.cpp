#include <Interpreters/TemporaryCtasTableName.h>

#include <Common/re2.h>
#include <Common/SipHash.h>


#include <base/hex.h>
#include <fmt/core.h>


namespace DB
{
    String TemporaryCtasTableName::toString() const
    {
        return fmt::format("_tmp_ctas_{}_{}", name_hash, random_suffix);
    }

    std::optional<TemporaryCtasTableName> TemporaryCtasTableName::fromString(const String & str)
    {
        static const re2::RE2 pattern(R"(^_tmp_ctas_(\w+)_(\w+)$)");
        TemporaryCtasTableName result;
        if (RE2::FullMatch(str, pattern, &result.name_hash, &result.random_suffix))
        {
            return result;
        }
        return std::nullopt;
    }

    String TemporaryCtasTableName::calculateHash(String database, String table)
    {
        return getHexUIntLowercase(sipHash64(std::move(database) + std::move(table)));
    }
}
