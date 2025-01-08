#include "TemporaryReplaceTableName.h"
#include <regex>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>

namespace DB
{
    String TemporaryReplaceTableName::toString() const
    {
        return fmt::format("_tmp_replace_{}_{}", getHexUIntLowercase(name_hash), random_suffix);
    }

    std::optional<TemporaryReplaceTableName> TemporaryReplaceTableName::fromString(const String & str)
    {
        const std::regex pattern("_tmp_replace_(.*)_(.*)");
        std::smatch matches;
        if (std::regex_match(str, matches, pattern) && matches.size() == 3)
        {
            TemporaryReplaceTableName result;
            result.name_hash = matches[1].str();
            result.random_suffix = matches[2].str();
            return result;
        }
        return std::nullopt;
    }
}
