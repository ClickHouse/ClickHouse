#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>

#include <Poco/String.h>
#include <base/find_symbols.h>
#include <boost/algorithm/string/trim.hpp>

namespace DB
{

const NameSet & getSupportedPackedSkipIndexTypes()
{
    static const NameSet supported = {"minmax", "bloom_filter", "tokenbf_v1", "ngrambf_v1"};
    return supported;
}

NameSet parsePackedSkipIndexTypes(const String & list)
{
    NameSet result;
    std::vector<std::string> tokens;
    splitInto<','>(tokens, list);
    for (auto & token : tokens)
    {
        boost::trim(token);
        if (!token.empty())
            result.emplace(Poco::toLower(token));
    }
    return result;
}

}
