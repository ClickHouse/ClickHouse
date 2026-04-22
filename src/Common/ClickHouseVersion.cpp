#include <Common/ClickHouseVersion.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ClickHouseVersion::ClickHouseVersion(std::string_view version)
{
    Strings split;
    boost::split(split, version, [](char c){ return c == '.'; });
    components.reserve(split.size());
    if (split.empty())
        throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};

    for (size_t i = 0; i < split.size(); ++i)
    {
        size_t component;
        ReadBufferFromString buf(split[i]);
        if (!tryReadIntText(component, buf) || !buf.eof())
        {
            /// Non-numeric component (e.g. "altinityantalya"): treat this and remaining parts as suffix.
            /// Valid version must have at least one numeric component (e.g. "26.1.3.20001.altinityantalya").
            if (components.empty())
                throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};
            Strings suffix_parts(split.begin() + i, split.end());
            suffix = boost::algorithm::join(suffix_parts, ".");
            break;
        }
        components.push_back(component);
    }
}

String ClickHouseVersion::toString() const
{
    String result = fmt::format("{}", fmt::join(components, "."));
    if (!suffix.empty())
        result += "." + suffix;
    return result;
}

std::strong_ordering ClickHouseVersion::operator<=>(const ClickHouseVersion & other) const
{
    if (auto cmp = components <=> other.components; cmp != 0)
        return cmp;
    return suffix <=> other.suffix;
}

}
