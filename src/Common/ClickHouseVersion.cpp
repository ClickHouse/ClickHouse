#include <Common/ClickHouseVersion.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <boost/algorithm/string.hpp>

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
    if (split.empty())
        throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};

    /// A version is 2 to 4 numeric components, optionally followed by a build flavour suffix
    /// (e.g. "26.1.3.20001.altinityantalya"), which is only valid after a full 4-component version.
    if (split.back() == "altinityantalya" || split.back() == "altinitystable")
    {
        suffix = split.back();
        split.pop_back();
        if (split.size() != 4)
            throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};
    }
    else if (split.size() < 2 || split.size() > 4)
        throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};

    components.reserve(split.size());
    for (const auto & token : split)
    {
        size_t component = 0;
        ReadBufferFromString buf(token);
        if (token.empty() || !tryReadIntText(component, buf) || !buf.eof())
            throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};
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
