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
    components.reserve(split.size());
    if (split.empty())
        throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};

    for (const auto & split_element : split)
    {
        size_t component;
        ReadBufferFromString buf(split_element);
        if (!tryReadIntText(component, buf) || !buf.eof())
            throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};
        components.push_back(component);
    }
}

String ClickHouseVersion::toString() const
{
    return fmt::format("{}", fmt::join(components, "."));
}

}
