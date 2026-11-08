#pragma once

#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <boost/algorithm/string/predicate.hpp>

#include <optional>
#include <string>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
}

/// The separators accepted by `boost::geometry::read_wkt` do not include form feed or vertical tab.
inline bool isWKTSeparator(char ch)
{
    return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r';
}

/// Leave untagged WKT unchanged; validate explicitly dimensioned coordinates before dropping extra ordinates.
/// Geometry structure is left intact for the existing 2D parser to validate.
inline std::optional<std::string> normalizeWKTDimensions(std::string_view wkt)
{
    size_t pos = 0;
    auto skip_separators = [&]
    {
        while (pos < wkt.size() && isWKTSeparator(wkt[pos]))
            ++pos;
    };
    skip_separators();
    while (pos < wkt.size() && !isWKTSeparator(wkt[pos]) && wkt[pos] != '(')
        ++pos;
    skip_separators();
    const size_t tag_start = pos;
    while (pos < wkt.size() && !isWKTSeparator(wkt[pos]) && wkt[pos] != '(')
        ++pos;
    const auto tag = wkt.substr(tag_start, pos - tag_start);
    size_t dimensions = 0;
    if (boost::iequals(tag, "ZM"))
        dimensions = 4;
    else if (boost::iequals(tag, "Z") || boost::iequals(tag, "M"))
        dimensions = 3;
    else
        return std::nullopt;

    std::string result(wkt.substr(0, tag_start));
    skip_separators();
    auto body = wkt.substr(pos);
    while (!body.empty() && isWKTSeparator(body.back()))
        body.remove_suffix(1);
    if (boost::iequals(body, "EMPTY"))
    {
        result.append(wkt.substr(pos));
        return result;
    }

    while (pos < wkt.size())
    {
        const char ch = wkt[pos];
        if (isWKTSeparator(ch) || ch == '(' || ch == ')' || ch == ',')
        {
            result.push_back(ch);
            ++pos;
            continue;
        }

        const size_t point_start = pos;
        size_t xy_end = pos;
        for (size_t ordinate = 0; ordinate < dimensions; ++ordinate)
        {
            const size_t number_start = pos;
            while (pos < wkt.size() && !isWKTSeparator(wkt[pos]) && wkt[pos] != '(' && wkt[pos] != ')' && wkt[pos] != ',')
                ++pos;
            ReadBufferFromString number(wkt.substr(number_start, pos - number_start));
            Float64 value = 0;
            if (!tryReadFloatTextPrecise(value, number) || !number.eof())
                throw Exception(
                    ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse WKT coordinate: {}", wkt.substr(number_start, pos - number_start));
            if (ordinate == 1)
                xy_end = pos;
            const size_t number_end = pos;
            skip_separators();
            if (ordinate + 1 < dimensions && number_end == pos)
                throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Expected {} WKT ordinates separated by whitespace", dimensions);
        }
        if (pos < wkt.size() && wkt[pos] != ',' && wkt[pos] != ')')
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Expected {} WKT ordinates", dimensions);
        result.append(wkt.substr(point_start, xy_end - point_start));
    }
    return result;
}

}
