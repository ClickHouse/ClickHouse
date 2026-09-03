#include <Common/parseRemoteDescription.h>
#include <Common/Exception.h>
#include <Common/checkStackSize.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

[[noreturn]] void throwTooManyAddresses(const String & func_name)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}': first argument generates too many result addresses", func_name);
}

/// Parse number from substring
bool parseNumber(const String & description, size_t l, size_t r, size_t & res)
{
    res = 0;
    for (size_t pos = l; pos < r; ++pos)
    {
        if (!isNumericASCII(description[pos]))
            return false;
        res = res * 10 + description[pos] - '0';
        if (static_cast<double>(res) > 1e15)
            return false;
    }
    return true;
}

/// `lhs * rhs`, or `std::nullopt` when it does not fit into `UInt64`.
std::optional<UInt64> multiply(std::optional<UInt64> lhs, UInt64 rhs)
{
    if (!lhs)
        return {};
    if (rhs != 0 && *lhs > std::numeric_limits<UInt64>::max() / rhs)
        return {};
    return *lhs * rhs;
}

/// `lhs + rhs`, or `std::nullopt` when it does not fit into `UInt64`.
std::optional<UInt64> add(std::optional<UInt64> lhs, std::optional<UInt64> rhs)
{
    if (!lhs || !rhs)
        return {};
    if (*lhs > std::numeric_limits<UInt64>::max() - *rhs)
        return {};
    return *lhs + *rhs;
}

}


UInt64 RemoteDescriptionGenerator::Factor::size() const
{
    return is_range ? range_end - range_begin + 1 : alternatives.size();
}

void RemoteDescriptionGenerator::Factor::appendElementTo(String & out, UInt64 index) const
{
    if (!is_range)
    {
        out += alternatives[index];
        return;
    }

    const String number = toString<UInt64>(range_begin + index);
    for (size_t i = number.size(); i < pad_width; ++i)
        out += '0';
    out += number;
}


RemoteDescriptionGenerator::RemoteDescriptionGenerator(
    const String & description, size_t l, size_t r, char separator, size_t max_addresses_, const String & func_name_)
    : max_addresses(max_addresses_)
    , func_name(func_name_)
{
    /// Groups holding the separator are parsed recursively, and `max_addresses` bounds the number
    /// of generated addresses, not the nesting depth: `{{{{...,...}}}}` recurses once per level.
    checkStackSize();

    /// An empty substring means a set of an empty string
    if (l >= r)
    {
        Factor factor;
        factor.alternatives.emplace_back();
        segments.push_back(Segment{{std::move(factor)}});
        total_count = 1;
        startSegment();
        return;
    }

    segments.emplace_back();

    /// Consecutive ordinary characters collapse into a single factor. Appending them one by one to
    /// every address generated so far is what used to make the parsing quadratic in the description
    /// length.
    String literal;
    auto flush_literal = [&]
    {
        if (literal.empty())
            return;
        Factor factor;
        factor.alternatives.push_back(std::move(literal));
        literal.clear();
        segments.back().factors.push_back(std::move(factor));
    };

    for (size_t i = l; i < r; ++i)
    {
        /// Either the numeric interval (8..10) or equivalent expression in brackets
        if (description[i] == '{')
        {
            ssize_t cnt = 1;
            ssize_t last_dot = -1; /// The rightmost pair of points, remember the index of the right of the two
            size_t m = 0;
            bool have_splitter = false;

            /// Look for the corresponding closing bracket
            for (m = i + 1; m < r; ++m)
            {
                if (description[m] == '{')
                    ++cnt;
                if (description[m] == '}')
                    --cnt;
                if (description[m] == '.' && description[m-1] == '.')
                    last_dot = m;
                if (description[m] == separator)
                    have_splitter = true;
                if (cnt == 0)
                    break;
            }
            if (cnt != 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}': incorrect brace sequence in first argument", func_name);

            Factor factor;

            /// The presence of a dot - numeric interval
            if (last_dot != -1)
            {
                size_t left = 0;
                size_t right = 0;
                if (description[last_dot - 1] != '.')
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Table function '{}': incorrect argument in braces (only one dot): {}",
                        func_name,
                        description.substr(i, m - i + 1));
                if (!parseNumber(description, i + 1, last_dot - 1, left))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Table function '{}': "
                        "incorrect argument in braces (Incorrect left number): {}",
                        func_name,
                        description.substr(i, m - i + 1));
                if (!parseNumber(description, last_dot + 1, m, right))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Table function '{}': "
                        "incorrect argument in braces (Incorrect right number): {}",
                        func_name,
                        description.substr(i, m - i + 1));
                if (left > right)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Table function '{}': "
                        "incorrect argument in braces (left number is greater then right): {}",
                        func_name,
                        description.substr(i, m - i + 1));

                factor.is_range = true;
                factor.range_begin = left;
                factor.range_end = right;
                /// If the left and right borders have equal numbers, then you must add leading zeros.
                if (last_dot - 1 - (i + 1) == m - (last_dot + 1))
                    factor.pad_width = last_dot - 1 - (i + 1);
            }
            else if (have_splitter)
            {
                /// A group with the current separator inside is a set of alternatives, and the direct
                /// product needs all of them up front. It cannot contain a numeric interval - a `..`
                /// anywhere inside braces takes the branch above - so this only materializes literal
                /// text, but keep it bounded all the same.
                RemoteDescriptionGenerator nested(description, i + 1, m, separator, max_addresses, func_name);
                String alternative;
                while (nested.next(alternative))
                    factor.alternatives.push_back(alternative);
            }
            else
            {
                /// Otherwise just copy, spawn will occur when you call with the correct delimiter
                factor.alternatives.push_back(description.substr(i, m - i + 1));
            }

            flush_literal();
            /// An empty group, as in `a{,}b`, contributes nothing rather than making the product empty.
            if (factor.size() != 0)
                segments.back().factors.push_back(std::move(factor));
            i = m;
        }
        else if (description[i] == separator)
        {
            flush_literal();
            segments.emplace_back();
        }
        else
        {
            literal += description[i];
        }
    }
    flush_literal();

    total_count = 0;
    for (const auto & segment : segments)
    {
        if (segment.factors.empty())
            continue;

        std::optional<UInt64> count = 1;
        for (const auto & factor : segment.factors)
            count = multiply(count, factor.size());
        total_count = add(total_count, count);
    }

    startSegment();
}

void RemoteDescriptionGenerator::startSegment()
{
    while (segment_index < segments.size() && segments[segment_index].factors.empty())
        ++segment_index;

    if (segment_index == segments.size())
    {
        finished = true;
        return;
    }

    digits.assign(segments[segment_index].factors.size(), 0);
}

bool RemoteDescriptionGenerator::next(String & out)
{
    if (finished)
        return false;

    if (generated == max_addresses)
        throwTooManyAddresses(func_name);
    ++generated;

    const auto & factors = segments[segment_index].factors;

    out.clear();
    for (size_t i = 0; i < factors.size(); ++i)
        factors[i].appendElementTo(out, digits[i]);

    /// Advance the odometer. The last factor is the least significant digit.
    size_t position = factors.size();
    while (position > 0)
    {
        --position;
        if (++digits[position] < factors[position].size())
            return true;
        digits[position] = 0;
    }

    ++segment_index;
    startSegment();
    return true;
}


std::vector<String> parseRemoteDescription(
    const String & description, size_t l, size_t r, char separator, size_t max_addresses, const String & func_name)
{
    RemoteDescriptionGenerator generator(description, l, r, separator, max_addresses, func_name);

    /// Every address is needed at once, so reject a pattern that generates too many of them before
    /// generating any.
    const auto total_count = generator.totalCount();
    if (!total_count || *total_count > max_addresses)
        throwTooManyAddresses(func_name);

    std::vector<String> res;
    res.reserve(*total_count);

    String address;
    while (generator.next(address))
        res.push_back(address);

    return res;
}


std::vector<std::pair<String, uint16_t>> parseRemoteDescriptionForExternalDatabase(const String & description, size_t max_addresses, UInt16 default_port)
{
    auto addresses = parseRemoteDescription(description, 0, description.size(), '|', max_addresses);
    std::vector<std::pair<String, uint16_t>> result;

    for (const auto & address : addresses)
    {
        const size_t close_bracket = address.rfind(']');
        size_t colon = 0;
        std::string host;
        if (address.length() > 2 && address[0] == '[' && close_bracket != String::npos)
        {
            colon = address.find(':', close_bracket + 1);
            host = address.substr(1, close_bracket - 1);
        }
        else
        {
            colon = address.find(':');
            if (colon == String::npos)
                host = address;
            else
                host = address.substr(0, colon);

        }
        if (colon == String::npos)
        {
            LOG_WARNING(getLogger("ParseRemoteDescription"), "Port is not found for host: {}. Using default port {}", address, default_port);
            result.emplace_back(std::make_pair(host, default_port));
        }
        else
        {
            result.emplace_back(std::make_pair(host, DB::parseFromStringWithoutAssertEOF<UInt16>(address.substr(colon + 1))));
        }
    }

    return result;
}

}
