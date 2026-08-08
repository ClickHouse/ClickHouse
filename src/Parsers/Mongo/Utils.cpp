#include <Parsers/Mongo/Utils.h>

#include <optional>
#include <string>

#include <Common/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Mongo
{

std::pair<const char *, const char *> getMetadataSubstring(const char * begin, const char * end)
{
    const char * position = findKth<'('>(begin, end, 1);
    if (position == end)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: can not parse metadata in query");
    }
    return {begin, position};
}

const char * findMatchingParenthesis(const char * begin, const char * end)
{
    /// The parenthesis that closes the one at `begin`, and not the first one in the text: a
    /// parenthesis inside a string literal - a regular expression such as `(?:www\.)?` for
    /// instance - is part of the argument, and so is a parenthesis of a nested call.
    size_t depth = 0;
    bool inside_string = false;
    char quote = 0;
    for (const char * position = begin; position != end; ++position)
    {
        if (inside_string)
        {
            if (*position == '\\' && position + 1 != end)
                ++position;
            else if (*position == quote)
                inside_string = false;
            continue;
        }

        if (*position == '"' || *position == '\'')
        {
            inside_string = true;
            quote = *position;
        }
        else if (*position == '(')
            ++depth;
        else if (*position == ')')
        {
            --depth;
            if (depth == 0)
                return position;
        }
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: can not find settings in your query ");
}

std::pair<const char *, const char *> getSettingsSubstring(const char * begin, const char * end)
{
    const char * position_start = findKth<'('>(begin, end, 1);
    if (position_start == end)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: can not find settings in query");
    }

    const char * position_end = findMatchingParenthesis(position_start, end);
    return {position_start + 1, position_end};
}

const char * findStatementEnd(const char * begin, const char * end)
{
    bool inside_string = false;
    char quote = 0;
    for (const char * position = begin; position != end; ++position)
    {
        if (inside_string)
        {
            if (*position == '\\' && position + 1 != end)
                ++position;
            else if (*position == quote)
                inside_string = false;
        }
        else if (*position == '"' || *position == '\'')
        {
            inside_string = true;
            quote = *position;
        }
        else if (*position == ';')
            return position;
    }
    return end;
}

std::optional<UInt32> decimalScaleOfNumberDecimal(std::string_view text)
{
    /// The precision of `Decimal128`.
    static constexpr Int64 max_precision = 38;

    size_t pos = 0;
    if (pos < text.size() && (text[pos] == '+' || text[pos] == '-'))
        ++pos;

    const size_t integer_begin = pos;
    while (pos < text.size() && isNumericASCII(text[pos]))
        ++pos;
    const size_t integer_end = pos;

    size_t fractional_begin = pos;
    size_t fractional_end = pos;
    if (pos < text.size() && text[pos] == '.')
    {
        ++pos;
        fractional_begin = pos;
        while (pos < text.size() && isNumericASCII(text[pos]))
            ++pos;
        fractional_end = pos;
    }

    if (integer_end == integer_begin && fractional_end == fractional_begin)
        return std::nullopt;

    Int64 exponent = 0;
    if (pos < text.size() && (text[pos] == 'e' || text[pos] == 'E'))
    {
        ++pos;
        bool negative_exponent = false;
        if (pos < text.size() && (text[pos] == '+' || text[pos] == '-'))
            negative_exponent = text[pos++] == '-';
        const size_t exponent_begin = pos;
        while (pos < text.size() && isNumericASCII(text[pos]))
        {
            exponent = exponent * 10 + (text[pos] - '0');
            /// Mongo caps the exponent at four digits; anything this far out cannot fit anyway,
            /// and the early exit keeps the accumulator from overflowing.
            if (exponent > 10000)
                return std::nullopt;
            ++pos;
        }
        if (pos == exponent_begin)
            return std::nullopt;
        if (negative_exponent)
            exponent = -exponent;
    }

    if (pos != text.size())
        return std::nullopt;

    /// The count of significant digits of the coefficient: leading zeros carry no information,
    /// while trailing fractional zeros do - they widen the scale.
    size_t first_significant = integer_begin;
    while (first_significant < integer_end && text[first_significant] == '0')
        ++first_significant;
    Int64 significant = integer_end - first_significant;
    if (significant == 0)
    {
        first_significant = fractional_begin;
        while (first_significant < fractional_end && text[first_significant] == '0')
            ++first_significant;
        significant = fractional_end - first_significant;
    }
    else
        significant += fractional_end - fractional_begin;

    const Int64 fractional_digits = fractional_end - fractional_begin;
    const Int64 scale = std::max<Int64>(0, fractional_digits - exponent);
    /// The value scaled by `10^scale` is an integer of this many digits; it must fit the type.
    const Int64 precision = significant + std::max<Int64>(0, exponent - fractional_digits);
    if (scale > max_precision || precision > max_precision)
        return std::nullopt;
    return static_cast<UInt32>(scale);
}

std::optional<rapidjson::Value>
findField(const rapidjson::Value & value, const std::string & key, rapidjson::Document::AllocatorType & allocator)
{
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
    {
        if (it->name.GetString() == key)
        {
            return copyValue(it->value, allocator);
        }
    }
    return std::nullopt;
}

namespace
{

/** Converts Mongo shell style single quoted string literals into JSON double quoted ones,
  * respecting string boundaries: apostrophes inside double quoted strings are preserved
  * (`{"name": "O'Reilly"}` stays intact), double quotes inside single quoted strings are
  * escaped, and backslash escape sequences are honored in both kinds of strings.
  */
std::string normalizeQuotes(const std::string & input)
{
    enum class State
    {
        Outside,
        InsideDoubleQuoted,
        InsideSingleQuoted,
    };

    std::string result;
    result.reserve(input.size());
    State state = State::Outside;

    for (size_t i = 0; i < input.size(); ++i)
    {
        const char c = input[i];
        switch (state)
        {
            case State::Outside:
            {
                if (c == '"')
                {
                    state = State::InsideDoubleQuoted;
                    result.push_back(c);
                }
                else if (c == '\'')
                {
                    state = State::InsideSingleQuoted;
                    result.push_back('"');
                }
                else
                    result.push_back(c);
                break;
            }
            case State::InsideDoubleQuoted:
            {
                if (c == '\\' && i + 1 < input.size())
                {
                    result.push_back(c);
                    result.push_back(input[++i]);
                }
                else
                {
                    if (c == '"')
                        state = State::Outside;
                    result.push_back(c);
                }
                break;
            }
            case State::InsideSingleQuoted:
            {
                if (c == '\\' && i + 1 < input.size())
                {
                    const char next = input[++i];
                    /// `\'` is an escaped apostrophe in a single quoted string; JSON needs it bare.
                    if (next == '\'')
                        result.push_back('\'');
                    else
                    {
                        result.push_back('\\');
                        result.push_back(next);
                    }
                }
                else if (c == '\'')
                {
                    state = State::Outside;
                    result.push_back('"');
                }
                else if (c == '"')
                    result.append("\\\"");
                else
                    result.push_back(c);
                break;
            }
        }
    }

    /// An unterminated string literal is left as-is; the JSON parser below reports it.
    return result;
}

}

rapidjson::Value parseData(const char * begin, const char * end, rapidjson::Document::AllocatorType & allocator, bool wrap_into_array)
{
    std::string input(begin, end);
    if (wrap_into_array)
        input = "[" + input + "]";
    input = normalizeQuotes(input);

    rapidjson::Document document;

    if (document.Parse(input.data()).HasParseError())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while parsing json in parseData {}", input);
    }
    return copyValue(document, allocator);
}

std::optional<size_t> MongoQueryKeyNameExtractor::findPosition(const char * begin, const char * end)
{
    size_t size_str = end - begin;
    /// The pattern and the parenthesis that must follow it have to fit, and the subtraction below
    /// is unsigned: a text shorter than the pattern would wrap it around into a huge count.
    if (size_str < pattern.size() + 1)
        return std::nullopt;

    for (size_t i = 0; i + pattern.size() < size_str; ++i)
    {
        bool match = true;
        for (size_t j = 0; j < pattern.size(); ++j)
        {
            if (begin[i + j] != pattern[j])
            {
                match = false;
                break;
            }
        }
        if (match)
        {
            if (begin[i + pattern.size()] != '(')
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : after {} should be (", pattern);
            }
            return i + pattern.size() + 1;
        }
    }
    return std::nullopt;
}

std::optional<int> MongoQueryKeyNameExtractor::extractInt(const char * begin, const char * end)
{
    auto maybe_start_position = findPosition(begin, end);
    if (!maybe_start_position)
    {
        return std::nullopt;
    }
    auto start_position = *maybe_start_position;
    std::string str_representation;
    /// The end of the text bounds the walk: an unclosed `(` would otherwise read past it.
    while (begin + start_position != end && begin[start_position] != ')')
    {
        if (begin[start_position] < '0' || begin[start_position] > '9')
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : pattern {} should contain only numbers", pattern);
        }
        str_representation.push_back(begin[start_position]);
        ++start_position;
    }
    if (begin + start_position == end)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : the '{}' is not closed", pattern);
    if (str_representation.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : the '{}' has no argument", pattern);
    return std::stoi(str_representation);
}

std::optional<std::string> MongoQueryKeyNameExtractor::extractString(const char * begin, const char * end)
{
    auto maybe_start_position = findPosition(begin, end);
    if (!maybe_start_position)
    {
        return std::nullopt;
    }
    auto start_position = *maybe_start_position;
    std::string result;
    while (begin + start_position != end && begin[start_position] != ')')
    {
        result.push_back(begin[start_position]);
        ++start_position;
    }
    if (begin + start_position == end)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : the '{}' is not closed", pattern);
    return result;
}

}

}
