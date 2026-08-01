#include <Parsers/Mongo/Utils.h>

#include <optional>
#include <string>

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

std::pair<const char *, const char *> getSettingsSubstring(const char * begin, const char * end)
{
    const char * position_start = findKth<'('>(begin, end, 1);
    if (position_start == end)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: can not find settings in query");
    }

    /// The parenthesis that closes the argument list, and not the first one in the text: a
    /// parenthesis inside a string literal - a regular expression such as `(?:www\.)?` for
    /// instance - is part of the argument, and so is a parenthesis of a nested call.
    size_t depth = 0;
    bool inside_string = false;
    char quote = 0;
    for (const char * position = position_start; position != end; ++position)
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
                return {position_start + 1, position};
        }
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: can not find settings in your query ");
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
    for (size_t i = 0; i < size_str - pattern.size() + 1; ++i)
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
    while (begin[start_position] != ')')
    {
        if (begin[start_position] < '0' || begin[start_position] > '9')
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : pattern {} should contain only numbers", pattern);
        }
        str_representation.push_back(begin[start_position]);
        ++start_position;
    }
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
    while (begin[start_position] != ')')
    {
        result.push_back(begin[start_position]);
        ++start_position;
    }
    return result;
}

}

}
