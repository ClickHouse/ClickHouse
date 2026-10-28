#include <Parsers/Mongo/Utils.h>

#include <charconv>
#include <optional>
#include <string>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Common/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int TOO_DEEP_RECURSION;
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
    /// The whitespace around the namespace is not part of it: a query sent as is to the server
    /// may start with a line break, which must not become a part of the name of the database.
    while (begin != position && isWhitespaceASCII(*begin))
        ++begin;
    const char * metadata_end = position;
    while (metadata_end != begin && isWhitespaceASCII(*(metadata_end - 1)))
        --metadata_end;
    return {begin, metadata_end};
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

bool isInFieldSubtree(std::string_view column, std::string_view path)
{
    return column.starts_with(path) && (column.size() == path.size() || column[path.size()] == '.');
}

String makeFieldSubtreePattern(const std::vector<String> & paths)
{
    String pattern = "^(?:";
    for (size_t i = 0; i < paths.size(); ++i)
    {
        if (i != 0)
            pattern += '|';
        /// Every ASCII character but a letter, a digit and `_` is escaped, so a field name is
        /// matched literally, the same way as `RE2::QuoteMeta` quotes it.
        for (const char symbol : paths[i])
        {
            if (symbol == '\0')
            {
                pattern += "\\x00";
                continue;
            }
            if (!isAlphaNumericASCII(symbol) && symbol != '_' && static_cast<unsigned char>(symbol) < 0x80)
                pattern += '\\';
            pattern += symbol;
        }
    }
    pattern += ")(?:\\.|$)";
    return pattern;
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

namespace
{

/// The depth of the nesting is bounded before anything recursive - the copy below, the translation
/// of a filter or an expression - walks the value, and this check itself recurses no deeper than
/// the bound.
void checkNestingDepth(const rapidjson::Value & value, size_t depth, size_t max_depth)
{
    if (depth > max_depth)
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Maximum parse depth ({}) exceeded by the nesting of the Mongo query. Consider rising max_parser_depth parameter.",
            max_depth);

    if (value.IsObject())
    {
        for (const auto & member : value.GetObject())
            checkNestingDepth(member.value, depth + 1, max_depth);
    }
    else if (value.IsArray())
    {
        for (const auto & element : value.GetArray())
            checkNestingDepth(element, depth + 1, max_depth);
    }
}

}

rapidjson::Value
parseData(const char * begin, const char * end, rapidjson::Document::AllocatorType & allocator, size_t max_depth, bool wrap_into_array)
{
    std::string input(begin, end);
    if (wrap_into_array)
        input = "[" + input + "]";
    input = normalizeQuotes(input);

    rapidjson::Document document;

    /// The iterative parser does not recurse, so a deeply nested input cannot exhaust the stack
    /// before its depth is checked.
    if (document.Parse<rapidjson::kParseIterativeFlag>(input.data()).HasParseError())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while parsing json in parseData {}", input);
    }
    if (max_depth)
        checkNestingDepth(document, 0, max_depth);
    return copyValue(document, allocator);
}

ASTPtr makeMongoSortKey(ASTPtr key)
{
    auto is_array = makeASTFunction(
        "startsWith", makeASTFunction("toTypeName", key->clone()), make_intrusive<ASTLiteral>(Field(String("Array("))));
    auto guard = makeASTFunction(
        "throwIf",
        std::move(is_array),
        make_intrusive<ASTLiteral>(Field(String(
            "A sort on an array is not supported: Mongo sorts an array by its smallest or its largest element, "
            "and ClickHouse compares arrays element by element"))));
    return makeASTFunction("if", std::move(guard), key->clone(), key);
}

ASTPtr makeMongoOrderByElement(ASTPtr key, int direction)
{
    auto element = make_intrusive<ASTOrderByElement>();
    element->children.push_back(makeMongoSortKey(std::move(key)));
    element->direction = direction;
    /// `nulls_direction` equal to `direction` means `NULLS LAST`, the opposite one `NULLS FIRST`.
    element->nulls_direction = -1;
    element->nulls_direction_was_explicitly_specified = true;
    return element;
}

std::optional<size_t> MongoQueryKeyNameExtractor::findPosition(const char * begin, const char * end)
{
    size_t size_str = end - begin;
    /// The pattern and the parenthesis that must follow it have to fit, and the subtraction below
    /// is unsigned: a text shorter than the pattern would wrap it around into a huge count.
    if (size_str < pattern.size() + 1)
        return std::nullopt;

    size_t nesting = 0;
    char quote = 0;
    for (size_t i = 0; i + pattern.size() < size_str; ++i)
    {
        const char c = begin[i];
        if (quote)
        {
            if (c == '\\' && i + 1 < size_str)
            {
                ++i;
                continue;
            }
            if (c == quote)
                quote = 0;
            continue;
        }
        if (c == '\'' || c == '\"')
        {
            quote = c;
            continue;
        }
        if (c == '(')
        {
            ++nesting;
            continue;
        }
        if (c == ')')
        {
            if (nesting)
                --nesting;
            continue;
        }
        if (nesting)
            continue;

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
            if (begin[i + pattern.size()] == '(')
                return i + pattern.size() + 1;
        }
    }
    return std::nullopt;
}

std::optional<Int64> MongoQueryKeyNameExtractor::extractInt(const char * begin, const char * end)
{
    auto maybe_start_position = findPosition(begin, end);
    if (!maybe_start_position)
    {
        return std::nullopt;
    }
    auto start_position = *maybe_start_position;
    /// The end of the text bounds the walk: an unclosed `(` would otherwise read past it.
    const char * value_begin = begin + start_position;
    while (begin + start_position != end && begin[start_position] != ')')
    {
        if ((begin[start_position] < '0' || begin[start_position] > '9') && (start_position != *maybe_start_position || begin[start_position] != '-'))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : pattern {} should contain only numbers", pattern);
        }
        ++start_position;
    }
    if (begin + start_position == end)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : the '{}' is not closed", pattern);
    const char * value_end = begin + start_position;
    if (value_begin == value_end || (value_end == value_begin + 1 && *value_begin == '-'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : the '{}' has no argument", pattern);
    Int64 result{};
    const auto [parse_end, error] = std::from_chars(value_begin, value_end, result);
    if (error != std::errc{} || parse_end != value_end)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect query : the '{}' is out of range", pattern);
    }
    return result;
}

namespace
{

/// Whether `accurateCast` can check the value at all, see `makeRepresentableConstant`.
bool isCheckableConstant(const ASTPtr & ast)
{
    if (ast->as<ASTLiteral>())
        return true;
    /// The Extended JSON wrappers lower into functions of literals - a `CAST` for `$numberDecimal`,
    /// `fromUnixTimestamp64Milli` for `$date` - and arrays nest. An embedded document becomes a
    /// `JSON` value, which has no range to check.
    const auto * function = ast->as<ASTFunction>();
    if (!function || !function->arguments)
        return false;
    if (function->name == "CAST")
    {
        const auto & arguments = function->arguments->children;
        const auto * type = arguments.size() == 2 ? arguments[1]->as<ASTLiteral>() : nullptr;
        if (!type || (type->value.getType() == Field::Types::String && type->value.safeGet<String>() == "JSON"))
            return false;
    }
    for (const auto & argument : function->arguments->children)
        if (!isCheckableConstant(argument))
            return false;
    return true;
}

/// Whether a checkable constant holds a value that `accurateCast` may truncate: a decimal, which loses
/// the digits past the scale of a decimal column or the fraction for an integer one, or a double.
bool hasFractionalConstant(const ASTPtr & ast)
{
    if (const auto * literal = ast->as<ASTLiteral>())
        return literal->value.getType() == Field::Types::Float64;
    const auto * function = ast->as<ASTFunction>();
    if (!function || !function->arguments)
        return false;
    if (function->name == "CAST")
    {
        const auto & arguments = function->arguments->children;
        const auto * type = arguments.size() == 2 ? arguments[1]->as<ASTLiteral>() : nullptr;
        if (type && type->value.getType() == Field::Types::String && type->value.safeGet<String>().starts_with("Decimal"))
            return true;
    }
    for (const auto & argument : function->arguments->children)
        if (hasFractionalConstant(argument))
            return true;
    return false;
}

}

ASTPtr makeRepresentableConstant(ASTPtr value, ASTPtr type, const String & message)
{
    if (!isCheckableConstant(value))
        return value;
    /// A floating point column narrower than a double rounds a value rather than misrepresenting it,
    /// the way any double is already a rounded number, so the value is only checked to be a double:
    /// `0.1` goes into a `Float32` column, which `accurateCast` would reject as inexact.
    type = makeASTFunction(
        "replaceRegexpAll",
        std::move(type),
        make_intrusive<ASTLiteral>(Field(String("\\b(BFloat16|Float32)\\b"))),
        make_intrusive<ASTLiteral>(Field(String("Float64"))));
    auto converted = makeASTFunction("accurateCast", value->clone(), std::move(type));
    if (!hasFractionalConstant(value))
        return converted;

    /** `accurateCast` truncates the digits a decimal type does not keep - `1.5` becomes `1` in an
      * `Int32` and `1.25` becomes `1.2` in a `Decimal(38, 1)` - so a value with fractional digits is
      * also checked to come back unchanged when its conversion is converted back to its own type,
      * which compares the two whatever the type of the column is. The check has to be folded while
      * the query is analyzed, like the conversion, and `throwIf` is not folded while a mutation is
      * validated, so the error is raised by a conversion that fails: the `CAST` of the message to an
      * enumeration without it.
      */
    auto converted_back = makeASTFunction("accurateCast", converted->clone(), makeASTFunction("toTypeName", value->clone()));
    auto exact = makeASTFunction(
        "or", makeASTFunction("isNull", value->clone()), makeASTFunction("equals", std::move(converted_back), value->clone()));
    auto guard = makeASTFunction(
        "CAST",
        makeASTFunction("if", std::move(exact), make_intrusive<ASTLiteral>(Field(String())), make_intrusive<ASTLiteral>(Field(message))),
        make_intrusive<ASTLiteral>(Field(String("Enum8('' = 0)"))));
    return makeASTFunction(
        "tupleElement", makeASTFunction("tuple", std::move(converted), std::move(guard)), make_intrusive<ASTLiteral>(Field(UInt64(1))));
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
