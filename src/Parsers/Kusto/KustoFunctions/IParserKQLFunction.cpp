#include "KQLFunctionFactory.h"
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <boost/lexical_cast.hpp>
#include <magic_enum.hpp>
#include <pcg_random.hpp>
#include <Poco/String.h>
#include <format>
#include <numeric>
#include <stack>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int SYNTAX_ERROR;
extern const int UNKNOWN_FUNCTION;
}

namespace
{
constexpr DB::TokenType determineClosingPair(const DB::TokenType token_type)
{
    if (token_type == DB::TokenType::OpeningCurlyBrace)
        return DB::TokenType::ClosingCurlyBrace;
    if (token_type == DB::TokenType::OpeningRoundBracket)
        return DB::TokenType::ClosingRoundBracket;
    if (token_type == DB::TokenType::OpeningSquareBracket)
        return DB::TokenType::ClosingSquareBracket;

    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Unhandled token: {}", magic_enum::enum_name(token_type));
}

constexpr bool isClosingBracket(const DB::TokenType token_type)
{
    return token_type == DB::TokenType::ClosingCurlyBrace || token_type == DB::TokenType::ClosingRoundBracket
        || token_type == DB::TokenType::ClosingSquareBracket;
}

constexpr bool isOpeningBracket(const DB::TokenType token_type)
{
    return token_type == DB::TokenType::OpeningCurlyBrace || token_type == DB::TokenType::OpeningRoundBracket
        || token_type == DB::TokenType::OpeningSquareBracket;
}
}

namespace DB
{
bool IParserKQLFunction::convert(String & out, IParser::Pos & pos)
{
    return wrapConvertImpl(
        pos,
        IncreaseDepthTag{},
        [&]
        {
            bool res = convertImpl(out, pos);
            if (!res)
                out = "";
            return res;
        });
}

bool IParserKQLFunction::directMapping(
    String & out, IParser::Pos & pos, const std::string_view ch_fn, const Interval & argument_count_interval)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    out.append(ch_fn.data(), ch_fn.length());
    out.push_back('(');

    int argument_count = 0;
    const auto begin = pos;
    while (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (pos != begin)
            out.append(", ");

        if (const auto argument = getOptionalArgument(fn_name, pos))
        {
            ++argument_count;
            out.append(*argument);
        }

        if (pos->type == TokenType::ClosingRoundBracket)
        {
            if (!argument_count_interval.IsWithinBounds(argument_count))
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "{}: between {} and {} arguments are expected, but {} were provided",
                    fn_name,
                    argument_count_interval.Min(),
                    argument_count_interval.Max(),
                    argument_count);

            out.push_back(')');
            return true;
        }
    }

    out.clear();
    pos = begin;
    return false;
}

String IParserKQLFunction::generateUniqueIdentifier()
{
    // This particular random generator hits each number exactly once before looping over.
    // Because of this, it's sufficient for queries consisting of up to 2^16 (= 65536) distinct function calls.
    // Reference: https://www.pcg-random.org/using-pcg-cpp.html#insecure-generators
    static thread_local pcg32_once_insecure random_generator;
    return std::to_string(random_generator());
}

String IParserKQLFunction::getArgument(const String & function_name, DB::IParser::Pos & pos, const ArgumentState argument_state)
{
    if (auto optional_argument = getOptionalArgument(function_name, pos, argument_state))
        return std::move(*optional_argument);

    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Required argument was not provided in {}", function_name);
}

std::vector<std::string> IParserKQLFunction::getArguments(
    const String & function_name, DB::IParser::Pos & pos, const ArgumentState argument_state, const Interval & argument_count_interval)
{
    std::vector<std::string> arguments;
    while (auto argument = getOptionalArgument(function_name, pos, argument_state))
    {
        arguments.push_back(std::move(*argument));
    }
    if (!argument_count_interval.IsWithinBounds(static_cast<int>(arguments.size())))
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "{}: between {} and {} arguments are expected, but {} were provided",
            function_name,
            argument_count_interval.Min(),
            argument_count_interval.Max(),
            arguments.size());

    return arguments;
}

String IParserKQLFunction::getConvertedArgument(const String & fn_name, IParser::Pos & pos)
{
    int32_t round_bracket_count = 0, square_bracket_count = 0;
    if (pos->type == TokenType::ClosingRoundBracket || pos->type == TokenType::ClosingSquareBracket)
        return {};

    if (!isValidKQLPos(pos) || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Need more argument(s) in function: {}", fn_name);

    std::vector<String> tokens;
    while (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++round_bracket_count;
        if (pos->type == TokenType::ClosingRoundBracket)
            --round_bracket_count;

        if (pos->type == TokenType::OpeningSquareBracket)
            ++square_bracket_count;
        if (pos->type == TokenType::ClosingSquareBracket)
            --square_bracket_count;

        if (!KQLOperators::convert(tokens, pos))
        {
            if (pos->type == TokenType::BareWord)
            {
                tokens.push_back(IParserKQLFunction::getExpression(pos));
            }
            else if (
                pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket
                || pos->type == TokenType::ClosingSquareBracket)
            {
                if (pos->type == TokenType::Comma)
                    break;
                if (pos->type == TokenType::ClosingRoundBracket && round_bracket_count == -1)
                    break;
                if (pos->type == TokenType::ClosingSquareBracket && square_bracket_count == 0)
                    break;
                tokens.push_back(String(pos->begin, pos->end));
            }
            else
            {
                String token;
                if (pos->type == TokenType::QuotedIdentifier)
                    token = "'" + escapeSingleQuotes(String(pos->begin + 1, pos->end - 1)) + "'";
                else if (pos->type == TokenType::OpeningSquareBracket)
                {
                    ++pos;
                    String array_index;
                    while (isValidKQLPos(pos) && pos->type != TokenType::ClosingSquareBracket)
                    {
                        array_index += getExpression(pos);
                        ++pos;
                    }
                    token = std::format("[ {0} >=0 ? {0} + 1 : {0}]", array_index);
                }
                else
                    token = String(pos->begin, pos->end);

                tokens.push_back(token);
            }
        }

        ++pos;
        if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket || pos->type == TokenType::ClosingSquareBracket)
        {
            if (pos->type == TokenType::Comma)
                break;
            if (pos->type == TokenType::ClosingRoundBracket && round_bracket_count == -1)
                break;
            if (pos->type == TokenType::ClosingSquareBracket && square_bracket_count == 0)
                break;
        }
    }

    String converted_arg;
    for (const auto & token : tokens)
        converted_arg.append((converted_arg.empty() ? "" : " ") + token);

    return converted_arg;
}

std::optional<String>
IParserKQLFunction::getOptionalArgument(const String & function_name, DB::IParser::Pos & pos, const ArgumentState argument_state)
{
    if (const auto type = pos->type; type != DB::TokenType::Comma && type != DB::TokenType::OpeningRoundBracket)
        return {};

    ++pos;
    if (const auto type = pos->type; type == DB::TokenType::ClosingRoundBracket || type == DB::TokenType::ClosingSquareBracket)
        return {};

    if (argument_state == ArgumentState::Parsed)
        return getConvertedArgument(function_name, pos);

    if (argument_state != ArgumentState::Raw)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Argument extraction is not implemented for {}::{}",
            magic_enum::enum_type_name<ArgumentState>(),
            magic_enum::enum_name(argument_state));

    const auto * begin = pos->begin;
    std::stack<DB::TokenType> scopes;
    while (isValidKQLPos(pos) && (!scopes.empty() || (pos->type != DB::TokenType::Comma && pos->type != DB::TokenType::ClosingRoundBracket)))
    {
        const auto token_type = pos->type;
        if (isOpeningBracket(token_type))
            scopes.push(token_type);
        else if (isClosingBracket(token_type))
        {
            if (scopes.empty() || determineClosingPair(scopes.top()) != token_type)
                throw Exception(
                    DB::ErrorCodes::SYNTAX_ERROR, "Unmatched token: {} when parsing {}", magic_enum::enum_name(token_type), function_name);

            scopes.pop();
        }

        ++pos;
    }

    return std::string(begin, pos->begin);
}

String IParserKQLFunction::getKQLFunctionName(IParser::Pos & pos)
{
    String fn_name(pos->begin, pos->end);
    ++pos;
    if (pos->type != TokenType::OpeningRoundBracket)
    {
        --pos;
        return "";
    }
    return fn_name;
}

String IParserKQLFunction::kqlCallToExpression(
    const std::string_view function_name, const std::initializer_list<const std::string_view> params, uint32_t max_depth, uint32_t max_backtracks)
{
    return kqlCallToExpression(function_name, std::span(params), max_depth, max_backtracks);
}

String IParserKQLFunction::kqlCallToExpression(
    const std::string_view function_name, const std::span<const std::string_view> params, uint32_t max_depth, uint32_t max_backtracks)
{
    const auto params_str = std::accumulate(
        std::cbegin(params),
        std::cend(params),
        String(),
        [](String acc, const std::string_view param)
        {
            if (!acc.empty())
                acc.append(", ");

            acc.append(param.data(), param.length());
            return acc;
        });

    const auto kql_call = std::format("{}({})", function_name, params_str);
    Tokens call_tokens(kql_call.data(), kql_call.data() + kql_call.length(), 0, true);
    IParser::Pos tokens_pos(call_tokens, max_depth, max_backtracks);
    return DB::IParserKQLFunction::getExpression(tokens_pos);
}

void IParserKQLFunction::validateEndOfFunction(const String & fn_name, IParser::Pos & pos)
{
    if (pos->type != TokenType::ClosingRoundBracket)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Too many arguments in function: {}", fn_name);
}

String IParserKQLFunction::getExpression(IParser::Pos & pos)
{
    String arg(pos->begin, pos->end);
    auto parseConstTimespan = [&]()
    {
        ParserKQLDateTypeTimespan time_span;
        ASTPtr node;
        Expected expected;

        if (time_span.parse(pos, node, expected))
            arg = boost::lexical_cast<std::string>(time_span.toSeconds());
    };

    if (pos->type == TokenType::BareWord)
    {
        const auto fun = KQLFunctionFactory::get(arg);
        if (String new_arg; fun && fun->convert(new_arg, pos))
        {
            validateEndOfFunction(arg, pos);
            arg = std::move(new_arg);
        }
        else
        {
            if (!fun)
            {
                ++pos;
                if (pos->type == TokenType::OpeningRoundBracket)
                {
                    if (Poco::toLower(arg) != "and" && Poco::toLower(arg) != "or")
                        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "{} is not a supported kusto function", arg);
                }
                --pos;
            }

            parseConstTimespan();
        }
    }
    else if (pos->type == TokenType::ErrorWrongNumber)
        parseConstTimespan();
    else if (pos->type == TokenType::QuotedIdentifier)
        arg = "'" + escapeSingleQuotes(String(pos->begin + 1, pos->end - 1)) + "'";
    else if (pos->type == TokenType::OpeningSquareBracket)
    {
        ++pos;
        String array_index;
        while (isValidKQLPos(pos) && pos->type != TokenType::ClosingSquareBracket)
        {
            array_index += getExpression(pos);
            ++pos;
        }
        arg = std::format("[ {0} >=0 ? {0} + 1 : {0}]", array_index);
    }

    return arg;
}

String IParserKQLFunction::escapeSingleQuotes(const String & input)
{
    String output;
    for (const auto & ch : input)
    {
        if (ch == '\'')
            output += ch;
        output += ch;
    }
    return output;
}
}
