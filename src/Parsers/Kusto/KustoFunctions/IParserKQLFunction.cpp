#include "KQLFunctionFactory.h"
#include <Parsers/Kusto/IKQLParser.h>
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

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int SYNTAX_ERROR;
extern const int UNKNOWN_FUNCTION;
}

constexpr KQLTokenType determineClosingPair(const KQLTokenType token_type)
{
    if (token_type == KQLTokenType::OpeningCurlyBrace)
        return KQLTokenType::ClosingCurlyBrace;
    else if (token_type == KQLTokenType::OpeningRoundBracket)
        return KQLTokenType::ClosingRoundBracket;
    else if (token_type == KQLTokenType::OpeningSquareBracket)
        return KQLTokenType::ClosingSquareBracket;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unhandled token: {}", magic_enum::enum_name(token_type));
}

constexpr bool isClosingBracket(const KQLTokenType token_type)
{
    return token_type == KQLTokenType::ClosingCurlyBrace || token_type == KQLTokenType::ClosingRoundBracket
        || token_type == KQLTokenType::ClosingSquareBracket;
}

constexpr bool isOpeningBracket(const KQLTokenType token_type)
{
    return token_type == KQLTokenType::OpeningCurlyBrace || token_type == KQLTokenType::OpeningRoundBracket
        || token_type == KQLTokenType::OpeningSquareBracket;
}
//}

//namespace DB
//{
bool IParserKQLFunction::convert(String & out, IKQLParser::KQLPos & pos)
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
    String & out, IKQLParser::KQLPos & pos, const std::string_view ch_fn, const Interval & argument_count_interval)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    out.append(ch_fn.data(), ch_fn.length());
    out.push_back('(');

    int argument_count = 0;
    const auto begin = pos;
    while (isValidKQLPos(pos) && pos->type != KQLTokenType::PipeMark && pos->type != KQLTokenType::Semicolon)
    {
        if (pos != begin)
            out.append(", ");

        if (const auto argument = getOptionalArgument(fn_name, pos))
        {
            ++argument_count;
            out.append(*argument);
        }

        if (pos->type == KQLTokenType::ClosingRoundBracket)
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

String IParserKQLFunction::getArgument(const String & function_name, IKQLParser::KQLPos & pos, const ArgumentState argument_state)
{
    if (auto optional_argument = getOptionalArgument(function_name, pos, argument_state))
        return std::move(*optional_argument);

    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Required argument was not provided in {}", function_name);
}

std::vector<std::string> IParserKQLFunction::getArguments(
    const String & function_name, IKQLParser::KQLPos & pos, const ArgumentState argument_state, const Interval & argument_count_interval)
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

String IParserKQLFunction::getConvertedArgument(const String & fn_name, IKQLParser::KQLPos & pos)
{
    int32_t round_bracket_count = 0, square_bracket_count = 0;
    if (pos->type == KQLTokenType::ClosingRoundBracket || pos->type == KQLTokenType::ClosingSquareBracket)
        return {};

    if (!isValidKQLPos(pos) || pos->type == KQLTokenType::PipeMark || pos->type == KQLTokenType::Semicolon)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Need more argument(s) in function: {}", fn_name);

    std::vector<String> tokens;
    while (isValidKQLPos(pos) && pos->type != KQLTokenType::PipeMark && pos->type != KQLTokenType::Semicolon)
    {
        if (pos->type == KQLTokenType::OpeningRoundBracket)
            ++round_bracket_count;
        if (pos->type == KQLTokenType::ClosingRoundBracket)
            --round_bracket_count;

        if (pos->type == KQLTokenType::OpeningSquareBracket)
            ++square_bracket_count;
        if (pos->type == KQLTokenType::ClosingSquareBracket)
            --square_bracket_count;

        if (!KQLOperators::convert(tokens, pos))
        {
            if (pos->type == KQLTokenType::BareWord)
            {
                tokens.push_back(IParserKQLFunction::getExpression(pos));
            }
            else if (
                pos->type == KQLTokenType::Comma || pos->type == KQLTokenType::ClosingRoundBracket
                || pos->type == KQLTokenType::ClosingSquareBracket)
            {
                if (pos->type == KQLTokenType::Comma)
                    break;
                if (pos->type == KQLTokenType::ClosingRoundBracket && round_bracket_count == -1)
                    break;
                if (pos->type == KQLTokenType::ClosingSquareBracket && square_bracket_count == 0)
                    break;
                tokens.push_back(String(pos->begin, pos->end));
            }
            else
            {
                String token;
                if (pos->type == KQLTokenType::QuotedIdentifier)
                    token = "'" + escapeSingleQuotes(String(pos->begin + 1, pos->end - 1)) + "'";
                else if (pos->type == KQLTokenType::OpeningSquareBracket)
                {
                    ++pos;
                    String array_index;
                    while (isValidKQLPos(pos) && pos->type != KQLTokenType::ClosingSquareBracket)
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
        if (pos->type == KQLTokenType::Comma || pos->type == KQLTokenType::ClosingRoundBracket || pos->type == KQLTokenType::ClosingSquareBracket)
        {
            if (pos->type == KQLTokenType::Comma)
                break;
            if (pos->type == KQLTokenType::ClosingRoundBracket && round_bracket_count == -1)
                break;
            if (pos->type == KQLTokenType::ClosingSquareBracket && square_bracket_count == 0)
                break;
        }
    }

    String converted_arg;
    for (const auto & token : tokens)
        converted_arg.append((converted_arg.empty() ? "" : " ") + token);

    return converted_arg;
}

std::optional<String>
IParserKQLFunction::getOptionalArgument(const String & function_name, IKQLParser::KQLPos & pos, const ArgumentState argument_state)
{
    if (const auto type = pos->type; type != KQLTokenType::Comma && type != KQLTokenType::OpeningRoundBracket)
        return {};

    ++pos;
    if (const auto type = pos->type; type == KQLTokenType::ClosingRoundBracket || type == KQLTokenType::ClosingSquareBracket)
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
    std::stack<KQLTokenType> scopes;
    while (isValidKQLPos(pos) && (!scopes.empty() || (pos->type != KQLTokenType::Comma && pos->type != KQLTokenType::ClosingRoundBracket)))
    {
        const auto token_type = pos->type;
        if (isOpeningBracket(token_type))
            scopes.push(token_type);
        else if (isClosingBracket(token_type))
        {
            if (scopes.empty() || determineClosingPair(scopes.top()) != token_type)
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR, "Unmatched token: {} when parsing {}", magic_enum::enum_name(token_type), function_name);

            scopes.pop();
        }

        ++pos;
    }

    return std::string(begin, pos->begin);
}

String IParserKQLFunction::getKQLFunctionName(IKQLParser::KQLPos & pos)
{
    String fn_name(pos->begin, pos->end);
    ++pos;
    if (pos->type != KQLTokenType::OpeningRoundBracket)
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
    KQLTokens call_tokens(kql_call.data(), kql_call.data() + kql_call.length());
    IKQLParser::KQLPos tokens_pos(call_tokens, max_depth, max_backtracks);
    return IParserKQLFunction::getExpression(tokens_pos);
}

void IParserKQLFunction::validateEndOfFunction(const String & fn_name, IKQLParser::KQLPos & pos)
{
    if (pos->type != KQLTokenType::ClosingRoundBracket)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Too many arguments in function: {}", fn_name);
}

String IParserKQLFunction::getExpression(IKQLParser::KQLPos & pos)
{
    String arg(pos->begin, pos->end);
    auto parseConstTimespan = [&]()
    {
        ParserKQLDateTypeTimespan time_span;
        ASTPtr node;
        KQLExpected expected;

        if (time_span.parse(pos, node, expected))
            arg = boost::lexical_cast<std::string>(time_span.toSeconds());
    };

    if (pos->type == KQLTokenType::BareWord)
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
                if (pos->type == KQLTokenType::OpeningRoundBracket)
                {
                    if (Poco::toLower(arg) != "and" && Poco::toLower(arg) != "or")
                        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "{} is not a supported kusto function", arg);
                }
                --pos;
            }

            parseConstTimespan();
        }
    }
    else if (pos->type == KQLTokenType::QuotedIdentifier)
        arg = "'" + escapeSingleQuotes(String(pos->begin + 1, pos->end - 1)) + "'";
    else if (pos->type == KQLTokenType::OpeningSquareBracket)
    {
        ++pos;
        String array_index;
        while (isValidKQLPos(pos) && pos->type != KQLTokenType::ClosingSquareBracket)
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
