#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>

#include <pcg_random.hpp>

#include <format>

namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

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

bool IParserKQLFunction::directMapping(String & out, IParser::Pos & pos, const String & ch_fn)
{
    std::vector<String> arguments;

    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    String res;
    auto begin = pos;
    ++pos;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        String argument = getConvertedArgument(fn_name, pos);
        arguments.push_back(argument);

        if (pos->type == TokenType::ClosingRoundBracket)
        {
            for (auto arg : arguments)
            {
                if (res.empty())
                    res = ch_fn + "(" + arg;
                else
                    res = res + ", " + arg;
            }
            res += ")";

            out = res;
            return true;
        }
        ++pos;
    }

    pos = begin;
    return false;
}

String IParserKQLFunction::generateUniqueIdentifier()
{
    static pcg32_unique unique_random_generator;
    return std::to_string(unique_random_generator());
}

String IParserKQLFunction::getArgument(const String & function_name, DB::IParser::Pos & pos)
{
    if (auto optionalArgument = getOptionalArgument(function_name, pos))
        return std::move(*optionalArgument);

    throw Exception(std::format("Required argument was not provided in {}", function_name), ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

String IParserKQLFunction::getConvertedArgument(const String & fn_name, IParser::Pos & pos)
{
    String converted_arg;
    std::vector<String> tokens;
    std::unique_ptr<IParserKQLFunction> fun;

    if (pos->type == TokenType::ClosingRoundBracket || pos->type == TokenType::ClosingSquareBracket)
        return converted_arg;

    if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        throw Exception("Need more argument(s) in function: " + fn_name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        String new_token;
        if (!KQLOperators().convert(tokens, pos))
        {
            if (pos->type == TokenType::BareWord)
            {
                tokens.push_back(IParserKQLFunction::getExpression(pos));
            }
            else if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket || pos->type == TokenType::ClosingSquareBracket)
            {
                break;
            }
            else
            {
                String token;
                if (pos->type == TokenType::QuotedIdentifier)
                    token = "'" + String(pos->begin + 1,pos->end - 1) + "'";
                else
                    token = String(pos->begin, pos->end);

                tokens.push_back(token);
            }
        }
        ++pos;
        if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket || pos->type == TokenType::ClosingSquareBracket)
            break;
    }
    for (auto token : tokens)
        converted_arg = converted_arg.empty() ? token : converted_arg + " " + token ;

    return converted_arg;
}

std::optional<String> IParserKQLFunction::getOptionalArgument(const String & function_name, DB::IParser::Pos & pos)
{
    if (const auto & type = pos->type; type != DB::TokenType::Comma && type != DB::TokenType::OpeningRoundBracket)
        return {};

    ++pos;
    return getConvertedArgument(function_name, pos);
}

String IParserKQLFunction::getKQLFunctionName(IParser::Pos & pos)
{
    String fn_name = String(pos->begin, pos->end);
    ++pos;
    if (pos->type != TokenType::OpeningRoundBracket)
    {
        --pos;
        return "";
    }
    return fn_name;
}

String IParserKQLFunction::kqlCallToExpression(
    const String & function_name, std::initializer_list<std::string_view> params, const uint32_t max_depth)
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
    DB::Tokens call_tokens(kql_call.c_str(), kql_call.c_str() + kql_call.length());
    DB::IParser::Pos tokens_pos(call_tokens, max_depth);
    return DB::IParserKQLFunction::getExpression(tokens_pos);
}

void IParserKQLFunction::validateEndOfFunction(const String & fn_name, IParser::Pos & pos)
{
    if (pos->type != TokenType::ClosingRoundBracket)
        throw Exception("Too many arguments in function: " + fn_name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

String IParserKQLFunction::getExpression(IParser::Pos & pos)
{
    String arg = String(pos->begin, pos->end);
    if (pos->type == TokenType::BareWord)
    {
        String new_arg;
        auto fun = KQLFunctionFactory::get(arg);
        if (fun && fun->convert(new_arg, pos))
        {
            validateEndOfFunction(arg, pos);
            arg = new_arg;
        }
        else
        {
            ParserKQLDateTypeTimespan time_span;
            ASTPtr node;
            Expected expected;

            if (time_span.parse(pos, node, expected))
                arg = std::to_string(time_span.toSeconds());
        }
    }
    else if (pos->type == TokenType::QuotedIdentifier)
        arg = "'" + String(pos->begin + 1,pos->end - 1) + "'";

    return arg;
}

int IParserKQLFunction::getNullCounts(String arg)
{
    int nullCount = 0;
    size_t i;
    String temp;
    for(i = 0; i < arg.size(); i++)
        if(arg[i] != ' ')
            temp += arg[i];
    arg = temp;
    temp = "";
    for(i = 0; i < arg.size(); i++)
        if(arg[i] == '(')
            break;

    while(i < arg.size())
    {
        if(arg[i] != ',' && arg[i] != ')')
            temp += arg[i];
        else
        {
            if(temp == "NULL" || temp == "null")
                nullCount += 1;
            temp = "";    
        }
        i += 1;
    }
    return nullCount;
}

int IParserKQLFunction::IParserKQLFunction::getArrayLength(String arg)
{
    int array_length = 0;
    bool comma_found = false;
    for(size_t i = 0; i < arg.size(); i++)
    {
        if(arg[i] == ',')
        {
            comma_found = true;
            array_length += 1;
        }
    }
    return comma_found ? array_length + 1 : 0;
}

String IParserKQLFunction::ArraySortHelper(String & out,IParser::Pos & pos, bool ascending)
{
    String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return "false";

    String reverse;
    String second_arg;
    String expr;

    if(!ascending)
        reverse = "Reverse";
    ++pos;
    String first_arg = getConvertedArgument(fn_name, pos);
    int nullCount = getNullCounts(first_arg);
    if(pos->type == TokenType::Comma)
        ++pos;

    if(pos->type != TokenType::ClosingRoundBracket  && String(pos->begin, pos->end) != "dynamic")
    {
        second_arg = getConvertedArgument(fn_name, pos);
        out =  "if (" + second_arg + ", array" + reverse + "Sort(" + first_arg + "), concat( arraySlice(array" + reverse + "Sort(" + first_arg + ") as as1, indexOf(as1, NULL) as len1 ), arraySlice( as1, 1, len1-1)))";
        return out;
    }
    --pos;
    std::vector<String> argument_list;
    if(pos->type != TokenType::ClosingRoundBracket)
    {
        while(pos->type != TokenType::ClosingRoundBracket)
        {
            ++pos;
            if(String(pos->begin, pos->end) != "dynamic")
            {
                expr = getConvertedArgument(fn_name, pos);
                break;
            }
            second_arg = getConvertedArgument(fn_name, pos);
            argument_list.push_back(second_arg);
        }
    }
    else
    {
        ++pos;
        out = "array"+ reverse +"Sort(" + first_arg + ")";
    }

    if(argument_list.size() > 0)
    {
        String temp_first_arg = first_arg;
        int first_arg_length = getArrayLength(temp_first_arg);

        if(nullCount > 0 && expr.empty())
            expr = "true";
        if(nullCount > 0)
            first_arg =  "if (" + expr + ", array" + reverse + "Sort(" + first_arg + "), concat( arraySlice(array" + reverse + "Sort(" + first_arg + ") as as1, indexOf(as1, NULL) as len1 ), arraySlice( as1, 1, len1-1) ) )";
        else
            first_arg = "array" + reverse + "Sort(" + first_arg + ")";

        out = first_arg + " AS array0_sorted";
        
        for(size_t i = 0; i < argument_list.size(); i++)
        {
            out += " , ";
            if(first_arg_length != getArrayLength(argument_list[i]))
                out += "NULL AS array" + std::to_string(i + 1)+ "_sorted";
            else if(nullCount > 0)
                out +=  "If ( " + expr + "," + "array" + reverse + "Sort((x, y) -> y, " + argument_list[i] + "," + temp_first_arg + "), arrayConcat( arraySlice( " + "array" + reverse + "Sort((x, y) -> y, " + argument_list[i] + "," + temp_first_arg + ") , length(" + temp_first_arg + ") - " + std::to_string(nullCount) + " + 1) , arraySlice( " + "array" + reverse + "Sort((x, y) -> y, " + argument_list[i] + "," + temp_first_arg + ") , 1, length( " + temp_first_arg + ") - " + std::to_string(nullCount) + ") ) )" + "AS array" + std::to_string(i + 1)+ "_sorted";
            else
                out += "array" + reverse + "Sort((x, y) -> y, " + argument_list[i] + "," + temp_first_arg + ")" + "AS array" + std::to_string(i + 1)+ "_sorted";
        }
        out += " )";
    }
    return out;
}
}
