#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>
#include <cstdlib>
#include <Parsers/CommonParsers.h>

namespace DB::ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace DB
{

bool Base64EncodeToString::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out,pos,"base64Encode");
}

bool Base64EncodeFromGuid::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Base64DecodeToString::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out,pos,"base64Decode");
}

bool Base64DecodeToArray::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Base64DecodeToGuid::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool CountOf::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    auto begin = pos;

    ++pos;
    const String source = getConvertedArgument(fn_name, pos);

    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    const String search = getConvertedArgument(fn_name, pos);

    String kind = "'normal' ";
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        kind = getConvertedArgument(fn_name,pos);
    }
    assert (kind =="'normal' " || kind =="'regex' ");

    if (pos->type == TokenType::ClosingRoundBracket)
    {
        if (kind == "'normal' " )
            out = "countSubstrings(" + source + ", " + search + ")";
        else
            out = "countMatches("+ source + ", " + search + ")";
        return true;
    }
    pos = begin;
    return false;
}

bool Extract::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    auto begin = pos;

    ++pos;
    String regex = getConvertedArgument(fn_name, pos);

    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    size_t capture_group = stoi(getConvertedArgument(fn_name, pos));

    ++pos;
    String source = getConvertedArgument(fn_name, pos);

    String type_literal;

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        type_literal = getConvertedArgument(fn_name, pos);
    }

    if (capture_group == 0)
    {
        String tmp_regex;
        for (auto c : regex)
        {
            if (c != '(' && c != ')')
                tmp_regex += c;
        }
        regex = std::move(tmp_regex);
    }
    else
    {
        size_t group_idx = 0;
        size_t str_idx = -1;
        for (size_t i = 0; i < regex.length(); ++i)
        {
            if (regex[i] == '(')
            {
                ++group_idx;
                if (group_idx == capture_group)
                {
                    str_idx = i + 1;
                    break;
                }
            }
        }
        String tmp_regex;
        if (str_idx > 0)
        {
            for (size_t i = str_idx; i < regex.length(); ++i)
            {
                if (regex[i] == ')')
                    break;
                tmp_regex += regex[i];
            }
        }
        regex = "'" + tmp_regex + "'";
    }

    if (pos->type == TokenType::ClosingRoundBracket)
    {
        out = "extract(" + source + ", " + regex + ")";
        if (!type_literal.empty())
        {
            std::unordered_map<String,String> type_cast =
            { {"bool", "Boolean"},
              {"boolean", "Boolean"},
              {"datetime", "DateTime"},
              {"date", "DateTime"},
              {"dynamic", "Array"},
              {"guid", "UUID"},
              {"int", "Int32"},
              {"long", "Int64"},
              {"real", "Float64"},
              {"double", "Float64"},
              {"string", "String"},
              {"decimal", "Decimal"}
            };

            Tokens token_type(type_literal.c_str(), type_literal.c_str() + type_literal.size());
            IParser::Pos pos_type(token_type, pos.max_depth);
            ParserKeyword s_kql("typeof");
            Expected expected;

            if (s_kql.ignore(pos_type, expected))
            {
                ++pos_type;
                auto kql_type= String(pos_type->begin,pos_type->end);
                if (type_cast.find(kql_type) == type_cast.end())
                    return false;
                auto ch_type = type_cast[kql_type];
                out = "CAST(" + out + ", '" + ch_type + "')";
            }
            else
                return false;
        }
        return true;
    }

    pos = begin;
    return false;
}

bool ExtractAll::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    auto begin = pos;

    ++pos;
    const String regex = getConvertedArgument(fn_name, pos);

    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    const String second_arg = getConvertedArgument(fn_name, pos);

    String third_arg;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        third_arg = getConvertedArgument(fn_name, pos);
    }

    if (!third_arg.empty())  // currently the captureGroups not supported
        return false;

    if (pos->type == TokenType::ClosingRoundBracket)
    {
        out = "extractAllGroups(" + second_arg + ", " + regex + ")";
        return true;
    }
    pos = begin;
    return false;
}

bool ExtractJson::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool HasAnyIndex::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool IndexOf::convertImpl(String & out,IParser::Pos & pos)
{
    int start_index = 0, length = -1, occurrence = 1;

    String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    auto begin = pos;

    ++pos;
    const String source = getConvertedArgument(fn_name, pos);
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    const String lookup = getConvertedArgument(fn_name, pos);

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        start_index = stoi(getConvertedArgument(fn_name, pos));

        if (pos->type == TokenType::Comma)
        {
            ++pos;
            length = stoi(getConvertedArgument(fn_name, pos));

            if (pos->type == TokenType::Comma)
            {
                ++pos;
                occurrence = stoi(getConvertedArgument(fn_name, pos));

            }
        }
    }

    if (pos->type == TokenType::ClosingRoundBracket)
    {
        if (occurrence < 0 || length < -1)
            out = "";
        else if (length == -1)
            out = "position(" + source + ", " + lookup + ", " + std::to_string(start_index + 1) + ") - 1";
        else
        {

        }

        return true;
    }

    pos = begin;
    return false;
}

bool IsEmpty::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "empty");
}

bool IsNotEmpty::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "notEmpty");
}

bool IsNotNull::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "isNotNull");
}

bool ParseCommandLine::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool IsNull::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "isNull");
}

bool ParseCSV::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseJson::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseURL::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseURLQuery::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseVersion::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ReplaceRegex::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Reverse::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Split::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    auto begin = pos;

    ++pos;
    const String source = getConvertedArgument(fn_name, pos);
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    const String delimiter = getConvertedArgument(fn_name, pos);

    int requestedIndex = -1;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        requestedIndex = std::stoi(getConvertedArgument(fn_name, pos));
    }

    if (pos->type == TokenType::ClosingRoundBracket)
    {
        out = "splitByString(" + delimiter + ", " + source + ")";
        if (requestedIndex >= 0)
        {
            out = "arrayPushBack([],arrayElement(" + out + ", " + std::to_string(requestedIndex + 1) + "))";
        }
        return true;
    }

    pos = begin;
    return false;
}

bool StrCat::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "concat");
}

bool StrCatDelim::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    auto begin = pos;

    ++pos;
    const String delimiter = getConvertedArgument(fn_name, pos);
    if (pos->type != TokenType::Comma)
        return false;

    int arg_count = 0;
    String args;

    while (!pos->isEnd() && pos->type != TokenType::Semicolon && pos->type != TokenType::ClosingRoundBracket)
    {
        ++pos;
        String arg = getConvertedArgument(fn_name, pos);
        if (args.empty())
            args = "concat(" + arg;
        else
            args = args + ", " + delimiter + ", " + arg;
        ++arg_count;
    }
    args += ")";

    if (arg_count < 2 || arg_count > 64)
        throw Exception("argument count out of bound in function: " + fn_name, ErrorCodes::SYNTAX_ERROR);

    if (pos->type == TokenType::ClosingRoundBracket)
    {
        out = std::move(args);
        return true;
    }

    pos = begin;
    return false;
}

bool StrCmp::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool StrLen::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "lengthUTF8");
}

bool StrRep::convertImpl(String & out,IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    auto begin = pos;

    ++pos;
    String value = getConvertedArgument(fn_name, pos);
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    String multiplier = getConvertedArgument(fn_name, pos);

    String delimiter;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        delimiter = getConvertedArgument(fn_name, pos);
    }

    if (pos->type == TokenType::ClosingRoundBracket)
    {
        if (!delimiter.empty())
        {
            String repeated_str = "repeat(concat("+value+"," + delimiter + ")," + multiplier + ")";
            out = "substr("+ repeated_str + ", 1, length(" + repeated_str + ") - length(" + delimiter + "))";
        }
        else
            out = "repeat("+ value + ", " + multiplier + ")";

        return true;
    }

    pos = begin;
    return false;
}

bool SubString::convertImpl(String & out,IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    auto begin = pos;

    ++pos;
    String source = getConvertedArgument(fn_name, pos);
    
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    String startingIndex = getConvertedArgument(fn_name, pos);

    String length;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        length = getConvertedArgument(fn_name, pos);
    }

    if (pos->type == TokenType::ClosingRoundBracket)
    {
        if (length.empty())
            out = "substr("+ source + "," + startingIndex +" + 1)";
        else
            out = "substr("+ source + ", " + startingIndex +" + 1, " + length + ")";
        return true;
    }
    pos = begin;
    return false;
}

bool ToLower::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "lower");
}

bool ToUpper::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "upper");
}

bool Translate::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Trim::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool TrimEnd::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool TrimStart::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool URLDecode::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "decodeURLComponent");
}

bool URLEncode::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "encodeURLComponent");
}

}
