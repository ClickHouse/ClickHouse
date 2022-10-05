#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>
#include <format>
#include <cstdlib>
#include <Parsers/CommonParsers.h>

namespace DB::ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int UNKNOWN_TYPE;
}

namespace DB
{

bool Base64EncodeToString::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out,pos,"base64Encode");
}

bool Base64EncodeFromGuid::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
        
    String guid;

    ++pos;
    if(pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
    {
        --pos;
        const String arg = getArgument(fn_name, pos);
        guid = kqlCallToExpression("guid", {arg}, pos.max_depth);
    }
    else
    {
        guid = getConvertedArgument(fn_name, pos);
    }

    out = std::format("base64Encode(toString({}))", guid);

    return true;
}

bool Base64DecodeToString::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out,pos,"base64Decode");
}

bool Base64DecodeToArray::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String str = getConvertedArgument(fn_name, pos);

    out = std::format("arrayMap(x -> (reinterpretAsUInt8(x)), splitByRegexp ('',base64Decode({})))", str);

    return true;
}

bool Base64DecodeToGuid::convertImpl(String & out,IParser::Pos & pos)
{
   return directMapping(out,pos,"base64Decode");
}

bool CountOf::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String source = getConvertedArgument(fn_name, pos);

    ++pos;
    const String search = getConvertedArgument(fn_name, pos);

    String kind = "'normal'";
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        kind = getConvertedArgument(fn_name,pos);
    }
    assert (kind =="'normal'" || kind =="'regex'");

    if (kind == "'normal'" )
        out = "countSubstrings(" + source + ", " + search + ")";
    else
        out = "countMatches("+ source + ", " + search + ")";
    return true;

}

bool Extract::convertImpl(String & out, IParser::Pos & pos)
{
    ParserKeyword s_kql("typeof");
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);
    Expected expected;

    std::unordered_map<String,String> type_cast =
    {   {"bool", "Boolean"},
        {"boolean", "Boolean"},
        {"datetime", "DateTime"},
        {"date", "DateTime"},
        {"guid", "UUID"},
        {"int", "Int32"},
        {"long", "Int64"},
        {"real", "Float64"},
        {"double", "Float64"},
        {"string", "String"},
        {"decimal", "Decimal"}
    };

    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String regex = getConvertedArgument(fn_name, pos);

    ++pos;
    size_t capture_group = stoi(getConvertedArgument(fn_name, pos));

    ++pos;
    String source = getConvertedArgument(fn_name, pos);

    String type_literal;

    if (pos->type == TokenType::Comma)
    {
        ++pos;

        if (s_kql.ignore(pos, expected))
        {
            if (!open_bracket.ignore(pos, expected))
                throw Exception("Syntax error near typeof", ErrorCodes::SYNTAX_ERROR);

            type_literal= String(pos->begin, pos->end);

            if (type_cast.find(type_literal) == type_cast.end())
                throw Exception(type_literal + " is not a supported kusto data type for extract", ErrorCodes::UNKNOWN_TYPE);

            type_literal = type_cast[type_literal];
            ++pos;

            if (!close_bracket.ignore(pos, expected))
                throw Exception("Syntax error near typeof", ErrorCodes::SYNTAX_ERROR);
        }
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

    out = "extract(" + source + ", " + regex + ")";

    if (type_literal == "Decimal")
    {
        out = std::format("countSubstrings({0}, '.') > 1 ? NULL: {0}, length(substr({0}, position({0},'.') + 1)))", out);
        out = std::format("toDecimal128OrNull({0})", out);
    }
    else
    {
        if (type_literal == "Boolean")
            out = std::format("toInt64OrNull({})", out);

        if (!type_literal.empty())
            out = "accurateCastOrNull(" + out + ", '" + type_literal + "')";
    }
    return true;
}

bool ExtractAll::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String regex = getConvertedArgument(fn_name, pos);

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

    out = "extractAllGroups(" + second_arg + ", " + regex + ")";
    return true;
}

bool ExtractJson::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool HasAnyIndex::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String source = getConvertedArgument(fn_name, pos);

    ++pos;
    const String lookup = getConvertedArgument(fn_name, pos);
    String src_array = std::format("splitByChar(' ',{})", source);
    out = std::format("if (empty({1}), -1, indexOf(arrayMap ( x -> (x in {0}),  if (empty({1}),[''], arrayMap(x->(toString(x)),{1}))),1) - 1)",
        src_array, lookup);
    return true;
}

bool IndexOf::convertImpl(String & out,IParser::Pos & pos)
{
    int start_index = 0, length = -1, occurrence = 1;

    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String source = getConvertedArgument(fn_name, pos);

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
    return directMapping(out,pos,"isNull");
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
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String url = getConvertedArgument(fn_name, pos);

    const String scheme = std::format("concat('\"Scheme\":\"', protocol({0}),'\"')",url);
    const String host = std::format("concat('\"Host\":\"', domain({0}),'\"')",url);
    const String port = std::format("concat('\"Port\":\"', toString(port({0})),'\"')",url);
    const String path = std::format("concat('\"Path\":\"', path({0}),'\"')",url);
    const String username_pwd = std::format("netloc({0})",url);
    const String query_string = std::format("queryString({0})",url);
    const String fragment = std::format("concat('\"Fragment\":\"',fragment({0}),'\"')",url);
    const String username = std::format("concat('\"Username\":\"', arrayElement(splitByChar(':',arrayElement(splitByChar('@',{0}) ,1)),1),'\"')", username_pwd);
    const String password = std::format("concat('\"Password\":\"', arrayElement(splitByChar(':',arrayElement(splitByChar('@',{0}) ,1)),2),'\"')", username_pwd);
    const String query_parameters = std::format("concat('\"Query Parameters\":', concat('{{\"', replace(replace({}, '=', '\":\"'),'&','\",\"') ,'\"}}'))", query_string);

    out = std::format("concat('{{',{},',',{},',',{},',',{},',',{},',',{},',',{},',',{},'}}')",scheme, host, port, path, username, password, query_parameters,fragment);
    return true;
}

bool ParseURLQuery::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String query = getConvertedArgument(fn_name, pos);

    const String query_string = std::format("if (position({},'?') > 0, queryString({}), {})", query, query, query);
    const String query_parameters = std::format("concat('\"Query Parameters\":', concat('{{\"', replace(replace({}, '=', '\":\"'),'&','\",\"') ,'\"}}'))", query_string);
    out = std::format("concat('{{',{},'}}')",query_parameters);
    return true;
}

bool ParseVersion::convertImpl(String & out,IParser::Pos & pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ReplaceRegex::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "replaceRegexpAll");
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

    ++pos;
    const String source = getConvertedArgument(fn_name, pos);

    ++pos;
    const String delimiter = getConvertedArgument(fn_name, pos);

    int requested_index = -1;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        requested_index = std::stoi(getConvertedArgument(fn_name, pos));
    }

    out = "splitByString(" + delimiter + ", " + source + ")";
    if (requested_index >= 0)
    {
        out = "arrayPushBack([],arrayElement(" + out + ", " + std::to_string(requested_index + 1) + "))";
    }
    return true;
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

    ++pos;
    const String delimiter = getConvertedArgument(fn_name, pos);

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

    out = std::move(args);
    return true;
}

bool StrCmp::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    const String string1 = getConvertedArgument(fn_name, pos);
    ++pos;
    const String string2 = getConvertedArgument(fn_name, pos);

    out = std::format("multiIf({0} == {1}, 0, {0} < {1}, -1, 1)", string1, string2);
    return true;
}

bool StrLen::convertImpl(String & out,IParser::Pos & pos)
{
    return directMapping(out, pos, "lengthUTF8");
}

bool StrRep::convertImpl(String & out,IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    const String value = getConvertedArgument(fn_name, pos);

    ++pos;
    const String multiplier = getConvertedArgument(fn_name, pos);

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        const String delimiter = getConvertedArgument(fn_name, pos);
        const String repeated_str = "repeat(concat("+value+"," + delimiter + ")," + multiplier + ")";
        out = "substr("+ repeated_str + ", 1, length(" + repeated_str + ") - length(" + delimiter + "))";
    }
    else
        out = "repeat("+ value + ", " + multiplier + ")";

    return true;
}

bool SubString::convertImpl(String & out,IParser::Pos & pos)
{
    String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String source = getConvertedArgument(fn_name, pos);

    ++pos;
    String startingIndex = getConvertedArgument(fn_name, pos);

    if (pos->type == TokenType::Comma)
    {
        ++pos;
        auto length = getConvertedArgument(fn_name, pos);

        if(startingIndex.empty())
            throw Exception("number of arguments do not match in function: " + fn_name, ErrorCodes::SYNTAX_ERROR);
        else
            out = "substr("+ source + ", " + "((" + startingIndex + "% (toInt64(length(" + source + ")) AS n)  + n) % n)  + 1, " + length + ")";
    }
    else
        out = "substr("+ source + "," + "((" + startingIndex + "% (toInt64(length(" + source + ")) AS n)  + n) % n) + 1)";

    return true;
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
    const String fn_name = getKQLFunctionName(pos);

    if (fn_name.empty())
        return false;

    ++pos;
    String from = getConvertedArgument(fn_name, pos);
    ++pos;
    String to = getConvertedArgument(fn_name, pos);
    ++pos;
    String source = getConvertedArgument(fn_name, pos);

    String len_diff = std::format("length({}) - length({})", from, to);
    String to_str = std::format("multiIf(length({1}) = 0, {0}, {2} > 0, concat({1},repeat(substr({1},length({1}),1),toUInt16({2}))),{2} < 0 , substr({1},1,length({0})),{1})",
         from, to, len_diff);
    out = std::format("if (length({3}) = 0,'',translate({0},{1},{2}))", source, from, to_str, to); 
    return true;
}

bool Trim::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto regex = getArgument(fn_name, pos, ArgumentState::Raw);
    const auto source = getArgument(fn_name, pos, ArgumentState::Raw);
    out = kqlCallToExpression("trim_start", {regex, std::format("trim_end({0}, {1})", regex, source)}, pos.max_depth);

    return true;
}

bool TrimEnd::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto regex = getArgument(fn_name, pos);
    const auto source = getArgument(fn_name, pos);
    out = std::format("replaceRegexpOne({0}, concat({1}, '$'), '')", source, regex);

    return true;
}

bool TrimStart::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    const auto regex = getArgument(fn_name, pos);
    const auto source = getArgument(fn_name, pos);
    out = std::format("replaceRegexpOne({0}, concat('^', {1}), '')", source, regex);

    return true;
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
