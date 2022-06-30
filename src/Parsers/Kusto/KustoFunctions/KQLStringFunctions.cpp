#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>

namespace DB
{

bool Base64EncodeToString::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"base64Encode");
}

bool Base64EncodeFromGuid::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Base64DecodeToString::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"base64Decode");
}

bool Base64DecodeToArray::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Base64DecodeToGuid::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool CountOf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Extract::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ExtractAll::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ExtractJson::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool HasAnyIndex::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool IndexOf::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool IsEmpty::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"empty");
}

bool IsNotEmpty::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"notEmpty");
}

bool IsNotNull::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"isNotNull");
}

bool ParseCommandLine::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool IsNull::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"isNull");
}

bool ParseCSV::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseJson::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseURL::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseURLQuery::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseVersion::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ReplaceRegex::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Reverse::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Split::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool StrCat::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"concat");
}

bool StrCatDelim::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool StrCmp::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool StrLen::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"lengthUTF8");
}

bool StrRep::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos); //String(pos->begin,pos->end);

    if (fn_name.empty())
        return false;

    auto begin = pos;

    ++pos;
    String value = getConvertedArgument(fn_name,pos);
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    String multiplier = getConvertedArgument(fn_name,pos);

    String delimiter;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        delimiter = getConvertedArgument(fn_name,pos);
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


bool SubString::convertImpl(String &out,IParser::Pos &pos)
{
    String fn_name = getKQLFunctionName(pos); 

    if (fn_name.empty())
        return false;

    auto begin = pos;

    ++pos;
    String source = getConvertedArgument(fn_name,pos);
    
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    String startingIndex = getConvertedArgument(fn_name,pos);

    String length;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        length = getConvertedArgument(fn_name,pos);
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

bool ToLower::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"lower");
}

bool ToUpper::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"upper");
}

bool Translate::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Trim::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool TrimEnd::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool TrimStart::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool URLDecode::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"decodeURLComponent");
}

bool URLEncode::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out,pos,"encodeURLComponent");
}

}
