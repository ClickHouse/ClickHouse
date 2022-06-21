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
    return directMapping(out,pos,"base64Decode");
}

bool Base64DecodeToString::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
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
    std::unique_ptr<IParserKQLFunction> fun;
    String res = String(pos->begin,pos->end);
    ++pos;
    if (pos->type != TokenType::OpeningRoundBracket)
    {
        --pos;
        return false;
    }
    ++pos;
    String value = String(pos->begin,pos->end);
    if (pos->type == TokenType::BareWord )
    {   String func_value;
        fun = KQLFunctionFactory::get(value);
        if (fun && fun->convert(func_value,pos))
            value  = func_value;
    }
    ++pos;
    if (pos->type != TokenType::Comma)
        return false;

    ++pos;
    String multiplier = String(pos->begin,pos->end);
    String new_multiplier;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::BareWord )
        {
            String fun_multiplier;
            fun = KQLFunctionFactory::get(multiplier);
            if ( fun && fun->convert(fun_multiplier,pos))
                new_multiplier += fun_multiplier;
            else
                new_multiplier = multiplier;
        }
        else if (pos->type == TokenType::Comma ||pos->type == TokenType::ClosingRoundBracket) // has delimiter
        {
            break;
        }
        else
            new_multiplier += String(pos->begin,pos->end);
        ++pos;
    }

    if (!new_multiplier.empty())
        multiplier = new_multiplier;

    String delimiter ;
    if (pos->type == TokenType::Comma)
    {
        ++pos;
        delimiter = String(pos->begin,pos->end);
        if (pos->type == TokenType::BareWord )
        {   String func_delimiter;
            fun = KQLFunctionFactory::get(delimiter);
            if (fun && fun->convert(func_delimiter,pos))
                delimiter  = func_delimiter;
        }
        ++pos;
    }
    if (pos->type == TokenType::ClosingRoundBracket)
    {
        if (!delimiter.empty())
        {
            String repeated_str = "repeat(concat("+value+"," + delimiter + ")," + multiplier + ")";
            res = "substr("+ repeated_str + ", 1, length(" + repeated_str + ") - length(" + delimiter + "))";
        }
        else
            res = "repeat("+ value + ", " + multiplier + ")";
        out = res;
        return true;
    }
    return false;
}

bool SubString::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
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
