#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <base/extended_types.h>

namespace DB
{
class Base64EncodeToString : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "base64_encode_tostring()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Base64EncodeFromGuid : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "base64_encode_fromguid()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Base64DecodeToString : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "base64_decode_tostring()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Base64DecodeToArray : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "base64_decode_toarray()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Base64DecodeToGuid : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "base64_decode_toguid()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class CountOf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "countof()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Extract : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "extract()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ExtractAll : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "extract_all()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ExtractJSON : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "extract_json(), extractjson()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class HasAnyIndex : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "has_any_index()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IndexOf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "indexof()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsEmpty : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isempty()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsNotEmpty : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isnotempty()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsNotNull : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isnotnull()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsNull : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isnull()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseCommandLine : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parse_command_line()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseCSV : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parse_csv()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseJSON : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parse_json()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseURL : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parse_url()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseURLQuery : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parse_urlquery()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseVersion : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parse_version()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ReplaceRegex : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "replace_regex()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Reverse : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "reverse()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Split : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "split()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class StrCat : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "strcat()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class StrCatDelim : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "strcat_delim()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class StrCmp : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "strcmp()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class StrLen : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "strlen()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class StrRep : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "strrep()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class SubString : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "substring()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToLower : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "tolower()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToUpper : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "toupper()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Translate : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "translate()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Trim : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "trim()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class TrimEnd : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "trim_end()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class TrimStart : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "trim_start()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class URLDecode : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "url_decode()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class URLEncode : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "url_encode()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

}
