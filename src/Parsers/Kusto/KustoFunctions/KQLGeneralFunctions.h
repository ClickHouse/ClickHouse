#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
namespace DB
{
class Bin : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "bin()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class BinAt : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "bin_at()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Floor : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "floor()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Iif : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "iif()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Iff : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "iff()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Not : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "not()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MinOf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "min_of()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class MaxOf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "max_of()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Coalesce : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "coalesce()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Case : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "case()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class GeoDistance2Points : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "geo_distance_2points()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class GeoPointToGeohash : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "geo_point_to_geohash()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ReplaceString : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "replace_string()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class URLEncodeComponent : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "url_encode_component()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class PadLeft : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "padleft()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class PadRight : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "padright()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class TrimWs : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "trimws()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseHex : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parsehex()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToHex : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "tohex()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsAscii : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isascii()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsUtf8 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isutf8()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class StrcatArray : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "strcat_array()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class DatetimeUtcToLocal : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "datetime_utc_to_local()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToDateTimeFmt : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "todatetimefmt()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class EndsWith : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "endswith()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Any : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "any()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class RowNumber : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "row_number()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Format : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "format()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class FormatInterp : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "format_interp()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

}
