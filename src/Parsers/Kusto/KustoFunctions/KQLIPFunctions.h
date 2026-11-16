#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
namespace DB
{
class Ipv4Compare : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "ipv4_compare()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Ipv4IsInRange : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "ipv4_is_in_range()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Ipv4IsMatch : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "ipv4_is_match()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Ipv4IsPrivate : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "ipv4_is_private()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Ipv4NetmaskSuffix : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "ipv4_netmask_suffix()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseIpv4 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parse_ipv4()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseIpv4Mask : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parse_ipv4_mask()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Ipv6Compare : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "ipv6_compare()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Ipv6IsMatch : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "ipv6_is_match()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseIpv6 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parse_ipv6()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ParseIpv6Mask : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "parse_ipv6_mask()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class FormatIpv4 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "format_ipv4()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class FormatIpv4Mask : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "format_ipv4_mask()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

}
