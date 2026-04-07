#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>

namespace DB
{
class ToBool : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "tobool()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToDateTime : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "todatetime()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToDouble : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "todouble()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToInt : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "toint()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToLong : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "tolong()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToString : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "tostring()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToTimeSpan : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "totimespan()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class ToDecimal : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "todecimal()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};
}
