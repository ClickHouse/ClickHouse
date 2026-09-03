#pragma once

#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>

namespace DB
{
class IsNan : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isnan()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Round : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "round()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Abs : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "abs()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Sqrt : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "sqrt()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Pow : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "pow()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Log : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "log()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Log10 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "log10()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Log2 : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "log2()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Sign : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "sign()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsFinite : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isfinite()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class IsInf : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "isinf()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

class Exp : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "exp()"; }
    bool convertImpl(String & out, IParser::Pos & pos) override;
};

}
