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

}
