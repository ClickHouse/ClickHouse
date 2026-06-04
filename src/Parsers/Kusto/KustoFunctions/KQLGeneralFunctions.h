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

}
