#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserDefaultValue : public IParserBase
{
protected:
    const char * getName() const override { return "default value for column"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserColumnIdentifier : public IParserBase
{
protected:
    const char * getName() const override { return "name of defaulted column"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

};
}