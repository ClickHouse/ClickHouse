#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserPhysicalizeTypeReferencesQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "PHYSICALIZE TYPE REFERENCES query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserApplyPhysicalizeTypeReferencesQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "PHYSICALIZE TYPE REFERENCES APPLY query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
