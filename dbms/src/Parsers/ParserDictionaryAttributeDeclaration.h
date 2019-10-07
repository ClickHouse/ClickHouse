#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

class ParserDictionaryAttributeDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "attribute declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserDictionaryAttributeDeclarationList : public IParserBase
{
protected:
    const char * getName() const { return "attribute declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
