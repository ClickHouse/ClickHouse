#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserProjectionColumnDeclaration : public IParserBase
{
protected:
    const char * getName() const  override{ return "projection column declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserProjectionDescriptionDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "projection description (column, index) declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserProjectionDescriptionDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "projection columns and indices"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserProjectionDescription : public IParserBase
{
protected:
    const char * getName() const override { return "PROJECTION description"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserProjectionSelectQuery : public IParserBase
{
protected:
    const char * getName() const override { return "PROJECTION SELECT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
