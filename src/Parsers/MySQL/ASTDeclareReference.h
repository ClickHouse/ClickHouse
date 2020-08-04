#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>

namespace DB
{

namespace MySQLParser
{

class ASTDeclareReference : public IAST
{
public:
    enum MatchKind
    {
        MATCH_FULL,
        MATCH_PARTIAL,
        MATCH_SIMPLE
    };

    enum ReferenceOption
    {
        RESTRICT,
        CASCADE,
        SET_NULL,
        NO_ACTION,
        SET_DEFAULT
    };

    MatchKind kind;
    String reference_table_name;
    ASTPtr reference_expression;
    ReferenceOption on_delete_option;
    ReferenceOption on_update_option;

    ASTPtr clone() const override;

    String getID(char /*delimiter*/) const override { return "subpartition declaration"; }
};

class ParserDeclareReference : public IParserBase
{
protected:
    const char * getName() const override { return "reference declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
