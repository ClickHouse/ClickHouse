#pragma once

#include <Core/Field.h>
#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>


namespace DB
{

namespace MySQLParser
{

class ASTDeclareIndex: public IAST
{
public:
    String index_name;
    String index_type;
    ASTPtr index_columns;
    ASTPtr index_options;
    ASTPtr reference_definition;

    ASTPtr clone() const override;

    String getID(char /*delimiter*/) const override { return "index declaration"; }
};

class ParserDeclareIndex : public IParserBase
{
protected:
    const char * getName() const override { return "index declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    bool parseDeclareOrdinaryIndex(Pos & pos, String & index_name, String & index_type, Expected & expected);

    bool parseDeclareConstraintIndex(Pos & pos, String & index_name, String & index_type, Expected & expected);

};

}

}
