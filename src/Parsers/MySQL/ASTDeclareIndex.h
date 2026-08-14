#pragma once

#include <Core/Field.h>
#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

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

protected:
    void formatImpl(WriteBuffer & /*ostr*/, const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method formatImpl is not supported by MySQLParser::ASTDeclareIndex.");
    }
};

class ParserDeclareIndex : public IParserBase
{
protected:
    const char * getName() const override { return "index declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
