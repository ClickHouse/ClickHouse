#pragma once

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

class ASTDeclarePartition : public IAST
{
public:
    String partition_name;
    ASTPtr less_than;
    ASTPtr in_expression;
    ASTPtr options;
    ASTPtr subpartitions;

    ASTPtr clone() const override;

    String getID(char /*delimiter*/) const override { return "partition declaration"; }

protected:
    void formatImpl(WriteBuffer & /*ostr*/, const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method formatImpl is not supported by MySQLParser::ASTDeclarePartition.");
    }
};

class ParserDeclarePartition : public IParserBase
{
protected:
    const char * getName() const override { return "partition declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}

}
