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

class ASTDeclarePartitionOptions : public IAST
{
public:
    String partition_type;
    ASTPtr partition_numbers;
    ASTPtr partition_expression;
    String subpartition_type;
    ASTPtr subpartition_numbers;
    ASTPtr subpartition_expression;
    ASTPtr declare_partitions;

    ASTPtr clone() const override;

    String getID(char /*delimiter*/) const override { return "partition options declaration"; }

protected:
    void formatImpl(const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        throw Exception("Method formatImpl is not supported by MySQLParser::ASTDeclarePartitionOptions.", ErrorCodes::NOT_IMPLEMENTED);
    }
};

class ParserDeclarePartitionOptions : public IParserBase
{
protected:
    const char * getName() const override { return "partition options declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
