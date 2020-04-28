#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>

namespace DB
{

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
};

class ParserDeclarePartitionOptions : public IParserBase
{
protected:
    const char * getName() const override { return "partition options declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    bool parsePartitionExpression(Pos & pos, std::string & type, ASTPtr & node, Expected & expected, bool subpartition = false);
};

}

}
