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

class ASTDeclareSubPartition : public IAST
{
public:
    ASTPtr options;
    String logical_name;

    ASTPtr clone() const override;

    String getID(char /*delimiter*/) const override { return "subpartition declaration"; }

protected:
    void formatImpl(const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        throw Exception("Method formatImpl is not supported by MySQLParser::ASTDeclareSubPartition.", ErrorCodes::NOT_IMPLEMENTED);
    }
};

class ParserDeclareSubPartition : public IParserBase
{
protected:
    const char * getName() const override { return "subpartition declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
