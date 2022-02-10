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

class ASTDDLTableIdentifier : public IAST
{
public:
    String table;
    String database;

    ASTPtr clone() const override;

    String getID(char) const override { return "table identifier"; }

protected:
    void formatImpl(const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        throw Exception("Method formatImpl is not supported by MySQLParser::ASTDDLTableIdentifier.", ErrorCodes::NOT_IMPLEMENTED);
    }
};

class ParseDDLTableIdentifier : public IParserBase
{
protected:
    const char * getName() const override { return "table identifier"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
