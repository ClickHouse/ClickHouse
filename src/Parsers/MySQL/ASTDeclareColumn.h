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

class ASTDeclareColumn : public IAST
{
public:
    String name;
    ASTPtr data_type;
    ASTPtr column_options;

    ASTPtr clone() const override;

    String getID(char /*delimiter*/) const override { return "Column definition"; }

protected:
    void formatImpl(const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        throw Exception("Method formatImpl is not supported by MySQLParser::ASTDeclareColumn.", ErrorCodes::NOT_IMPLEMENTED);
    }
};

class ParserDeclareColumn : public IParserBase
{
protected:
    const char * getName() const override { return "index declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


}

}
