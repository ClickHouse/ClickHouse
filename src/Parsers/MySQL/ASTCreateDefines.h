#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace MySQLParser
{

class ASTCreateDefines : public IAST
{
public:
    ASTExpressionList * columns = nullptr;
    ASTExpressionList * indices = nullptr;
    ASTExpressionList * constraints = nullptr;

    ASTPtr clone() const override;

    String getID(char) const override { return "Create definitions"; }

protected:
    void formatImpl(WriteBuffer & /*ostr*/, const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method formatImpl is not supported by MySQLParser::ASTCreateDefines.");
    }

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&columns));
        f(reinterpret_cast<void **>(&indices));
        f(reinterpret_cast<void **>(&constraints));
    }
};

class ParserCreateDefines : public IParserBase
{
protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    const char * getName() const override { return "table property list (column, index, constraint)"; }
};

}

}
