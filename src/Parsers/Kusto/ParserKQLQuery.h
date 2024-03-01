#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{
class ParserKQLBase : public IParserBase
{
public:
    static String getExprFromToken(Pos & pos);
    static String getExprFromPipe(Pos & pos);
    static String getExprFromToken(const String & text, const uint32_t & max_depth);
};

class ParserKQLQuery : public IParserBase
{

protected:
    static std::unique_ptr<IParserBase> getOperator(String &op_name);
    const char * getName() const override { return "KQL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserKQLSubquery : public IParserBase
{
protected:
    const char * getName() const override { return "KQL subquery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
