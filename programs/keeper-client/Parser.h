#pragma once

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


namespace DB
{

bool parseKeeperArg(IParser::Pos & pos, Expected & expected, String & result);
bool parseKeeperPath(IParser::Pos & pos, Expected & expected, String & path);


class ASTKeeperQuery : public IAST
{
public:
    String getID(char) const override { return "KeeperQuery"; }
    ASTPtr clone() const override { return std::make_shared<ASTKeeperQuery>(*this); }

    String command;
    std::vector<Field> args;
};

class KeeperParser : public IParserBase
{
protected:
    const char * getName() const override { return "Keeper client query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
