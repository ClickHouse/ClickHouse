#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>

namespace DB
{

class ASTPreparedStatement : public IAST
{
public:
    ASTPreparedStatement() = default;

    String function_name;
    String function_body;

    String getID(char) const override { return "PreparedStatement"; }

    ASTPtr clone() const override;
};

class ASTExecute : public IAST
{
public:
    ASTExecute() = default;

    String function_name;
    std::vector<String> arguments;

    String getID(char) const override { return "Execute"; }

    ASTPtr clone() const override;
};

class ASTDeallocate : public IAST
{
public:
    ASTDeallocate() = default;

    String function_name;

    String getID(char) const override { return "Deallocate"; }

    ASTPtr clone() const override;
};

class ParserPrepare : public IParserBase
{
public:
    ParserPrepare() = default;

protected:
    const char * getName() const override { return "PREPARE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserExecute : public IParserBase
{
public:
    ParserExecute() = default;

protected:
    const char * getName() const override { return "EXECUTE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserDeallocate : public IParserBase
{
public:
    ParserDeallocate() = default;

protected:
    const char * getName() const override { return "DEALLOCATE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
