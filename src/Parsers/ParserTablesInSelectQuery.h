#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

struct ASTTableJoin;

/** List of single or multiple JOIN-ed tables or subqueries in SELECT query, with ARRAY JOINs and SAMPLE, FINAL modifiers.
  */
class ParserTablesInSelectQuery : public IParserBase
{
protected:
    const char * getName() const override { return "table, table function, subquery or list of joined tables"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserTablesInSelectQueryElement : public IParserBase
{
public:
    explicit ParserTablesInSelectQueryElement(bool is_first_) : is_first(is_first_) {}

protected:
    const char * getName() const override { return "table, table function, subquery or list of joined tables"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool is_first;

    static void parseJoinStrictness(Pos & pos, ASTTableJoin & table_join);
};


class ParserTableExpression : public IParserBase
{
protected:
    const char * getName() const override { return "table or subquery or table function"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserArrayJoin : public IParserBase
{
protected:
    const char * getName() const override { return "array join"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


}
