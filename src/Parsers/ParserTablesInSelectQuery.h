#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

struct ASTTableJoin;

/** List of single or multiple JOIN-ed tables or subqueries in SELECT query, with ARRAY JOINs and SAMPLE, FINAL modifiers.
  */
class ParserTablesInSelectQuery : public IParserBase
{
public:
    explicit ParserTablesInSelectQuery(bool allow_alias_without_as_keyword_ = true)
        : allow_alias_without_as_keyword(allow_alias_without_as_keyword_) {}

protected:
    const char * getName() const override { return "table, table function, subquery or list of joined tables"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_alias_without_as_keyword;
};


class ParserTablesInSelectQueryElement : public IParserBase
{
public:
    explicit ParserTablesInSelectQueryElement(bool is_first_, bool allow_alias_without_as_keyword_ = true)
        : is_first(is_first_), allow_alias_without_as_keyword(allow_alias_without_as_keyword_) {}

protected:
    const char * getName() const override { return "table, table function, subquery or list of joined tables"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool is_first;
    bool allow_alias_without_as_keyword;
};


class ParserTableExpression : public IParserBase
{
public:
    explicit ParserTableExpression(bool allow_alias_without_as_keyword_ = true)
        : allow_alias_without_as_keyword(allow_alias_without_as_keyword_) {}

protected:
    const char * getName() const override { return "table or subquery or table function"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_alias_without_as_keyword;
};


class ParserArrayJoin : public IParserBase
{
protected:
    const char * getName() const override { return "array join"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


}
