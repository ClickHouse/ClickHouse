#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserSelectQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SELECT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    /// If this flag is enabled, it will accept queries without SELECT, e.g. 1 + 2,
    /// (without SELECT, WITH is also not allowed; queries starting with FROM are also not allowed without SELECT)
    /// in this case, ClickHouse can be used as a calculator in the command line.
    bool implicit_select = false;

public:
    explicit ParserSelectQuery(bool implicit_select_ = false) : implicit_select(implicit_select_) {}
};

/// Parses the body of an ORDER BY clause (everything after the ORDER BY keyword itself):
/// either ALL with an optional direction and NULLS modifier, or a list of expressions
/// with an optional trailing INTERPOLATE list (allowed when at least one element has WITH FILL).
/// Shared between the ordinary SELECT query and the |> ORDER BY pipe operator.
bool parseOrderByClauseBody(
    IParser::Pos & pos,
    Expected & expected,
    ASTPtr & order_expression_list,
    ASTPtr & interpolate_expression_list,
    bool & order_by_all);

}
