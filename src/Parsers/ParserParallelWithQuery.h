#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/// Parses a statement like
///     statement1 PARALLEL WITH statement2 PARALLEL WITH statement3 ... PARALLEL WITH statementN
class ParserParallelWithQuery : public IParserBase
{
public:
    ParserParallelWithQuery(IParser & subquery_parser_, ASTPtr first_subquery_);

protected:
    const char * getName() const override { return "ParallelWithClause"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    IParser & subquery_parser;
    ASTPtr first_subquery;
};

}
