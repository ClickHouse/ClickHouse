#include <Parsers/ParserParallelWithQuery.h>

#include <Parsers/ASTParallelWithQuery.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

ParserParallelWithQuery::ParserParallelWithQuery(IParser & subquery_parser_, ASTPtr first_subquery_)
    : subquery_parser(subquery_parser_), first_subquery(first_subquery_)
{
}


bool ParserParallelWithQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword keyword_parallel_with{Keyword::PARALLEL_WITH};

    auto old_pos = pos;
    if (!keyword_parallel_with.ignore(pos, expected))
        return true;

    ASTs subqueries;
    subqueries.push_back(first_subquery);

    do
    {
        ASTPtr subquery;
        if (!subquery_parser.parse(pos, subquery, expected))
        {
            pos = old_pos;
            break;
        }
        subqueries.push_back(subquery);
        old_pos = pos;
    } while (keyword_parallel_with.ignore(pos, expected));

    auto res = std::make_shared<ASTParallelWithQuery>();
    res->children = std::move(subqueries);
    node = res;

    return true;
}

}
