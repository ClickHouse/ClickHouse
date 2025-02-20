#include <Parsers/ParserDeduceQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/CommonParsers.h>

#include <Parsers/ASTDeduceQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include "Parsers/ExpressionElementParsers.h"


namespace DB
{

bool ParserDeduceQueryColumnsSpecification::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    // Do not allow APPLY and REPLACE transformers.
    // Since we use Columns Transformers only to get list of columns,
    // we can't actually modify content of the columns for deduplication.
    const auto allowed_transformers = ParserColumnsTransformers::ColumnTransformers{ParserColumnsTransformers::ColumnTransformer::EXCEPT};

    return ParserColumnsMatcher(allowed_transformers).parse(pos, node, expected)
        || ParserAsterisk(allowed_transformers).parse(pos, node, expected)
        || ParserIdentifier(false).parse(pos, node, expected);
}


bool ParserDeduceQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_deduce_table(Keyword::DEDUCE_TABLE);
    ParserIdentifier name_p(true);
    ParserKeyword s_by(Keyword::BY);

    ASTPtr table;
    ASTPtr col_to_deduce;

    if (!s_deduce_table.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, table, expected))
        return false;

    if (!s_by.parse(pos, table, expected))
        return false;

    if (!name_p.parse(pos, col_to_deduce, expected))
        return false;

    auto query = std::make_shared<ASTDeduceQuery>();
    node = query;

    query->table = table;
    query->col_to_deduce = col_to_deduce;

    if (table)
        query->children.push_back(table);

    return true;
}


}
