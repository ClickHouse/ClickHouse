#include <Parsers/ParserShowIndexesQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

#include <fmt/ranges.h>


namespace DB
{

bool ParserShowIndexesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr from1;
    ASTPtr from2;

    auto query = make_intrusive<ASTShowIndexesQuery>();

    if (!ParserKeyword(Keyword::SHOW).ignore(pos, expected))
        return false;

    if (ParserKeyword(Keyword::EXTENDED).ignore(pos, expected))
        query->extended = true;

    if (!(ParserKeyword(Keyword::INDEX).ignore(pos, expected) || ParserKeyword(Keyword::INDEXES).ignore(pos, expected) || ParserKeyword(Keyword::INDICES).ignore(pos, expected) || ParserKeyword(Keyword::KEYS).ignore(pos, expected)))
        return false;

    if (ParserKeyword(Keyword::FROM).ignore(pos, expected) || ParserKeyword(Keyword::IN).ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier().parse(pos, from1, expected))
            return false;
    }
    else
        return false;

    const auto * table_id = from1->as<ASTIdentifier>();
    if (!table_id)
        return false;
    query->table = table_id->shortName();
    if (table_id->compound())
    {
        /// `db.table`, or a hierarchical name `a.b.table` (see `DatabaseCatalog`): the database is all the parts but the last.
        query->database = fmt::format("{}", fmt::join(table_id->name_parts.begin(), table_id->name_parts.end() - 1, "."));
    }
    else
    {
        /// The database, possibly a hierarchical name `a.b`.
        if (ParserKeyword(Keyword::FROM).ignore(pos, expected) || ParserKeyword(Keyword::IN).ignore(pos, expected))
            if (!ParserCompoundIdentifier().parse(pos, from2, expected))
                return false;
        if (from2)
            query->database = from2->as<ASTIdentifier &>().name();
    }

    if (ParserKeyword(Keyword::WHERE).ignore(pos, expected))
        if (!ParserExpressionWithOptionalAlias(false).parse(pos, query->where_expression, expected))
            return false;

    node = query;

    return true;
}

}

