#include <Parsers/ParserShowColumnsQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

bool ParserShowColumnsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr like;
    ASTPtr from1;
    ASTPtr from2;


    auto query = make_intrusive<ASTShowColumnsQuery>();

    if (!ParserKeyword(Keyword::SHOW).ignore(pos, expected))
        return false;

    if (ParserKeyword(Keyword::EXTENDED).ignore(pos, expected))
        query->extended = true;

    if (ParserKeyword(Keyword::FULL).ignore(pos, expected))
        query->full = true;

    if (!(ParserKeyword(Keyword::COLUMNS).ignore(pos, expected) || ParserKeyword(Keyword::FIELDS).ignore(pos, expected)))
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
    if (table_id->compound())
    {
        const auto & parts = table_id->name_parts;
        query->database = parts[0];
        /// Fold namespace parts into the table name (DataLakeCatalog databases):
        /// catalog.ns1.ns2.table -> table `ns1.ns2.table`
        /// a quoted component with a literal dot would alias another path - reject it
        for (size_t i = 1; i < parts.size(); ++i)
            if (parts.size() > 2 && parts[i].find('.') != String::npos)
                return false;
        query->table = parts[1];
        for (size_t i = 2; i < parts.size(); ++i)
            query->table += "." + parts[i];
    }
    else
    {
        query->table = table_id->shortName();
        if (ParserKeyword(Keyword::FROM).ignore(pos, expected) || ParserKeyword(Keyword::IN).ignore(pos, expected))
            if (!ParserCompoundIdentifier().parse(pos, from2, expected))
                return false;
        if (from2)
        {
            /// keep the operand as one (possibly dotted) database name;
            /// runtime resolution decides between a real database and a namespace
            tryGetIdentifierNameInto(from2, query->database);
        }
    }

    if (ParserKeyword(Keyword::NOT).ignore(pos, expected))
        query->not_like = true;

    if (bool insensitive = ParserKeyword(Keyword::ILIKE).ignore(pos, expected); insensitive || ParserKeyword(Keyword::LIKE).ignore(pos, expected))
    {
        if (insensitive)
            query->case_insensitive_like = true;

        if (!ParserStringLiteral().parse(pos, like, expected))
            return false;
    }
    else if (query->not_like)
        return false;
    else if (ParserKeyword(Keyword::WHERE).ignore(pos, expected))
        if (!ParserExpressionWithOptionalAlias(false).parse(pos, query->where_expression, expected))
            return false;

    if (ParserKeyword(Keyword::LIMIT).ignore(pos, expected))
        if (!ParserExpressionWithOptionalAlias(false).parse(pos, query->limit_length, expected))
            return false;

    if (like)
        query->like = like->as<ASTLiteral &>().value.safeGet<String>();

    node = query;

    return true;
}

}
