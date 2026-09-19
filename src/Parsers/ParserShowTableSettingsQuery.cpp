#include <Parsers/ParserShowTableSettingsQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowTableSettingsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserShowTableSettingsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = make_intrusive<ASTShowTableSettingsQuery>();

    if (!ParserKeyword(Keyword::SHOW).ignore(pos, expected))
        return false;

    /// `SHOW CHANGED SETTINGS` is a different statement, about the session's settings, and
    /// `ParserShowTablesQuery` handles it. This one only matches when `TABLE` follows.
    const bool changed = ParserKeyword(Keyword::CHANGED).ignore(pos, expected);

    /// `TABLE`, not `TABLES`: `SHOW TABLES` is a different statement and its parser rejects the
    /// singular, so the two cannot be confused.
    if (!ParserKeyword(Keyword::TABLE).ignore(pos, expected))
        return false;
    if (!ParserKeyword(Keyword::SETTINGS).ignore(pos, expected))
        return false;

    query->changed = changed;

    if (!ParserKeyword(Keyword::FROM).ignore(pos, expected) && !ParserKeyword(Keyword::IN).ignore(pos, expected))
        return false;

    ASTPtr table_identifier;
    if (!ParserCompoundIdentifier().parse(pos, table_identifier, expected))
        return false;

    /// `ParserCompoundIdentifier` yields an `ASTIdentifier`, not an `ASTTableIdentifier`, so read
    /// the parts rather than casting - the same way `ParserShowColumnsQuery` does.
    const auto * identifier = table_identifier->as<ASTIdentifier>();
    if (!identifier)
        return false;

    query->table = identifier->shortName();
    if (identifier->compound())
        query->database = identifier->name_parts[0];

    if (ParserKeyword(Keyword::NOT).ignore(pos, expected))
        query->not_like = true;

    if (bool insensitive = ParserKeyword(Keyword::ILIKE).ignore(pos, expected);
        insensitive || ParserKeyword(Keyword::LIKE).ignore(pos, expected))
    {
        if (insensitive)
            query->case_insensitive_like = true;

        ASTPtr like;
        if (!ParserStringLiteral().parse(pos, like, expected))
            return false;

        query->like = like->as<ASTLiteral &>().value.safeGet<String>();
        query->has_like = true;
    }
    else if (query->not_like)
    {
        return false;
    }

    node = query;
    return true;
}

}
