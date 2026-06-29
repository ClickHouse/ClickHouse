#pragma once

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

#include <vector>


namespace DB
{

/// Parse one side of an `ALTER CLUSTER ... REPLACE from... TO to...` / `ALTER SHARD ... REPLACE from... TO to...` clause.
///
/// Accepts either a parenthesized list `(id, id, ...)` or a bare comma-separated list `id, id, ...`.
///
/// For the **from** side (`is_to_side = false`), the bare list stops at the first `TO` keyword (the comma before `TO` is
/// restored so the caller sees `TO` next).
///
/// For the **to** side (`is_to_side = true`), it additionally stops at a comma followed by `REPLACE` (next `REPLACE ... TO ...`
/// clause), `ON` (start of `ON CLUSTER`), or `MODIFY PROPERTIES` (trailing shard/cluster properties block) — again, the comma
/// before the terminator is restored.
///
/// Identifiers-only: quoted / backticked identifiers are unquoted via `tryGetIdentifierNameInto`.
inline bool parseSQLClusterReplaceList(std::vector<String> & out, IParser::Pos & pos, Expected & expected, bool is_to_side)
{
    ParserToken s_comma(TokenType::Comma);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserIdentifier name_p;
    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_replace(Keyword::REPLACE);
    ParserKeyword s_properties(Keyword::PROPERTIES);
    ParserKeyword s_modify_kw(Keyword::MODIFY);
    ParserKeyword s_on(Keyword::ON);

    if (s_lparen.ignore(pos, expected))
    {
        ASTPtr id;
        if (!name_p.parse(pos, id, expected))
            return false;
        tryGetIdentifierNameInto(id, out.emplace_back());
        while (s_comma.ignore(pos, expected))
        {
            if (!name_p.parse(pos, id, expected))
                return false;
            tryGetIdentifierNameInto(id, out.emplace_back());
        }
        if (!s_rparen.ignore(pos, expected))
            return false;
        return !out.empty();
    }

    ASTPtr id;
    if (!name_p.parse(pos, id, expected))
        return false;
    tryGetIdentifierNameInto(id, out.emplace_back());

    while (true)
    {
        const auto before_comma = pos;
        if (!s_comma.ignore(pos, expected))
            break;
        if (s_to.check(pos, expected))
        {
            pos = before_comma;
            break;
        }
        if (is_to_side && (s_replace.check(pos, expected) || s_on.check(pos, expected)))
        {
            pos = before_comma;
            break;
        }
        if (is_to_side)
        {
            auto lookahead = pos;
            if (s_modify_kw.ignore(lookahead, expected) && s_properties.check(lookahead, expected))
            {
                pos = before_comma;
                break;
            }
        }
        if (!name_p.parse(pos, id, expected))
            return false;
        tryGetIdentifierNameInto(id, out.emplace_back());
    }
    return true;
}

}
