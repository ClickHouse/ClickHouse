#include <Parsers/ParserViewTargets.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    bool tryParseViewTarget(ViewTarget::Kind kind, Keyword keyword, IParser::Pos & pos, Expected & expected, boost::intrusive_ptr<ASTViewTargets> & res)
    {
        auto current = pos;

        if (!ParserKeyword{keyword}.ignore(pos, expected))
            return false;

        ASTPtr ast;
        if (!res || (res->getInnerUUID(kind) == UUIDHelpers::Nil))
        {
            if (ParserKeyword{Keyword::INNER_UUID}.ignore(pos, expected))
            {
                if (!ParserStringLiteral{}.parse(pos, ast, expected))
                {
                    pos = current;
                    return false;
                }
                auto inner_uuid = parseFromString<UUID>(ast->as<ASTLiteral>()->value.safeGet<String>());
                if (!res)
                    res = make_intrusive<ASTViewTargets>();
                res->setInnerUUID(kind, inner_uuid);
                return true;
            }
        }

        if (!res || !res->getInnerColumns(kind))
        {
            if (ParserKeyword{Keyword::INNER_COLUMNS}.ignore(pos, expected))
            {
                ASTPtr col_list;
                if (ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected)
                    && ParserColumnDeclarationList{}.parse(pos, col_list, expected)
                    && ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                {
                    auto inner_columns = make_intrusive<ASTColumns>();
                    inner_columns->set(inner_columns->columns, col_list);
                    if (!res)
                        res = make_intrusive<ASTViewTargets>();
                    res->setInnerColumns(kind, inner_columns);
                    return true;
                }
                pos = current;
                return false;
            }
        }

        if (!res || !res->getInnerEngine(kind))
        {
            /// Skip optional INNER before ENGINE.
            /// We support both syntaxes: `TAGS/DATA/METRICS ENGINE` and `TAGS/DATA/METRICS INNER ENGINE`.
            ParserKeyword{Keyword::INNER}.ignore(pos, expected);

            if (ParserStorage{ParserStorage::TABLE_ENGINE}.parse(pos, ast, expected))
            {
                if (!res)
                    res = make_intrusive<ASTViewTargets>();
                res->setInnerEngine(kind, ast);
                return true;
            }
        }

        if (!res || !res->getTableID(kind))
        {
            ParserCompoundIdentifier table_name_p(/*table_name_with_optional_uuid*/ true, /*allow_query_parameter*/ true);
            if (table_name_p.parse(pos, ast, expected))
            {
                auto table_id = ast->as<ASTTableIdentifier>()->getTableId();
                if (!res)
                    res = make_intrusive<ASTViewTargets>();
                res->setTableID(kind, table_id);
                return true;
            }
        }

        pos = current;
        return false;
    }
}

bool ParserViewTargets::canAccept(ViewTarget::Kind kind) const
{
    return std::find(accept_kinds.begin(), accept_kinds.end(), kind) != accept_kinds.end();
}

bool ParserViewTargets::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    boost::intrusive_ptr<ASTViewTargets> res;

    bool stop = false;
    while (!stop)
    {
        bool parsed = false;
        for (auto kind : accept_kinds)
        {
            switch (kind)
            {
                case ViewTarget::Data:
                {
                    parsed |= tryParseViewTarget(kind, Keyword::DATA, pos, expected, res);
                    break;
                }

                case ViewTarget::Tags:
                {
                    parsed |= tryParseViewTarget(kind, Keyword::TAGS, pos, expected, res);
                    break;
                }

                case ViewTarget::Metrics:
                {
                    parsed |= tryParseViewTarget(kind, Keyword::METRICS, pos, expected, res);
                    break;
                }

                case ViewTarget::To:
                case ViewTarget::Inner:
                {
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ParserViewTargets doesn't support {}", kind);
                }
            }
        }
        stop = !parsed; /// We continue parsing while we can.
    }

    if (!res)
        return false;

    node = res;
    return true;
}

}
