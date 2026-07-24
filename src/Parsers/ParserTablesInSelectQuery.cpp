#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Core/Joins.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserTableExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto res = std::make_shared<ASTTableExpression>();

    if (!ParserWithOptionalAlias(std::make_unique<ParserSubquery>(), allow_alias_without_as_keyword).parse(pos, res->subquery, expected)
        && !ParserWithOptionalAlias(std::make_unique<ParserFunction>(false, true), allow_alias_without_as_keyword).parse(pos, res->table_function, expected)
        && !ParserWithOptionalAlias(std::make_unique<ParserCompoundIdentifier>(true, true), allow_alias_without_as_keyword)
                .parse(pos, res->database_and_table_name, expected)
        && !ParserWithOptionalAlias(std::make_unique<ParserTableAsStringLiteralIdentifier>(), allow_alias_without_as_keyword)
                .parse(pos, res->database_and_table_name, expected))
        return false;

    /// FINAL
    if (ParserKeyword(Keyword::FINAL).ignore(pos, expected))
        res->final = true;

    /// SAMPLE number
    if (ParserKeyword(Keyword::SAMPLE).ignore(pos, expected))
    {
        ParserSampleRatio ratio;

        if (!ratio.parse(pos, res->sample_size, expected))
            return false;

        /// OFFSET number
        if (ParserKeyword(Keyword::OFFSET).ignore(pos, expected))
        {
            if (!ratio.parse(pos, res->sample_offset, expected))
                return false;
        }
    }

    if (res->database_and_table_name)
        res->children.emplace_back(res->database_and_table_name);
    if (res->table_function)
        res->children.emplace_back(res->table_function);
    if (res->subquery)
        res->children.emplace_back(res->subquery);
    if (res->sample_size)
        res->children.emplace_back(res->sample_size);
    if (res->sample_offset)
        res->children.emplace_back(res->sample_offset);

    assert(res->database_and_table_name || res->table_function || res->subquery);

    node = res;
    return true;
}


bool ParserArrayJoin::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto res = std::make_shared<ASTArrayJoin>();

    /// [LEFT] ARRAY JOIN expr list
    Pos saved_pos = pos;
    bool has_array_join = false;

    if (ParserKeyword(Keyword::LEFT_ARRAY_JOIN).ignore(pos, expected))
    {
        res->kind = ASTArrayJoin::Kind::Left;
        has_array_join = true;
    }
    else
    {
        pos = saved_pos;

        /// INNER may be specified explicitly, otherwise it is assumed as default.
        ParserKeyword(Keyword::INNER).ignore(pos, expected);

        if (ParserKeyword(Keyword::ARRAY_JOIN).ignore(pos, expected))
        {
            res->kind = ASTArrayJoin::Kind::Inner;
            has_array_join = true;
        }
    }

    if (!has_array_join)
        return false;

    if (!ParserExpressionList(false).parse(pos, res->expression_list, expected))
        return false;

    if (res->expression_list)
        res->children.emplace_back(res->expression_list);

    node = res;
    return true;
}


static void parseJoinStrictness(IParser::Pos & pos, ASTTableJoin & table_join, Expected & expected)
{
    if (ParserKeyword(Keyword::ANY).ignore(pos, expected))
        table_join.strictness = JoinStrictness::Any;
    else if (ParserKeyword(Keyword::ALL).ignore(pos, expected))
        table_join.strictness = JoinStrictness::All;
    else if (ParserKeyword(Keyword::ASOF).ignore(pos, expected))
        table_join.strictness = JoinStrictness::Asof;
    else if (ParserKeyword(Keyword::SEMI).ignore(pos, expected))
        table_join.strictness = JoinStrictness::Semi;
    else if (ParserKeyword(Keyword::ANTI).ignore(pos, expected) || ParserKeyword(Keyword::ONLY).ignore(pos, expected))
        table_join.strictness = JoinStrictness::Anti;
}

bool ParserTablesInSelectQueryElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto res = std::make_shared<ASTTablesInSelectQueryElement>();

    if (is_first)
    {
        if (!ParserTableExpression(allow_alias_without_as_keyword).parse(pos, res->table_expression, expected))
            return false;
    }
    else if (ParserArrayJoin().parse(pos, res->array_join, expected))
    {
    }
    else
    {
        auto table_join = std::make_shared<ASTTableJoin>();

        if (pos->type == TokenType::Comma)
        {
            ++pos;
            table_join->kind = JoinKind::Comma;
        }
        else
        {
            if (ParserKeyword(Keyword::GLOBAL).ignore(pos, expected))
                table_join->locality = JoinLocality::Global;
            else if (ParserKeyword(Keyword::LOCAL).ignore(pos, expected))
                table_join->locality = JoinLocality::Local;

            table_join->strictness = JoinStrictness::Unspecified;

            /// Legacy: allow JOIN type before JOIN kind
            parseJoinStrictness(pos, *table_join, expected);

            bool no_kind = false;
            if (ParserKeyword(Keyword::INNER).ignore(pos, expected))
                table_join->kind = JoinKind::Inner;
            else if (ParserKeyword(Keyword::LEFT).ignore(pos, expected))
                table_join->kind = JoinKind::Left;
            else if (ParserKeyword(Keyword::RIGHT).ignore(pos, expected))
                table_join->kind = JoinKind::Right;
            else if (ParserKeyword(Keyword::FULL).ignore(pos, expected))
                table_join->kind = JoinKind::Full;
            else if (ParserKeyword(Keyword::CROSS).ignore(pos, expected))
                table_join->kind = JoinKind::Cross;
            else if (ParserKeyword(Keyword::PASTE).ignore(pos, expected))
                table_join->kind = JoinKind::Paste;
            else
                no_kind = true;

            /// Standard position: JOIN type after JOIN kind
            parseJoinStrictness(pos, *table_join, expected);

            /// Optional OUTER keyword for outer joins.
            if (table_join->kind == JoinKind::Left
                || table_join->kind == JoinKind::Right
                || table_join->kind == JoinKind::Full)
            {
                ParserKeyword(Keyword::OUTER).ignore(pos, expected);
            }

            if (no_kind)
            {
                /// Use INNER by default as in another DBMS.
                if (table_join->strictness == JoinStrictness::Semi ||
                    table_join->strictness == JoinStrictness::Anti)
                    table_join->kind = JoinKind::Left;
                else
                    table_join->kind = JoinKind::Inner;
            }

            if (table_join->strictness != JoinStrictness::Unspecified
                && (table_join->kind == JoinKind::Cross || table_join->kind == JoinKind::Paste))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "You must not specify ANY or ALL for {} JOIN.", toString(table_join->kind));

            if ((table_join->strictness == JoinStrictness::Semi || table_join->strictness == JoinStrictness::Anti) &&
                (table_join->kind != JoinKind::Left && table_join->kind != JoinKind::Right))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "SEMI|ANTI JOIN should be LEFT or RIGHT.");

            if (!ParserKeyword(Keyword::JOIN).ignore(pos, expected))
                return false;
        }

        if (!ParserTableExpression(allow_alias_without_as_keyword).parse(pos, res->table_expression, expected))
            return false;

        if (table_join->kind != JoinKind::Comma
            && table_join->kind != JoinKind::Cross && table_join->kind != JoinKind::Paste)
        {
            if (ParserKeyword(Keyword::USING).ignore(pos, expected))
            {
                /// Expression for USING could be in parentheses or not.
                bool in_parens = pos->type == TokenType::OpeningRoundBracket;
                if (in_parens)
                    ++pos;

                if (!ParserExpressionList(false).parse(pos, table_join->using_expression_list, expected))
                    return false;

                if (in_parens)
                {
                    if (pos->type != TokenType::ClosingRoundBracket)
                        return false;
                    ++pos;
                }
            }
            else if (ParserKeyword(Keyword::ON).ignore(pos, expected))
            {
                if (!ParserExpression().parse(pos, table_join->on_expression, expected))
                    return false;
            }
            else
            {
                return false;
            }
        }

        if (table_join->using_expression_list)
            table_join->children.emplace_back(table_join->using_expression_list);
        if (table_join->on_expression)
            table_join->children.emplace_back(table_join->on_expression);

        res->table_join = table_join;
    }

    if (res->table_expression)
        res->children.emplace_back(res->table_expression);
    if (res->table_join)
        res->children.emplace_back(res->table_join);
    if (res->array_join)
        res->children.emplace_back(res->array_join);

    node = res;
    return true;
}


bool ParserTablesInSelectQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto res = std::make_shared<ASTTablesInSelectQuery>();

    ASTPtr child;

    if (ParserTablesInSelectQueryElement(true, allow_alias_without_as_keyword).parse(pos, child, expected))
        res->children.emplace_back(child);
    else
        return false;

    while (ParserTablesInSelectQueryElement(false, allow_alias_without_as_keyword).parse(pos, child, expected))
        res->children.emplace_back(child);

    node = res;
    return true;
}

}
