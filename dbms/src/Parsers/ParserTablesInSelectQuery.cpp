#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserTablesInSelectQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserTableExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;
    auto res = std::make_shared<ASTTableExpression>();

    ws.ignore(pos, end);

    if (ParserWithOptionalAlias(std::make_unique<ParserSubquery>(), true)
        .parse(pos, end, res->subquery, max_parsed_pos, expected))
    {
    }
    else if (ParserWithOptionalAlias(std::make_unique<ParserFunction>(), true)
        .parse(pos, end, res->table_function, max_parsed_pos, expected))
    {
        static_cast<ASTFunction &>(*res->table_function).kind = ASTFunction::TABLE_FUNCTION;
    }
    else if (ParserWithOptionalAlias(std::make_unique<ParserCompoundIdentifier>(), true)
        .parse(pos, end, res->database_and_table_name, max_parsed_pos, expected))
    {
        static_cast<ASTIdentifier &>(*res->database_and_table_name).kind = ASTIdentifier::Table;
    }
    else
        return false;

    ws.ignore(pos, end);

    /// FINAL
    if (ParserString("FINAL", true, true).ignore(pos, end, max_parsed_pos, expected))
        res->final = true;

    ws.ignore(pos, end);

    /// SAMPLE number
    if (ParserString("SAMPLE", true, true).ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        ParserSampleRatio ratio;

        if (!ratio.parse(pos, end, res->sample_size, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        /// OFFSET number
        if (ParserString("OFFSET", true, true).ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end);

            if (!ratio.parse(pos, end, res->sample_offset, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);
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

    node = res;
    return true;
}


bool ParserArrayJoin::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;
    auto res = std::make_shared<ASTArrayJoin>();

    ws.ignore(pos, end);

    /// [LEFT] ARRAY JOIN expr list
    Pos saved_pos = pos;
    bool has_array_join = false;

    if (ParserString("LEFT", true, true).ignore(pos, end, max_parsed_pos, expected)
        && ws.ignore(pos, end)
        && ParserString("ARRAY", true, true).ignore(pos, end, max_parsed_pos, expected)
        && ws.ignore(pos, end)
        && ParserString("JOIN", true, true).ignore(pos, end, max_parsed_pos, expected))
    {
        res->kind = ASTArrayJoin::Kind::Left;
        has_array_join = true;
    }
    else
    {
        pos = saved_pos;

        /// INNER may be specified explicitly, otherwise it is assumed as default.
        ParserString("INNER", true, true).ignore(pos, end, max_parsed_pos, expected)
            && ws.ignore(pos, end);

        if (ParserString("ARRAY", true, true).ignore(pos, end, max_parsed_pos, expected)
            && ws.ignore(pos, end)
            && ParserString("JOIN", true, true).ignore(pos, end, max_parsed_pos, expected))
        {
            res->kind = ASTArrayJoin::Kind::Inner;
            has_array_join = true;
        }
    }

    if (!has_array_join)
        return false;

    ws.ignore(pos, end);

    if (!ParserExpressionList(false).parse(pos, end, res->expression_list, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (res->expression_list)
        res->children.emplace_back(res->expression_list);

    node = res;
    return true;
}


bool ParserTablesInSelectQueryElement::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;
    auto res = std::make_shared<ASTTablesInSelectQueryElement>();

    ws.ignore(pos, end);

    if (ParserArrayJoin().parse(pos, end, res->array_join, max_parsed_pos, expected))
    {
    }
    else if (is_first)
    {
        if (!ParserTableExpression().parse(pos, end, res->table_expression, max_parsed_pos, expected))
            return false;
    }
    else
    {
        auto table_join = std::make_shared<ASTTableJoin>();

        if (ParserString(",").ignore(pos, end, max_parsed_pos, expected))
        {
            table_join->kind = ASTTableJoin::Kind::Comma;
        }
        else
        {
            if (ParserString("GLOBAL", true, true).ignore(pos, end))
                table_join->locality = ASTTableJoin::Locality::Global;
            else if (ParserString("LOCAL", true, true).ignore(pos, end))
                table_join->locality = ASTTableJoin::Locality::Local;

            ws.ignore(pos, end);

            if (ParserString("ANY", true, true).ignore(pos, end))
                table_join->strictness = ASTTableJoin::Strictness::Any;
            else if (ParserString("ALL", true, true).ignore(pos, end))
                table_join->strictness = ASTTableJoin::Strictness::All;

            ws.ignore(pos, end);

            if (ParserString("INNER", true, true).ignore(pos, end))
                table_join->kind = ASTTableJoin::Kind::Inner;
            else if (ParserString("LEFT", true, true).ignore(pos, end))
                table_join->kind = ASTTableJoin::Kind::Left;
            else if (ParserString("RIGHT", true, true).ignore(pos, end))
                table_join->kind = ASTTableJoin::Kind::Right;
            else if (ParserString("FULL", true, true).ignore(pos, end))
                table_join->kind = ASTTableJoin::Kind::Full;
            else if (ParserString("CROSS", true, true).ignore(pos, end))
                table_join->kind = ASTTableJoin::Kind::Cross;
            else
            {
                /// Maybe need use INNER by default as in another DBMS.
                expected = "INNER|LEFT|RIGHT|FULL|CROSS";
                return false;
            }

            if (table_join->strictness != ASTTableJoin::Strictness::Unspecified
                && table_join->kind == ASTTableJoin::Kind::Cross)
                throw Exception("You must not specify ANY or ALL for CROSS JOIN.", ErrorCodes::SYNTAX_ERROR);

            ws.ignore(pos, end);

            /// Optional OUTER keyword for outer joins.
            if (table_join->kind == ASTTableJoin::Kind::Left
                || table_join->kind == ASTTableJoin::Kind::Right
                || table_join->kind == ASTTableJoin::Kind::Full)
            {
                ParserString("OUTER", true, true).ignore(pos, end)
                    && ws.ignore(pos, end);
            }

            if (!ParserString("JOIN", true, true).ignore(pos, end, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);
        }

        if (!ParserTableExpression().parse(pos, end, res->table_expression, max_parsed_pos, expected))
            return false;

        if (table_join->kind != ASTTableJoin::Kind::Comma
            && table_join->kind != ASTTableJoin::Kind::Cross)
        {
            ws.ignore(pos, end);

            if (ParserString("USING", true, true).ignore(pos, end, max_parsed_pos, expected))
            {
                ws.ignore(pos, end);

                /// Expression for USING could be in parentheses or not.
                bool in_parens = ParserString("(").ignore(pos, end);
                if (in_parens)
                    ws.ignore(pos, end);

                if (!ParserExpressionList(false).parse(pos, end, table_join->using_expression_list, max_parsed_pos, expected))
                    return false;

                if (in_parens)
                {
                    ws.ignore(pos, end);
                    if (!ParserString(")").ignore(pos, end))
                        return false;
                }

                ws.ignore(pos, end);
            }
            else if (ParserString("ON", true, true).ignore(pos, end, max_parsed_pos, expected))
            {
                ws.ignore(pos, end);

                /// OR is operator with lowest priority, so start parsing from it.
                if (!ParserLogicalOrExpression().parse(pos, end, table_join->on_expression, max_parsed_pos, expected))
                    return false;

                ws.ignore(pos, end);
            }
            else
            {
                expected = "USING or ON";
                return false;
            }
        }

        if (table_join->using_expression_list)
            table_join->children.emplace_back(table_join->using_expression_list);
        if (table_join->on_expression)
            table_join->children.emplace_back(table_join->on_expression);

        res->table_join = table_join;
    }

    ws.ignore(pos, end);

    if (res->table_expression)
        res->children.emplace_back(res->table_expression);
    if (res->table_join)
        res->children.emplace_back(res->table_join);
    if (res->array_join)
        res->children.emplace_back(res->array_join);

    node = res;
    return true;
}


bool ParserTablesInSelectQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    auto res = std::make_shared<ASTTablesInSelectQuery>();

    ASTPtr child;

    if (ParserTablesInSelectQueryElement(true).parse(pos, end, child, max_parsed_pos, expected))
        res->children.emplace_back(child);
    else
        return false;

    while (ParserTablesInSelectQueryElement(false).parse(pos, end, child, max_parsed_pos, expected))
        res->children.emplace_back(child);

    node = res;
    return true;
}

}
