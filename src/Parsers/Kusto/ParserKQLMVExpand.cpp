#include <format>
#include <unordered_map>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLMVExpand.h>
#include <Parsers/Kusto/ParserKQLMakeSeries.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

namespace DB::ErrorCodes
{
extern const int UNKNOWN_TYPE;
}

namespace DB
{

std::unordered_map<String, String> ParserKQLMVExpand::type_cast
    = {{"bool", "Boolean"},
       {"boolean", "Boolean"},
       {"datetime", "DateTime"},
       {"date", "DateTime"},
       {"guid", "UUID"},
       {"int", "Int32"},
       {"long", "Int64"},
       {"real", "Float64"},
       {"double", "Float64"},
       {"string", "String"}};

bool ParserKQLMVExpand::parseColumnArrayExprs(ColumnArrayExprs & column_array_exprs, Pos & pos, Expected & expected)
{
    ParserToken equals(TokenType::Equals);
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);
    ParserToken comma(TokenType::Comma);

    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_type(Keyword::TYPEOF);
    uint16_t bracket_count = 0;
    Pos expr_begin_pos = pos;
    Pos expr_end_pos = pos;

    String alias;
    String column_array_expr;
    String to_type;
    --expr_end_pos;

    while (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++bracket_count;

        if (pos->type == TokenType::ClosingRoundBracket)
            --bracket_count;

        if (String(pos->begin, pos->end) == "=")
        {
            --pos;
            alias = String(pos->begin, pos->end);
            ++pos;
            ++pos;
            expr_begin_pos = pos;
        }

        auto add_columns = [&]
        {
            column_array_expr = getExprFromToken(String(expr_begin_pos->begin, expr_end_pos->end), pos.max_depth, pos.max_backtracks);

            if (alias.empty())
            {
                alias = expr_begin_pos == expr_end_pos ? column_array_expr : String(expr_begin_pos->begin, expr_begin_pos->end) + "_";
            }
            column_array_exprs.push_back(ColumnArrayExpr(alias, column_array_expr, to_type));
        };

        if (s_to.ignore(pos, expected))
        {
            --pos;
            --pos;
            expr_end_pos = pos;
            ++pos;
            ++pos;

            column_array_expr = String(expr_begin_pos->begin, expr_end_pos->end);

            if (!s_type.ignore(pos, expected))
                return false;
            if (!open_bracket.ignore(pos, expected))
                return false;
            to_type = String(pos->begin, pos->end);

            if (type_cast.find(to_type) == type_cast.end())
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "{} is not a supported kusto data type for mv-expand", to_type);

            ++pos;
            if (!close_bracket.ignore(pos, expected))
                return false;
            --pos;
        }

        if ((pos->type == TokenType::Comma && bracket_count == 0) || String(pos->begin, pos->end) == "limit"
            || pos->type == TokenType::Semicolon)
        {
            if (column_array_expr.empty())
            {
                expr_end_pos = pos;
                --expr_end_pos;
            }
            add_columns();
            expr_begin_pos = pos;
            expr_end_pos = pos;
            ++expr_begin_pos;

            alias.clear();
            column_array_expr.clear();
            to_type.clear();

            if (pos->type == TokenType::Semicolon)
                break;
        }

        if (String(pos->begin, pos->end) == "limit")
            break;
        if (isValidKQLPos(pos))
            ++pos;
        if (!isValidKQLPos(pos) || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        {
            if (expr_end_pos < expr_begin_pos)
            {
                expr_end_pos = pos;
                --expr_end_pos;
            }
            add_columns();
            break;
        }
    }
    return true;
}

bool ParserKQLMVExpand::parserMVExpand(KQLMVExpand & kql_mv_expand, Pos & pos, Expected & expected)
{
    ParserKeyword s_bagexpansion(Keyword::BAGEXPANSION);
    ParserKeyword s_kind(Keyword::KIND);
    ParserKeyword s_with_itemindex(Keyword::WITH_ITEMINDEX);
    ParserKeyword s_limit(Keyword::LIMIT);

    ParserToken equals(TokenType::Equals);
    ParserToken comma(TokenType::Comma);

    auto & column_array_exprs = kql_mv_expand.column_array_exprs;
    auto & bagexpansion = kql_mv_expand.bagexpansion;
    auto & with_itemindex = kql_mv_expand.with_itemindex;
    auto & limit = kql_mv_expand.limit;

    if (s_bagexpansion.ignore(pos, expected))
    {
        if (!equals.ignore(pos, expected))
            return false;
        bagexpansion = String(pos->begin, pos->end);
        ++pos;
    }
    else if (s_kind.ignore(pos, expected))
    {
        if (!equals.ignore(pos, expected))
            return false;
        bagexpansion = String(pos->begin, pos->end);
        ++pos;
    }

    if (s_with_itemindex.ignore(pos, expected))
    {
        if (!equals.ignore(pos, expected))
            return false;
        with_itemindex = String(pos->begin, pos->end);
        ++pos;
    }

    if (!parseColumnArrayExprs(column_array_exprs, pos, expected))
        return false;

    if (s_limit.ignore(pos, expected))
        limit = String(pos->begin, pos->end);

    return true;
}

bool ParserKQLMVExpand::genQuery(KQLMVExpand & kql_mv_expand, ASTPtr & select_node, uint32_t max_depth, uint32_t max_backtracks)
{
    String expand_str;
    String cast_type_column_remove, cast_type_column_rename;
    String cast_type_column_restore, cast_type_column_restore_name;
    String row_count_str;
    String extra_columns;
    String input = "dummy_input";
    for (auto column : kql_mv_expand.column_array_exprs)
    {
        if (column.alias == column.column_array_expr)
            expand_str = expand_str.empty() ? String("ARRAY JOIN ") + column.alias : expand_str + "," + column.alias;
        else
        {
            expand_str = expand_str.empty() ? std::format("ARRAY JOIN {} AS {} ", column.column_array_expr, column.alias)
                                            : expand_str + std::format(", {} AS {}", column.column_array_expr, column.alias);
            extra_columns = extra_columns + ", " + column.alias;
        }

        if (!column.to_type.empty())
        {
            cast_type_column_remove
                = cast_type_column_remove.empty() ? " Except " + column.alias : cast_type_column_remove + " Except " + column.alias;
            String rename_str;

            if (type_cast[column.to_type] == "Boolean")
                rename_str = std::format(
                    "accurateCastOrNull(toInt64OrNull(toString({0})),'{1}') as {0}_ali", column.alias, type_cast[column.to_type]);
            else
                rename_str = std::format("accurateCastOrNull({0},'{1}') as {0}_ali", column.alias, type_cast[column.to_type]);

            cast_type_column_rename = cast_type_column_rename.empty() ? rename_str : cast_type_column_rename + "," + rename_str;
            cast_type_column_restore = cast_type_column_restore.empty()
                ? std::format(" Except {}_ali ", column.alias)
                : cast_type_column_restore + std::format(" Except {}_ali ", column.alias);
            cast_type_column_restore_name = cast_type_column_restore_name.empty()
                ? std::format("{0}_ali as {0}", column.alias)
                : cast_type_column_restore_name + std::format(", {0}_ali as {0}", column.alias);
        }

        if (!kql_mv_expand.with_itemindex.empty())
        {
            row_count_str = row_count_str.empty() ? "length(" + column.alias + ")" : row_count_str + ", length(" + column.alias + ")";
        }
    }

    String columns = "*";
    if (!row_count_str.empty())
    {
        expand_str += std::format(", range(0, arrayMax([{}])) AS {} ", row_count_str, kql_mv_expand.with_itemindex);
        columns = kql_mv_expand.with_itemindex + " , " + columns;
    }

    if (!kql_mv_expand.limit.empty())
        expand_str += " LIMIT " + kql_mv_expand.limit;

    auto query = std::format("(Select {} {} From {} {})", columns, extra_columns, input, expand_str);

    ASTPtr sub_query_node;
    Expected expected;

    if (cast_type_column_remove.empty())
    {
        query = std::format("Select {} {} From {} {}", columns, extra_columns, input, expand_str);
        if (!parseSQLQueryByString(std::make_unique<ParserSelectQuery>(), query, sub_query_node, max_depth, max_backtracks))
            return false;
        if (!setSubQuerySource(sub_query_node, select_node, false, false))
            return false;
        select_node = std::move(sub_query_node);
    }
    else
    {
        query = std::format("(Select {} {} From {} {})", columns, extra_columns, input, expand_str);
        if (!parseSQLQueryByString(std::make_unique<ParserTablesInSelectQuery>(), query, sub_query_node, max_depth, max_backtracks))
            return false;
        if (!setSubQuerySource(sub_query_node, select_node, true, false))
            return false;
        select_node = std::move(sub_query_node);

        auto rename_query = std::format("(Select * {}, {} From {})", cast_type_column_remove, cast_type_column_rename, "query");
        if (!parseSQLQueryByString(std::make_unique<ParserTablesInSelectQuery>(), rename_query, sub_query_node, max_depth, max_backtracks))
            return false;
        if (!setSubQuerySource(sub_query_node, select_node, true, true))
            return false;

        select_node = std::move(sub_query_node);
        query = std::format("Select * {}, {} from {}", cast_type_column_restore, cast_type_column_restore_name, "rename_query");

        if (!parseSQLQueryByString(std::make_unique<ParserSelectQuery>(), query, sub_query_node, max_depth, max_backtracks))
            return false;
        sub_query_node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(select_node));
        select_node = std::move(sub_query_node);
    }
    return true;
}

bool ParserKQLMVExpand::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr setting;
    ASTPtr select_expression_list;
    auto begin = pos;

    KQLMVExpand kql_mv_expand;
    if (!parserMVExpand(kql_mv_expand, pos, expected))
        return false;
    if (!genQuery(kql_mv_expand, node, pos.max_depth, pos.max_backtracks))
        return false;

    const String setting_str = "enable_unaligned_array_join = 1";
    Tokens token_settings(setting_str.data(), setting_str.data() + setting_str.size(), 0, true);
    IParser::Pos pos_settings(token_settings, pos.max_depth, pos.max_backtracks);

    if (!ParserSetQuery(true).parse(pos_settings, setting, expected))
        return false;
    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(setting));

    pos = begin;
    return true;
}

}
