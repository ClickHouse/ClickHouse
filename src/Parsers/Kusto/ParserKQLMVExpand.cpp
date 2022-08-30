#include <Parsers/ASTLiteral.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLMakeSeries.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLMVExpand.h>
#include <Parsers/ParserSetQuery.h>
#include <format>
#include <unordered_map>

namespace DB
{

std::unordered_map<String,String>  ParserKQLMVExpand::type_cast =
{   {"bool", "Boolean"},
    {"boolean", "Boolean"},
    {"datetime", "DateTime"},
    {"date", "DateTime"},
    {"guid", "UUID"},
    {"int", "Int32"},
    {"long", "Int64"},
    {"real", "Float64"},
    {"double", "Float64"},
    {"string", "String"}
};

bool ParserKQLMVExpand::parseColumnArrayExprs(ColumnArrayExprs & column_array_exprs, Pos & pos, Expected & expected)
{
    ParserToken equals(TokenType::Equals);
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);
    ParserToken comma(TokenType::Comma);

    ParserKeyword s_to("to");
    ParserKeyword s_type("typeof");
    uint16_t bracket_count = 0;
    Pos expr_begin_pos = pos;
    Pos expr_end_pos = pos;

    String alias;
    String column_array_expr;
    String to_type;
    --expr_end_pos;
    
    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if(pos->type == TokenType::OpeningRoundBracket)
            ++bracket_count;

        if(pos->type == TokenType::ClosingRoundBracket)
            --bracket_count;

        if (String(pos->begin,pos->end) == "=")
        {
            --pos;
            alias = String(pos->begin, pos->end);
            ++pos;
            ++pos;
            expr_begin_pos = pos;
        }

        auto addColumns = [&]
        {
            column_array_expr = getExprFromToken(String(expr_begin_pos->begin, expr_end_pos->end), pos.max_depth);

            if (alias.empty())
            {
                alias = expr_begin_pos == expr_end_pos ? column_array_expr : String(expr_begin_pos->begin,expr_begin_pos->end) + "_";
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

            if (!s_type.ignore(pos, expected))
                return false;
            if (!open_bracket.ignore(pos, expected))
                return false;
            to_type = String(pos->begin, pos->end);

            ++pos;
            if (!close_bracket.ignore(pos, expected))
                return false;
        }

        if ((pos->type == TokenType::Comma && bracket_count == 0) || String(pos->begin, pos->end) == "limit")
        {
            expr_end_pos = pos;
            --expr_end_pos;
            addColumns();
            expr_begin_pos = pos;
            expr_end_pos = pos;
            ++expr_begin_pos;

            alias.clear();
            column_array_expr.clear();
            to_type.clear();
        }

        if (String(pos->begin, pos->end) == "limit")
            break;

        ++pos;

        if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        {
            if (expr_end_pos < expr_begin_pos)
            {
                expr_end_pos = pos;
                --expr_end_pos;
            }
            addColumns();
        }
    }
    return true;
}


bool ParserKQLMVExpand::parserMVExpand(KQLMVExpand & kql_mv_expand, Pos & pos, Expected & expected)
{
    ParserKeyword s_bagexpansion("bagexpansion");
    ParserKeyword s_kind("kind");
    ParserKeyword s_with_itemindex("with_itemindex");
    ParserKeyword s_limit("limit");

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

String ParserKQLMVExpand::genQuery(KQLMVExpand & kql_mv_expand, String input)
{
    String expand_str;
    String cast_type_column_remove, cast_type_column_rename ;
    String cast_type_column_restore, cast_type_column_restore_name ;
    String row_count_str;
    String extra_columns;
    for (auto column : kql_mv_expand.column_array_exprs)
    {
        if (column.alias == column.column_array_expr)
            expand_str = expand_str.empty() ? String("ARRAY JOIN ") + column.alias : expand_str + "," + column.alias;
        else
        {
            expand_str = expand_str.empty() ? std::format("ARRAY JOIN {} AS {} ",  column.column_array_expr, column.alias): expand_str + std::format(", {} AS {}", column.column_array_expr, column.alias);
            extra_columns = extra_columns + ", " + column.alias;
        }

        if (!column.to_type.empty())
        {  
            cast_type_column_remove = cast_type_column_remove.empty() ? " Except " + column.alias : cast_type_column_remove + " Except " + column.alias ;
            auto rename_str = std::format("accurateCastOrNull({0},'{1}') as {0}_ali",column.alias, type_cast[column.to_type]);
            cast_type_column_rename = cast_type_column_rename.empty() ? rename_str : cast_type_column_rename + "," + rename_str;
            cast_type_column_restore = cast_type_column_restore.empty() ? std::format(" Except {}_ali ", column.alias) : cast_type_column_restore + std::format(" Except {}_ali ", column.alias);
            cast_type_column_restore_name =  cast_type_column_restore_name.empty() ? std::format("{0}_ali as {0}", column.alias ) :cast_type_column_restore_name + std::format(", {0}_ali as {0}", column.alias);
        }

        if (!kql_mv_expand.with_itemindex.empty())
        {
            row_count_str = row_count_str.empty() ? "length("+column.alias+")" : row_count_str + ", length("+column.alias+")";
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

    if (!cast_type_column_remove.empty())
    {
        auto rename_query = std::format("(Select * {}, {} From {})", cast_type_column_remove, cast_type_column_rename, query);
        query = std::format("(Select * {}, {} from {})", cast_type_column_restore, cast_type_column_restore_name, rename_query);
    }
    return query;
}

bool ParserKQLMVExpand::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr tmp_setting;
    if (op_pos.empty())
        return true;

    auto begin = pos;
    String input = table_name;

    for (auto npos : op_pos)
    {
        auto kql_mv_expand = std::make_unique<KQLMVExpand>();
        if (parserMVExpand(*kql_mv_expand, npos, expected))
            input = genQuery(*kql_mv_expand, input);
        else
            return false;
    }

    Tokens token_subquery(input.c_str(), input.c_str() + input.size());
    IParser::Pos pos_subquery(token_subquery, pos.max_depth);

    if (!ParserTablesInSelectQuery().parse(pos_subquery, node, expected))
        return false;

    const String setting_str = "enable_unaligned_array_join = 1";
    Tokens token_settings(setting_str.c_str(), setting_str.c_str() + setting_str.size());
    IParser::Pos pos_settings(token_settings, pos.max_depth);

    if (!ParserSetQuery(true).parse(pos_settings, tmp_setting, expected))
        return false;

    settings = std::move(tmp_setting);
    pos = begin;
    return true;
}

}
