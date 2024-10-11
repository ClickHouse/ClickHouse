#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>
#include <Parsers/Kusto/ParserKQLDistinct.h>
#include <Parsers/Kusto/ParserKQLExtend.h>
#include <Parsers/Kusto/ParserKQLFilter.h>
#include <Parsers/Kusto/ParserKQLLimit.h>
#include <Parsers/Kusto/ParserKQLMVExpand.h>
#include <Parsers/Kusto/ParserKQLMakeSeries.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLPrint.h>
#include <Parsers/Kusto/ParserKQLProject.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLSort.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/ParserKQLSummarize.h>
#include <Parsers/Kusto/ParserKQLTable.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

#include <format>
namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool ParserKQLBase::parseByString(String expr, ASTPtr & node, uint32_t max_depth, uint32_t max_backtracks)
{
    Expected expected;

    Tokens tokens(expr.data(), expr.data() + expr.size(), 0, true);
    IParser::Pos pos(tokens, max_depth, max_backtracks);
    return parse(pos, node, expected);
}

bool ParserKQLBase::parseSQLQueryByString(ParserPtr && parser, String & query, ASTPtr & select_node, uint32_t max_depth, uint32_t max_backtracks)
{
    Expected expected;
    Tokens token_subquery(query.data(), query.data() + query.size(), 0, true);
    IParser::Pos pos_subquery(token_subquery, max_depth, max_backtracks);
    if (!parser->parse(pos_subquery, select_node, expected))
        return false;
    return true;
};

bool ParserKQLBase::setSubQuerySource(ASTPtr & select_query, ASTPtr & source, bool dest_is_subquery, bool src_is_subquery)
{
    ASTPtr table_expr;
    if (!dest_is_subquery)
    {
        if (!select_query || !select_query->as<ASTSelectQuery>()->tables()
            || select_query->as<ASTSelectQuery>()->tables()->as<ASTTablesInSelectQuery>()->children.empty())
            return false;
        table_expr = select_query->as<ASTSelectQuery>()->tables()->as<ASTTablesInSelectQuery>()->children.at(0);
        table_expr->as<ASTTablesInSelectQueryElement>()->table_expression
            = source->as<ASTSelectQuery>()->tables()->children.at(0)->as<ASTTablesInSelectQueryElement>()->table_expression;
        table_expr->children.at(0) = table_expr->as<ASTTablesInSelectQueryElement>()->table_expression;
        return true;
    }

    if (!select_query || select_query->as<ASTTablesInSelectQuery>()->children.empty()
        || !select_query->as<ASTTablesInSelectQuery>()->children.at(0)->as<ASTTablesInSelectQueryElement>()->table_expression
        || select_query->as<ASTTablesInSelectQuery>()
               ->children.at(0)
               ->as<ASTTablesInSelectQueryElement>()
               ->table_expression->as<ASTTableExpression>()
               ->subquery->children.empty()
        || select_query->as<ASTTablesInSelectQuery>()
               ->children.at(0)
               ->as<ASTTablesInSelectQueryElement>()
               ->table_expression->as<ASTTableExpression>()
               ->subquery->children.at(0)
               ->as<ASTSelectWithUnionQuery>()
               ->list_of_selects->children.empty()
        || select_query->as<ASTTablesInSelectQuery>()
               ->children.at(0)
               ->as<ASTTablesInSelectQueryElement>()
               ->table_expression->as<ASTTableExpression>()
               ->subquery->children.at(0)
               ->as<ASTSelectWithUnionQuery>()
               ->list_of_selects->children.at(0)
               ->as<ASTSelectQuery>()
               ->tables()
               ->as<ASTTablesInSelectQuery>()
               ->children.empty())
        return false;

    table_expr = select_query->as<ASTTablesInSelectQuery>()
                     ->children.at(0)
                     ->as<ASTTablesInSelectQueryElement>()
                     ->table_expression->as<ASTTableExpression>()
                     ->subquery->children.at(0)
                     ->as<ASTSelectWithUnionQuery>()
                     ->list_of_selects->children.at(0)
                     ->as<ASTSelectQuery>()
                     ->tables()
                     ->as<ASTTablesInSelectQuery>()
                     ->children.at(0);

    if (!src_is_subquery)
    {
        table_expr->as<ASTTablesInSelectQueryElement>()->table_expression
            = source->as<ASTSelectQuery>()->tables()->children.at(0)->as<ASTTablesInSelectQueryElement>()->table_expression;
    }
    else
    {
        table_expr->as<ASTTablesInSelectQueryElement>()->table_expression
            = source->children.at(0)->as<ASTTablesInSelectQueryElement>()->table_expression;
    }

    table_expr->children.at(0) = table_expr->as<ASTTablesInSelectQueryElement>()->table_expression;
    return true;
}

String ParserKQLBase::getExprFromToken(const String & text, uint32_t max_depth, uint32_t max_backtracks)
{
    Tokens tokens(text.data(), text.data() + text.size(), 0, true);
    IParser::Pos pos(tokens, max_depth, max_backtracks);

    return getExprFromToken(pos);
}

String ParserKQLBase::getExprFromPipe(Pos & pos)
{
    BracketCount bracket_count;
    auto end = pos;
    while (isValidKQLPos(end) && end->type != TokenType::Semicolon)
    {
        bracket_count.count(end);
        if (end->type == TokenType::PipeMark && bracket_count.isZero())
            break;

        ++end;
    }
    if (end != pos)
        --end;
    return (pos <= end) ? String(pos->begin, end->end) : "";
}

String ParserKQLBase::getExprFromToken(Pos & pos)
{
    std::vector<Pos> comma_pos;
    comma_pos.push_back(pos);

    size_t paren_count = 0;
    while (isValidKQLPos(pos) && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::PipeMark && paren_count == 0)
            break;

        if (pos->type == TokenType::OpeningRoundBracket)
            ++paren_count;
        if (pos->type == TokenType::ClosingRoundBracket)
            --paren_count;

        if (pos->type == TokenType::Comma && paren_count == 0)
        {
            ++pos;
            comma_pos.push_back(pos);
            --pos;
        }
        ++pos;
    }

    std::vector<String> columns;
    auto set_columns = [&](Pos & start_pos, Pos & end_pos)
    {
        bool has_alias = false;
        auto equal_pos = start_pos;
        auto columms_start_pos = start_pos;
        auto it_pos = start_pos;
        if (String(it_pos->begin, it_pos->end) == "=")
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid equal symbol (=)");

        BracketCount bracket_count;
        while (it_pos < end_pos)
        {
            bracket_count.count(it_pos);
            if (String(it_pos->begin, it_pos->end) == "=")
            {
                ++it_pos;
                if (String(it_pos->begin, it_pos->end) != "~" && bracket_count.isZero())
                {
                    if (has_alias)
                        throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid equal symbol (=)");
                    has_alias = true;
                }

                --it_pos;
                if (equal_pos == start_pos)
                    equal_pos = it_pos;
            }
            ++it_pos;
        }

        if (has_alias)
        {
            columms_start_pos = equal_pos;
            ++columms_start_pos;
        }
        String column_str;
        String function_name;
        std::vector<String> tokens;

        while (columms_start_pos < end_pos)
        {
            if (!KQLOperators::convert(tokens, columms_start_pos))
            {
                if (columms_start_pos->type == TokenType::BareWord && function_name.empty())
                    function_name = String(columms_start_pos->begin, columms_start_pos->end);

                auto expr = IParserKQLFunction::getExpression(columms_start_pos);
                tokens.push_back(expr);
            }
            ++columms_start_pos;
        }

        for (const auto & token : tokens)
            column_str = column_str.empty() ? token : column_str + " " + token;

        if (has_alias)
        {
            --equal_pos;
            if (start_pos == equal_pos)
            {
                String new_column_str;
                if (start_pos->type != TokenType::BareWord)
                    throw Exception(
                        ErrorCodes::SYNTAX_ERROR, "{} is not a valid alias", std::string_view(start_pos->begin, start_pos->end));

                if (function_name == "array_sort_asc" || function_name == "array_sort_desc")
                    new_column_str = std::format("{0}[1] AS {1}", column_str, String(start_pos->begin, start_pos->end));
                else
                    new_column_str = std::format("{0} AS {1}", column_str, String(start_pos->begin, start_pos->end));

                columns.push_back(new_column_str);
            }
            else
            {
                String whole_alias(start_pos->begin, equal_pos->end);

                if (function_name != "array_sort_asc" && function_name != "array_sort_desc")
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "{} is not a valid alias", whole_alias);

                if (start_pos->type != TokenType::OpeningRoundBracket && equal_pos->type != TokenType::ClosingRoundBracket)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "{} is not a valid alias for {}", whole_alias, function_name);

                String alias_inside;
                bool comma_meet = false;
                size_t index = 1;
                ++start_pos;
                while (start_pos < equal_pos)
                {
                    if (start_pos->type == TokenType::Comma)
                    {
                        alias_inside.clear();
                        if (comma_meet)
                            throw Exception(ErrorCodes::SYNTAX_ERROR, "{} has invalid alias for {}", whole_alias, function_name);
                        comma_meet = true;
                    }
                    else
                    {
                        if (!alias_inside.empty() || start_pos->type != TokenType::BareWord)
                            throw Exception(ErrorCodes::SYNTAX_ERROR, "{} has invalid alias for {}", whole_alias, function_name);

                        alias_inside = String(start_pos->begin, start_pos->end);
                        auto new_column_str = std::format("{0}[{1}] AS {2}", column_str, index, alias_inside);
                        columns.push_back(new_column_str);
                        comma_meet = false;
                        ++index;
                    }
                    ++start_pos;
                }
            }
        }
        else
            columns.push_back(column_str);
    };

    size_t cloumn_size = comma_pos.size();
    for (size_t i = 0; i < cloumn_size; ++i)
    {
        if (i == cloumn_size - 1)
            set_columns(comma_pos[i], pos);
        else
        {
            auto end_pos = comma_pos[i + 1];
            --end_pos;
            set_columns(comma_pos[i], end_pos);
        }
    }

    String res;
    for (const auto & token : columns)
        res = res.empty() ? token : res + "," + token;
    return res;
}

std::unique_ptr<IParserBase> ParserKQLQuery::getOperator(String & op_name)
{
    if (op_name == "filter" || op_name == "where")
        return std::make_unique<ParserKQLFilter>();
    if (op_name == "limit" || op_name == "take")
        return std::make_unique<ParserKQLLimit>();
    if (op_name == "project")
        return std::make_unique<ParserKQLProject>();
    if (op_name == "distinct")
        return std::make_unique<ParserKQLDistinct>();
    if (op_name == "extend")
        return std::make_unique<ParserKQLExtend>();
    if (op_name == "sort by" || op_name == "order by")
        return std::make_unique<ParserKQLSort>();
    if (op_name == "summarize")
        return std::make_unique<ParserKQLSummarize>();
    if (op_name == "table")
        return std::make_unique<ParserKQLTable>();
    if (op_name == "make-series")
        return std::make_unique<ParserKQLMakeSeries>();
    if (op_name == "mv-expand")
        return std::make_unique<ParserKQLMVExpand>();
    if (op_name == "print")
        return std::make_unique<ParserKQLPrint>();
    return nullptr;
}

bool ParserKQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    struct KQLOperatorDataFlowState
    {
        String operator_name;
        bool need_input;
        bool gen_output;
        int8_t backspace_steps; // how many steps to last token of previous pipe
    };

    auto select_query = std::make_shared<ASTSelectQuery>();
    node = select_query;
    ASTPtr tables;

    std::unordered_map<std::string, KQLOperatorDataFlowState> kql_parser
        = {{"filter", {"filter", false, false, 3}},
           {"where", {"filter", false, false, 3}},
           {"limit", {"limit", false, true, 3}},
           {"take", {"limit", false, true, 3}},
           {"project", {"project", false, false, 3}},
           {"distinct", {"distinct", false, true, 3}},
           {"extend", {"extend", true, true, 3}},
           {"sort by", {"order by", false, false, 4}},
           {"order by", {"order by", false, false, 4}},
           {"table", {"table", false, false, 3}},
           {"print", {"print", false, true, 3}},
           {"summarize", {"summarize", true, true, 3}},
           {"make-series", {"make-series", true, true, 5}},
           {"mv-expand", {"mv-expand", true, true, 5}}};

    std::vector<std::pair<String, Pos>> operation_pos;

    String table_name(pos->begin, pos->end);

    if (table_name == "print")
        operation_pos.emplace_back(table_name, pos);
    else
        operation_pos.emplace_back("table", pos);

    ++pos;

    uint16_t bracket_count = 0;

    while (isValidKQLPos(pos) && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++bracket_count;
        if (pos->type == TokenType::ClosingRoundBracket)
            --bracket_count;

        if (pos->type == TokenType::PipeMark && bracket_count == 0)
        {
            ++pos;
            if (!isValidKQLPos(pos))
                return false;

            String kql_operator(pos->begin, pos->end);

            auto validate_kql_operator = [&]
            {
                if (kql_operator == "order" || kql_operator == "sort")
                {
                    ++pos;
                    if (!isValidKQLPos(pos))
                        return false;

                    ParserKeyword s_by(Keyword::BY);
                    if (s_by.ignore(pos, expected))
                    {
                        kql_operator = "order by";
                        --pos;
                    }
                }
                else
                {
                    auto op_pos_begin = pos;
                    ++pos;
                    if (!isValidKQLPos(pos))
                        return false;

                    ParserToken s_dash(TokenType::Minus);
                    if (s_dash.ignore(pos, expected))
                    {
                        if (!isValidKQLPos(pos))
                            return false;
                        kql_operator = String(op_pos_begin->begin, pos->end);
                    }
                    else
                        --pos;
                }
                if (kql_parser.find(kql_operator) == kql_parser.end())
                    return false;
                return true;
            };

            if (!validate_kql_operator())
                return false;
            ++pos;
            if (!isValidKQLPos(pos))
                return false;

            operation_pos.push_back(std::make_pair(kql_operator, pos));
        }
        else
            ++pos;
    }

    auto kql_operator_str = operation_pos.back().first;
    auto npos = operation_pos.back().second;
    if (!npos.isValid())
        return false;

    auto kql_operator_p = getOperator(kql_operator_str);

    if (!kql_operator_p)
        return false;

    if (operation_pos.size() == 1)
    {
        if (kql_operator_str == "print")
        {
            ++npos;
            if (!ParserKQLPrint().parse(npos, node, expected))
                return false;
        }
        else if (kql_operator_str == "table")
        {
            if (!kql_operator_p->parse(npos, node, expected))
                return false;
        }
    }
    else if (operation_pos.size() == 2 && operation_pos.front().first == "table")
    {
        npos = operation_pos.front().second;
        if (!ParserKQLTable().parse(npos, node, expected))
            return false;

        npos = operation_pos.back().second;
        if (!kql_operator_p->parse(npos, node, expected))
            return false;
    }
    else
    {
        String project_clause, order_clause, where_clause, limit_clause;
        auto last_pos = operation_pos.back().second;
        auto last_op = operation_pos.back().first;

        auto set_main_query_clause = [&](const String & op, Pos & op_pos)
        {
            auto op_str = ParserKQLBase::getExprFromPipe(op_pos);
            if (op == "project")
                project_clause = op_str;
            else if (op == "where" || op == "filter")
                where_clause = where_clause.empty() ? std::format("({})", op_str) : where_clause + std::format("AND ({})", op_str);
            else if (op == "limit" || op == "take")
                limit_clause = op_str;
            else if (op == "order by" || op == "sort by")
                order_clause = order_clause.empty() ? op_str : order_clause + "," + op_str;
        };

        set_main_query_clause(last_op, last_pos);

        operation_pos.pop_back();

        if (!kql_parser[last_op].need_input)
        {
            while (!operation_pos.empty())
            {
                auto prev_op = operation_pos.back().first;
                auto prev_pos = operation_pos.back().second;

                if (kql_parser[prev_op].gen_output)
                    break;
                if (!project_clause.empty() && prev_op == "project")
                    break;

                set_main_query_clause(prev_op, prev_pos);
                operation_pos.pop_back();
                last_op = prev_op;
                last_pos = prev_pos;
            }
        }

        if (!operation_pos.empty())
        {
            for (auto i = 0; i < kql_parser[last_op].backspace_steps; ++i)
                --last_pos;

            String sub_query = std::format("({})", String(operation_pos.front().second->begin, last_pos->end));
            Tokens token_subquery(sub_query.data(), sub_query.data() + sub_query.size(), 0, true);
            IParser::Pos pos_subquery(token_subquery, pos.max_depth, pos.max_backtracks);

            if (!ParserKQLSubquery().parse(pos_subquery, tables, expected))
                return false;
            select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
        }
        else
        {
            if (!ParserKQLTable().parse(last_pos, node, expected))
                return false;
        }

        if (!kql_operator_p->parse(npos, node, expected))
            return false;

        auto set_query_clasue = [&](String op_str, String op_calsue)
        {
            auto oprator = getOperator(op_str);
            if (oprator)
            {
                Tokens token_clause(op_calsue.data(), op_calsue.data() + op_calsue.size(), 0, true);
                IParser::Pos pos_clause(token_clause, pos.max_depth, pos.max_backtracks);
                if (!oprator->parse(pos_clause, node, expected))
                    return false;
            }
            return true;
        };

        if (!node->as<ASTSelectQuery>()->select())
        {
            if (project_clause.empty())
                project_clause = "*";
            if (!set_query_clasue("project", project_clause))
                return false;
        }

        if (!order_clause.empty())
            if (!set_query_clasue("order by", order_clause))
                return false;

        if (!where_clause.empty())
            if (!set_query_clasue("where", where_clause))
                return false;

        if (!limit_clause.empty())
            if (!set_query_clasue("limit", limit_clause))
                return false;
        return true;
    }

    if (!node->as<ASTSelectQuery>()->select())
    {
        auto expr = String("*");
        Tokens tokens(expr.data(), expr.data() + expr.size(), 0, true);
        IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);
        if (!std::make_unique<ParserKQLProject>()->parse(new_pos, node, expected))
            return false;
    }

    return true;
}

bool ParserKQLSubquery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_node;

    if (!ParserKQLTableFunction().parse(pos, select_node, expected))
        return false;

    ASTPtr node_subquery = std::make_shared<ASTSubquery>(std::move(select_node));

    ASTPtr node_table_expr = std::make_shared<ASTTableExpression>();
    node_table_expr->as<ASTTableExpression>()->subquery = node_subquery;

    node_table_expr->children.emplace_back(node_subquery);

    ASTPtr node_table_in_select_query_element = std::make_shared<ASTTablesInSelectQueryElement>();
    node_table_in_select_query_element->as<ASTTablesInSelectQueryElement>()->table_expression = node_table_expr;

    ASTPtr res = std::make_shared<ASTTablesInSelectQuery>();

    res->children.emplace_back(node_table_in_select_query_element);

    node = res;
    return true;
}

bool ParserSimpleCHSubquery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr sub_select_node;
    ParserSelectWithUnionQuery select;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    if (!select.parse(pos, sub_select_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    if (parent_select_node && parent_select_node->as<ASTSelectQuery>()->tables())
    {
        auto select_query = sub_select_node->as<ASTSelectWithUnionQuery>()->list_of_selects->children[0];
        select_query->as<ASTSelectQuery>()->setExpression(
            ASTSelectQuery::Expression::TABLES, parent_select_node->as<ASTSelectQuery>()->tables());
    }

    ASTPtr node_subquery = std::make_shared<ASTSubquery>(std::move(sub_select_node));

    ASTPtr node_table_expr = std::make_shared<ASTTableExpression>();
    node_table_expr->as<ASTTableExpression>()->subquery = node_subquery;

    node_table_expr->children.emplace_back(node_subquery);

    ASTPtr node_table_in_select_query_element = std::make_shared<ASTTablesInSelectQueryElement>();
    node_table_in_select_query_element->as<ASTTablesInSelectQueryElement>()->table_expression = node_table_expr;

    node_table_in_select_query_element->children.emplace_back(node_table_expr);
    ASTPtr res = std::make_shared<ASTTablesInSelectQuery>();

    res->children.emplace_back(node_table_in_select_query_element);

    node = res;
    return true;
}

}
