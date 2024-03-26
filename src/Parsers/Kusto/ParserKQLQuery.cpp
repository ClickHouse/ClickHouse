#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLTable.h>
#include <Parsers/Kusto/ParserKQLProject.h>
#include <Parsers/Kusto/ParserKQLFilter.h>
#include <Parsers/Kusto/ParserKQLSort.h>
#include <Parsers/Kusto/ParserKQLSummarize.h>
#include <Parsers/Kusto/ParserKQLLimit.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/CommonParsers.h>
#include <format>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/Kusto/ParserKQLOperators.h>

namespace DB
{

String ParserKQLBase :: getExprFromToken(const String & text, const uint32_t & max_depth)
{
    Tokens tokens(text.c_str(), text.c_str() + text.size());
    IParser::Pos pos(tokens, max_depth);

    return getExprFromToken(pos);
}

String ParserKQLBase :: getExprFromPipe(Pos & pos)
{
    uint16_t bracket_count = 0;
    auto begin = pos;
    auto end = pos;
    while (!end->isEnd() && end->type != TokenType::Semicolon)
    {
        if (end->type == TokenType::OpeningRoundBracket)
            ++bracket_count;

        if (end->type == TokenType::OpeningRoundBracket)
            --bracket_count;

        if (end->type == TokenType::PipeMark && bracket_count == 0)
            break;

        ++end;
    }
    --end;
    return String(begin->begin, end->end);
}

String ParserKQLBase :: getExprFromToken(Pos & pos)
{
    String res;
    std::vector<String> tokens;
    String alias;

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        String token = String(pos->begin,pos->end);

        if (token == "=")
        {
            ++pos;
            if (String(pos->begin,pos->end) != "~")
            {
                alias = tokens.back();
                tokens.pop_back();
            }
            --pos;
        }
        else if (!KQLOperators().convert(tokens,pos))
        {
            tokens.push_back(token);
        }

        if (pos->type == TokenType::Comma && !alias.empty())
        {
            tokens.pop_back();
            tokens.push_back("AS");
            tokens.push_back(alias);
            tokens.push_back(",");
            alias.clear();
        }
        ++pos;
    }

    if (!alias.empty())
    {
        tokens.push_back("AS");
        tokens.push_back(alias);
    }

    for (auto const &token : tokens)
        res = res.empty()? token : res +" " + token;
    return res;
}

std::unique_ptr<IParserBase> ParserKQLQuery::getOperator(String & op_name)
{
    if (op_name == "filter" || op_name == "where")
        return std::make_unique<ParserKQLFilter>();
    else if (op_name == "limit" || op_name == "take")
        return std::make_unique<ParserKQLLimit>();
    else if (op_name == "project")
        return std::make_unique<ParserKQLProject>();
    else if (op_name == "sort by" || op_name == "order by")
        return std::make_unique<ParserKQLSort>();
    else if (op_name == "summarize")
        return std::make_unique<ParserKQLSummarize>();
    else if (op_name == "table")
        return std::make_unique<ParserKQLTable>();
    else
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

    std::unordered_map<std::string, KQLOperatorDataFlowState> kql_parser =
    {
        { "filter", {"filter", false, false, 3}},
        { "where", {"filter", false, false, 3}},
        { "limit", {"limit", false, true, 3}},
        { "take", {"limit", false, true, 3}},
        { "project", {"project", false, false, 3}},
        { "sort by", {"order by", false, false, 4}},
        { "order by", {"order by", false, false, 4}},
        { "table", {"table", false, false, 3}},
        { "summarize", {"summarize", true, true, 3}}
    };

    std::vector<std::pair<String, Pos>> operation_pos;

    String table_name(pos->begin, pos->end);

    operation_pos.push_back(std::make_pair("table", pos));
    ++pos;
    uint16_t bracket_count = 0;

    while (!pos->isEnd() && pos->type != TokenType::Semicolon)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++bracket_count;
        if (pos->type == TokenType::OpeningRoundBracket)
            --bracket_count;

        if (pos->type == TokenType::PipeMark && bracket_count == 0)
        {
            ++pos;
            String kql_operator(pos->begin, pos->end);
            if (kql_operator == "order" || kql_operator == "sort")
            {
                ++pos;
                ParserKeyword s_by("by");
                if (s_by.ignore(pos,expected))
                {
                    kql_operator = "order by";
                    --pos;
                }
            }
            if (pos->type != TokenType::BareWord || kql_parser.find(kql_operator) == kql_parser.end())
                return false;
            ++pos;
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
        if (!kql_operator_p->parse(npos, node, expected))
            return false;
    }
    else if (operation_pos.size() == 2 && operation_pos.front().first == "table")
    {
        if (!kql_operator_p->parse(npos, node, expected))
            return false;
        npos = operation_pos.front().second;
        if (!ParserKQLTable().parse(npos, node, expected))
            return false;
    }
    else
    {
        String project_clause, order_clause, where_clause, limit_clause;
        auto last_pos = operation_pos.back().second;
        auto last_op = operation_pos.back().first;

        auto set_main_query_clause =[&](String & op, Pos & op_pos)
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

        if (kql_parser[last_op].need_input)
        {
            if (!kql_operator_p->parse(npos, node, expected))
                return false;
        }
        else
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
            for (auto i = 0; i< kql_parser[last_op].backspace_steps; ++i)
                --last_pos;

            String sub_query = std::format("({})", String(operation_pos.front().second->begin, last_pos->end));
            Tokens token_subquery(sub_query.c_str(), sub_query.c_str() + sub_query.size());
            IParser::Pos pos_subquery(token_subquery, pos.max_depth);

            if (!ParserKQLSubquery().parse(pos_subquery, tables, expected))
                return false;
            select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
        }
        else
        {
            if (!ParserKQLTable().parse(last_pos, node, expected))
                return false;
        }

        auto set_query_clasue =[&](String op_str, String op_calsue)
        {
            auto oprator = getOperator(op_str);
            if (oprator)
            {
                Tokens token_clause(op_calsue.c_str(), op_calsue.c_str() + op_calsue.size());
                IParser::Pos pos_clause(token_clause, pos.max_depth);
                if (!oprator->parse(pos_clause, node, expected))
                    return false;
            }
            return true;
        };

        if (!select_query->select())
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

    if (!select_query->select())
    {
        auto expr = String("*");
        Tokens tokens(expr.c_str(), expr.c_str()+expr.size());
        IParser::Pos new_pos(tokens, pos.max_depth);
        if (!std::make_unique<ParserKQLProject>()->parse(new_pos, node, expected))
            return false;
    }

     return true;
}

bool ParserKQLSubquery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_node;

    if (!ParserKQLTaleFunction().parse(pos, select_node, expected))
        return false;

    ASTPtr node_subquery = std::make_shared<ASTSubquery>();
    node_subquery->children.push_back(select_node);

    ASTPtr node_table_expr = std::make_shared<ASTTableExpression>();
    node_table_expr->as<ASTTableExpression>()->subquery = node_subquery;

    node_table_expr->children.emplace_back(node_subquery);

    ASTPtr node_table_in_select_query_emlement = std::make_shared<ASTTablesInSelectQueryElement>();
    node_table_in_select_query_emlement->as<ASTTablesInSelectQueryElement>()->table_expression = node_table_expr;

    ASTPtr res = std::make_shared<ASTTablesInSelectQuery>();

    res->children.emplace_back(node_table_in_select_query_emlement);

    node = res;
    return true;
}

}
