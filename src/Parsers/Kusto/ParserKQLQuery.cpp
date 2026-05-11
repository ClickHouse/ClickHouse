#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
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
#include <Parsers/Kusto/ParserKQLCount.h>
#include <Parsers/Kusto/ParserKQLJoin.h>
#include <Parsers/Kusto/ParserKQLPrint.h>
#include <Parsers/Kusto/ParserKQLTop.h>
#include <Parsers/Kusto/ParserKQLUnion.h>
#include <Parsers/Kusto/ParserKQLProject.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLSort.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/ParserKQLSummarize.h>
#include <Parsers/Kusto/ParserKQLTable.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

#include <Poco/String.h>

#include <fmt/format.h>

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

        /// Wrap expressions containing comparison/logical operators in toBool()
        /// so they output true/false instead of 0/1 in KQL mode
        {
            bool has_comparison = false;
            for (const auto & token : tokens)
            {
                if (token == ">" || token == "<" || token == ">=" || token == "<="
                    || token == "==" || token == "!="
                    || token == "and" || token == "or")
                {
                    has_comparison = true;
                    break;
                }
            }
            if (has_comparison && !column_str.empty())
                column_str = fmt::format("toBool({})", column_str);
        }

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
                    new_column_str = fmt::format("{0}[1] AS {1}", column_str, String(start_pos->begin, start_pos->end));
                else
                    new_column_str = fmt::format("{0} AS {1}", column_str, String(start_pos->begin, start_pos->end));

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
                        auto new_column_str = fmt::format("{0}[{1}] AS {2}", column_str, index, alias_inside);
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
    if (op_name == "count")
        return std::make_unique<ParserKQLCount>();
    if (op_name == "top")
        return std::make_unique<ParserKQLTop>();
    if (op_name == "join")
        return std::make_unique<ParserKQLJoin>();
    if (op_name == "union")
        return std::make_unique<ParserKQLUnion>();
    return nullptr;
}

/// Preprocess a KQL query to handle union and join operators by rewriting
/// the query into a form that the standard pipe parser can handle.
/// Returns true if rewriting was done and the query was reparsed.
static bool preprocessUnionJoin(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Scan for | union or | join at bracket depth 0
    auto scan_pos = pos;
    int bracket_depth = 0;
    bool found = false;
    auto pipe_pos = scan_pos;
    String op_type;

    while (isValidKQLPos(scan_pos) && scan_pos->type != TokenType::Semicolon)
    {
        if (scan_pos->type == TokenType::OpeningRoundBracket)
            ++bracket_depth;
        if (scan_pos->type == TokenType::ClosingRoundBracket)
            --bracket_depth;

        if (scan_pos->type == TokenType::PipeMark && bracket_depth == 0)
        {
            pipe_pos = scan_pos;
            auto next = scan_pos;
            ++next;
            if (isValidKQLPos(next))
            {
                /// KQL operators are case-insensitive, so match `UNION`/`Join` etc.
                const String token = Poco::toLower(String(next->begin, next->end));
                if (token == "union" || token == "join")
                {
                    found = true;
                    op_type = token;
                    break;
                }
            }
        }
        ++scan_pos;
    }

    if (!found)
        return false;

    /// Extract left-side query (everything before | union/join)
    /// Guard: if the pipe is at the very start, there's no left query
    if (pipe_pos == pos)
        return false;
    auto left_end = pipe_pos;
    --left_end;
    String left_query(pos->begin, left_end->end);

    /// Move past | and the operator keyword
    scan_pos = pipe_pos;
    ++scan_pos; /// skip |
    ++scan_pos; /// skip union/join keyword

    /// For join: parse kind=X. KQL keywords are case-insensitive.
    /// Validate every token before reading: malformed inputs like `| join kind`
    /// or `| join kind =` must fail cleanly instead of dereferencing past end-of-stream.
    String join_kind;
    if (op_type == "join" && isValidKQLPos(scan_pos)
        && Poco::toLower(String(scan_pos->begin, scan_pos->end)) == "kind")
    {
        ++scan_pos; /// skip kind
        if (!isValidKQLPos(scan_pos) || scan_pos->type != TokenType::Equals)
            return false;
        ++scan_pos; /// skip =
        if (!isValidKQLPos(scan_pos))
            return false;
        join_kind = Poco::toLower(String(scan_pos->begin, scan_pos->end));
        ++scan_pos; /// skip kind value
    }

    /// Parse parenthesized right-side subquery
    if (!isValidKQLPos(scan_pos) || scan_pos->type != TokenType::OpeningRoundBracket)
        return false;
    int depth = 1;
    ++scan_pos;
    auto right_start = scan_pos;
    while (isValidKQLPos(scan_pos) && depth > 0)
    {
        if (scan_pos->type == TokenType::OpeningRoundBracket) ++depth;
        if (scan_pos->type == TokenType::ClosingRoundBracket) --depth;
        if (depth > 0) ++scan_pos;
    }
    /// Guard: if closing bracket was never found, bail out
    if (!isValidKQLPos(scan_pos) || scan_pos->type != TokenType::ClosingRoundBracket)
        return false;
    /// Guard: empty parenthesized subquery (`union ()` / `join () on a`).
    /// Check before decrementing so we never construct a string from an inverted iterator range.
    if (scan_pos == right_start)
        return false;
    auto right_end_pos = scan_pos;
    --right_end_pos;
    String right_query(right_start->begin, right_end_pos->end);
    ++scan_pos; /// skip closing bracket

    /// For join: parse "on" clause. KQL keywords are case-insensitive.
    String join_on;
    if (op_type == "join" && isValidKQLPos(scan_pos)
        && Poco::toLower(String(scan_pos->begin, scan_pos->end)) == "on")
    {
        ++scan_pos;
        while (isValidKQLPos(scan_pos) && scan_pos->type != TokenType::PipeMark && scan_pos->type != TokenType::Semicolon)
        {
            if (!join_on.empty()) join_on += " ";
            join_on += String(scan_pos->begin, scan_pos->end);
            ++scan_pos;
        }
    }

    /// Parse left and right as KQL to get SQL
    ASTPtr left_ast;
    ASTPtr right_ast;

    Tokens left_tokens(left_query.data(), left_query.data() + left_query.size(), 0, true);
    IParser::Pos left_pos(left_tokens, pos.max_depth, pos.max_backtracks);
    if (!ParserKQLWithUnionQuery().parse(left_pos, left_ast, expected))
        return false;

    Tokens right_tokens(right_query.data(), right_query.data() + right_query.size(), 0, true);
    IParser::Pos right_pos(right_tokens, pos.max_depth, pos.max_backtracks);
    if (!ParserKQLWithUnionQuery().parse(right_pos, right_ast, expected))
        return false;

    String left_sql = left_ast->formatWithSecretsOneLine();
    String right_sql = right_ast->formatWithSecretsOneLine();

    /// Build combined SQL
    String combined;
    if (op_type == "union")
    {
        combined = fmt::format("({} UNION ALL {})", left_sql, right_sql);
    }
    else
    {
        /// Map KQL join kind to ClickHouse join type.
        /// Reject unsupported `kind=...` values rather than silently treating them as `INNER`,
        /// which would mask typos and unsupported kinds with semantically different results.
        /// Mirrors the explicit-reject behavior in `ParserKQLJoin::parseImpl`.
        String ch_join;
        if (join_kind.empty() || join_kind == "inner" || join_kind == "innerunique") ch_join = "INNER";
        else if (join_kind == "fullouter") ch_join = "FULL";
        else if (join_kind == "leftouter") ch_join = "LEFT";
        else if (join_kind == "rightouter") ch_join = "RIGHT";
        else if (join_kind == "leftanti" || join_kind == "leftantisemi") ch_join = "LEFT ANTI";
        else if (join_kind == "leftsemi") ch_join = "LEFT SEMI";
        else if (join_kind == "rightanti" || join_kind == "rightantisemi") ch_join = "RIGHT ANTI";
        else if (join_kind == "rightsemi") ch_join = "RIGHT SEMI";
        else return false;

        String on_clause;
        if (join_on.find("==") == String::npos && join_on.find('=') == String::npos)
        {
            /// Handle comma-separated join keys: on a, b -> a = a1 AND b = b1
            std::vector<String> keys;
            {
                String current;
                for (char c : join_on)
                {
                    if (c == ',')
                    {
                        auto start = current.find_first_not_of(' ');
                        auto end = current.find_last_not_of(' ');
                        if (start != String::npos)
                            keys.push_back(current.substr(start, end - start + 1));
                        current.clear();
                    }
                    else
                        current += c;
                }
                auto start = current.find_first_not_of(' ');
                auto end = current.find_last_not_of(' ');
                if (start != String::npos)
                    keys.push_back(current.substr(start, end - start + 1));
            }
            for (size_t i = 0; i < keys.size(); ++i)
            {
                if (i > 0)
                    on_clause += " AND ";
                on_clause += fmt::format("{0} = {0}1", keys[i]);
            }
        }
        else
            on_clause = join_on;

        combined = fmt::format("(SELECT * FROM ({}) ALL {} JOIN ({}) ON {})", left_sql, ch_join, right_sql, on_clause);
    }

    /// Build new KQL query: combined_source | remaining_pipes
    String remaining;
    if (isValidKQLPos(scan_pos) && scan_pos->type == TokenType::PipeMark)
    {
        auto rest_start = scan_pos;
        while (isValidKQLPos(scan_pos) && scan_pos->type != TokenType::Semicolon)
            ++scan_pos;
        --scan_pos;
        remaining = String(rest_start->begin, scan_pos->end);
        ++scan_pos;
    }

    if (remaining.empty())
    {
        /// No remaining pipes — parse as SQL
        String new_query = fmt::format("SELECT * FROM {}", combined);
        Tokens new_tokens(new_query.data(), new_query.data() + new_query.size(), 0, true);
        IParser::Pos new_pos(new_tokens, pos.max_depth, pos.max_backtracks);
        ParserSelectWithUnionQuery sql_parser;
        if (!sql_parser.parse(new_pos, node, expected))
            return false;
    }
    else
    {
        /// Remaining has KQL pipe operators — build KQL with subquery source
        /// Parse: (combined_subquery) | remaining_ops
        /// by recursively calling KQL parser
        String kql_query = fmt::format("{} {}", combined, remaining);
        Tokens kql_tokens(kql_query.data(), kql_query.data() + kql_query.size(), 0, true);
        IParser::Pos kql_pos(kql_tokens, pos.max_depth, pos.max_backtracks);
        if (!ParserKQLQuery().parse(kql_pos, node, expected))
            return false;
    }

    pos = scan_pos;
    return true;
}

bool ParserKQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// First, check if the query contains union or join operators at top level.
    /// If so, preprocess them by rewriting into SQL subqueries.
    if (preprocessUnionJoin(pos, node, expected))
        return true;

    struct KQLOperatorDataFlowState
    {
        String operator_name;
        bool need_input;
        bool gen_output;
        int8_t backspace_steps; // how many steps to last token of previous pipe
    };

    auto select_query = make_intrusive<ASTSelectQuery>();
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
           {"mv-expand", {"mv-expand", true, true, 5}},
           {"count", {"count", true, true, 2}},
           {"top", {"top", false, true, 3}},
           };

    std::vector<std::pair<String, Pos>> operation_pos;

    /// KQL keywords are case-insensitive, so dispatch on the lowercased token
    /// instead of the verbatim text — otherwise `PRINT 1` or `RANGE x from ...`
    /// silently bypass the dedicated paths and fall through to table parsing.
    String table_name = Poco::toLower(String(pos->begin, pos->end));

    if (table_name == "print")
        operation_pos.emplace_back(table_name, pos);
    else if (table_name == "range")
    {
        /// range col from start to end step step_val
        /// Translate to: (SELECT arrayJoin(...) AS col) [| pipe_ops]
        ++pos;
        if (!isValidKQLPos(pos))
            return false;
        String col_name(pos->begin, pos->end);
        ++pos;
        /// KQL keywords are case-insensitive, so `range x FROM 1 TO 10 STEP 1` must parse.
        if (!isValidKQLPos(pos) || Poco::toLower(String(pos->begin, pos->end)) != "from")
            return false;
        ++pos;
        String start_raw(pos->begin, pos->end);
        String start_val = IParserKQLFunction::getExpression(pos);
        /// getExpression may or may not advance pos past the token
        /// Check if we're already at 'to', if not advance
        if (isValidKQLPos(pos) && Poco::toLower(String(pos->begin, pos->end)) != "to")
            ++pos;
        if (!isValidKQLPos(pos) || Poco::toLower(String(pos->begin, pos->end)) != "to")
            return false;
        ++pos;
        String end_val = IParserKQLFunction::getExpression(pos);
        if (isValidKQLPos(pos) && Poco::toLower(String(pos->begin, pos->end)) != "step")
            ++pos;
        if (!isValidKQLPos(pos) || Poco::toLower(String(pos->begin, pos->end)) != "step")
            return false;
        ++pos;
        /// Reject `range x from a to b step` with no literal after `step`,
        /// which would otherwise dereference an invalid token below.
        if (!isValidKQLPos(pos))
            return false;
        /// Handle negative step: -N (may be single ErrorWrongNumber token or Minus + Number)
        String step_sign;
        String step_raw;
        String step_val;
        if (pos->type == TokenType::Minus || String(pos->begin, pos->end) == "-")
        {
            step_sign = "-";
            ++pos;
            /// Reject `range x from a to b step -` with no literal after the unary minus.
            if (!isValidKQLPos(pos))
                return false;
            step_raw = String(pos->begin, pos->end);
            step_val = step_sign + IParserKQLFunction::getExpression(pos);
        }
        else
        {
            step_raw = String(pos->begin, pos->end);
            step_val = IParserKQLFunction::getExpression(pos);
        }
        /// Advance past the step value if needed
        if (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
            ++pos;

        /// Detect if this is a timespan range (original tokens were timespan literals)
        /// A negative sign difference (step_raw="1" vs step_val="-1") is NOT a timespan
        bool is_timespan_range = (start_raw != start_val)
            || (step_raw != step_val && step_sign.empty());

        /// Build range as a SQL subquery; guard against step == 0 to avoid division by zero
        String range_expr = fmt::format(
            "arrayJoin(if(({3}) = 0, [NULL], if(({3}) > 0, "
            "arrayMap(x -> ({1}) + x * ({3}), range(0, toUInt64((({2}) - ({1})) / ({3})) + 1)), "
            "arrayMap(x -> ({1}) + x * ({3}), range(0, toUInt64((({1}) - ({2})) / abs({3})) + 1))"
            ")))",
            col_name, start_val, end_val, step_val);

        /// For timespan ranges, wrap output in timespan formatter
        String range_sql;
        if (is_timespan_range)
        {
            range_sql =
                "SELECT concat("
                "if((_v) < 0, '-', ''), "
                "if(abs(toInt64(_v)) >= 86400, concat(toString(intDiv(abs(toInt64(_v)), 86400)), '.'), ''), "
                "leftPad(toString(intDiv(abs(toInt64(_v)) % 86400, 3600)), 2, '0'), ':', "
                "leftPad(toString(intDiv(abs(toInt64(_v)) % 3600, 60)), 2, '0'), ':', "
                "leftPad(toString(abs(toInt64(_v)) % 60), 2, '0')) AS " + col_name + " "
                "FROM (SELECT " + range_expr + " AS _v)";
        }
        else
            range_sql = fmt::format("SELECT {} AS {}", range_expr, col_name);

        /// Collect remaining text (pipe operations + semicolon)
        String remaining;
        if (isValidKQLPos(pos) && pos->type == TokenType::PipeMark)
        {
            auto rest_start = pos;
            while (isValidKQLPos(pos) && pos->type != TokenType::Semicolon)
                ++pos;
            remaining = String(rest_start->begin, pos->begin);
        }

        /// Parse the range SQL to get the base AST, then wrap as subquery table source
        ASTPtr range_ast;
        {
            Tokens range_tokens(range_sql.data(), range_sql.data() + range_sql.size(), 0, true);
            IParser::Pos range_pos(range_tokens, pos.max_depth, pos.max_backtracks);
            ParserSelectWithUnionQuery sql_parser;
            if (!sql_parser.parse(range_pos, range_ast, expected))
                return false;
        }

        if (remaining.empty())
        {
            /// No pipe operations — return range directly
            node = range_ast;
            return true;
        }

        /// Has pipe operations — set range as subquery source and parse pipes
        auto range_select_query = make_intrusive<ASTSelectQuery>();
        node = range_select_query;

        ASTPtr range_subquery_node = make_intrusive<ASTSubquery>(std::move(range_ast));
        ASTPtr rte = make_intrusive<ASTTableExpression>();
        rte->as<ASTTableExpression>()->subquery = range_subquery_node;
        rte->children.emplace_back(range_subquery_node);
        auto rte_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        rte_elem->as<ASTTablesInSelectQueryElement>()->table_expression = rte;
        rte_elem->children.emplace_back(rte);
        auto rtables = make_intrusive<ASTTablesInSelectQuery>();
        rtables->children.emplace_back(rte_elem);
        range_select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(rtables));

        /// Parse remaining pipe operations using getExprFromPipe + operator parsers
        Tokens pipe_tokens(remaining.data(), remaining.data() + remaining.size(), 0, true);
        IParser::Pos pipe_pos(pipe_tokens, pos.max_depth, pos.max_backtracks);

        if (isValidKQLPos(pipe_pos) && pipe_pos->type == TokenType::PipeMark)
            ++pipe_pos;

        /// Use the standard operator parsing loop
        while (isValidKQLPos(pipe_pos) && pipe_pos->type != TokenType::Semicolon)
        {
            /// KQL operators are case-insensitive, so normalize before lookup —
            /// `getOperator` expects lowercase names and `... | COUNT` would
            /// otherwise return `nullptr` and reject a valid pipeline.
            String op_name = Poco::toLower(String(pipe_pos->begin, pipe_pos->end));

            /// Handle "sort by" / "order by"
            if (op_name == "sort" || op_name == "order")
            {
                ++pipe_pos;
                if (isValidKQLPos(pipe_pos))
                {
                    op_name += " by";
                    ++pipe_pos;
                }
            }
            else
                ++pipe_pos;

            auto kql_op = getOperator(op_name);
            if (!kql_op)
                return false;
            if (!kql_op->parse(pipe_pos, node, expected))
                return false;

            while (isValidKQLPos(pipe_pos) && pipe_pos->type != TokenType::PipeMark && pipe_pos->type != TokenType::Semicolon)
                ++pipe_pos;
            if (isValidKQLPos(pipe_pos) && pipe_pos->type == TokenType::PipeMark)
                ++pipe_pos;
        }

        if (!range_select_query->select())
        {
            String star = "*";
            Tokens star_tokens(star.data(), star.data() + star.size(), 0, true);
            IParser::Pos star_pos(star_tokens, pos.max_depth, pos.max_backtracks);
            ASTPtr select_expr;
            if (!ParserNotEmptyExpressionList(true).parse(star_pos, select_expr, expected))
                return false;
            range_select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr));
        }

        range_select_query->normalizeChildrenOrder();
        return true;
    }
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
                    {
                        /// Operator is the last token — check if it's valid as-is
                        --pos;
                        return kql_parser.contains(kql_operator);
                    }

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
                if (!kql_parser.contains(kql_operator))
                    return false;
                return true;
            };

            if (!validate_kql_operator())
                return false;

            /// count operator may have no arguments - handle specially
            if (kql_operator == "count")
            {
                /// Store a valid position for count (the keyword itself, not what follows).
                /// `count` may have no arguments, so we cannot store the post-`count` position:
                /// `npos.isValid()` checks below would reject bare `T | count` at end-of-stream.
                /// `ParserKQLCount` skips a leading `count` token before reading any `as <alias>`.
                auto count_pos = pos;
                ++pos;
                operation_pos.push_back(std::make_pair(kql_operator, count_pos));
                continue;
            }

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
        String project_clause;
        String order_clause;
        String where_clause;
        String limit_clause;
        auto last_pos = operation_pos.back().second;
        auto last_op = operation_pos.back().first;

        auto set_main_query_clause = [&](const String & op, Pos & op_pos)
        {
            auto op_str = ParserKQLBase::getExprFromPipe(op_pos);
            if (op == "project")
                project_clause = op_str;
            else if (op == "where" || op == "filter")
                where_clause = where_clause.empty() ? fmt::format("({})", op_str) : where_clause + fmt::format("AND ({})", op_str);
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

            String sub_query = fmt::format("({})", String(operation_pos.front().second->begin, last_pos->end));
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

        node->as<ASTSelectQuery>()->normalizeChildrenOrder();
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

    node->as<ASTSelectQuery>()->normalizeChildrenOrder();
    return true;
}

bool ParserKQLSubquery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_node;

    if (!ParserKQLTableFunction().parse(pos, select_node, expected))
        return false;

    ASTPtr node_subquery = make_intrusive<ASTSubquery>(std::move(select_node));

    ASTPtr node_table_expr = make_intrusive<ASTTableExpression>();
    node_table_expr->as<ASTTableExpression>()->subquery = node_subquery;

    node_table_expr->children.emplace_back(node_subquery);

    ASTPtr node_table_in_select_query_element = make_intrusive<ASTTablesInSelectQueryElement>();
    node_table_in_select_query_element->as<ASTTablesInSelectQueryElement>()->table_expression = node_table_expr;

    ASTPtr res = make_intrusive<ASTTablesInSelectQuery>();

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

    ASTPtr node_subquery = make_intrusive<ASTSubquery>(std::move(sub_select_node));

    ASTPtr node_table_expr = make_intrusive<ASTTableExpression>();
    node_table_expr->as<ASTTableExpression>()->subquery = node_subquery;

    node_table_expr->children.emplace_back(node_subquery);

    ASTPtr node_table_in_select_query_element = make_intrusive<ASTTablesInSelectQueryElement>();
    node_table_in_select_query_element->as<ASTTablesInSelectQueryElement>()->table_expression = node_table_expr;

    node_table_in_select_query_element->children.emplace_back(node_table_expr);
    ASTPtr res = make_intrusive<ASTTablesInSelectQuery>();

    res->children.emplace_back(node_table_in_select_query_element);

    node = res;
    return true;
}

}
