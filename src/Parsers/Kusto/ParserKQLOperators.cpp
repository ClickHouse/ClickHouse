#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/formatAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

enum class KQLOperatorValue
{
    none,
    between,
    not_between,
    contains,
    not_contains,
    contains_cs,
    not_contains_cs,
    endswith,
    not_endswith,
    endswith_cs,
    not_endswith_cs,
    equal, //=~
    not_equal, //!~
    equal_cs, //=
    not_equal_cs, //!=
    has,
    not_has,
    has_all,
    has_any,
    has_cs,
    not_has_cs,
    hasprefix,
    not_hasprefix,
    hasprefix_cs,
    not_hasprefix_cs,
    hassuffix,
    not_hassuffix,
    hassuffix_cs,
    not_hassuffix_cs,
    in_cs, //in
    not_in_cs, //!in
    in, //in~
    not_in, //!in~
    matches_regex,
    startswith,
    not_startswith,
    startswith_cs,
    not_startswith_cs,
};

const std::unordered_map<String, KQLOperatorValue> KQLOperator =
{
    {"between", KQLOperatorValue::between},
    {"!between", KQLOperatorValue::not_between},
    {"contains", KQLOperatorValue::contains},
    {"!contains", KQLOperatorValue::not_contains},
    {"contains_cs", KQLOperatorValue::contains_cs},
    {"!contains_cs", KQLOperatorValue::not_contains_cs},
    {"endswith", KQLOperatorValue::endswith},
    {"!endswith", KQLOperatorValue::not_endswith},
    {"endswith_cs", KQLOperatorValue::endswith_cs},
    {"!endswith_cs", KQLOperatorValue::not_endswith_cs},
    {"=~", KQLOperatorValue::equal},
    {"!~", KQLOperatorValue::not_equal},
    {"==", KQLOperatorValue::equal_cs},
    {"!=", KQLOperatorValue::not_equal_cs},
    {"has", KQLOperatorValue::has},
    {"!has", KQLOperatorValue::not_has},
    {"has_all", KQLOperatorValue::has_all},
    {"has_any", KQLOperatorValue::has_any},
    {"has_cs", KQLOperatorValue::has_cs},
    {"!has_cs", KQLOperatorValue::not_has_cs},
    {"hasprefix", KQLOperatorValue::hasprefix},
    {"!hasprefix", KQLOperatorValue::not_hasprefix},
    {"hasprefix_cs", KQLOperatorValue::hasprefix_cs},
    {"!hasprefix_cs", KQLOperatorValue::not_hasprefix_cs},
    {"hassuffix", KQLOperatorValue::hassuffix},
    {"!hassuffix", KQLOperatorValue::not_hassuffix},
    {"hassuffix_cs", KQLOperatorValue::hassuffix_cs},
    {"!hassuffix_cs", KQLOperatorValue::not_hassuffix_cs},
    {"in", KQLOperatorValue::in_cs},
    {"!in", KQLOperatorValue::not_in_cs},
    {"in~", KQLOperatorValue::in},
    {"!in~", KQLOperatorValue::not_in},
    {"matches regex", KQLOperatorValue::matches_regex},
    {"startswith", KQLOperatorValue::startswith},
    {"!startswith", KQLOperatorValue::not_startswith},
    {"startswith_cs", KQLOperatorValue::startswith_cs},
    {"!startswith_cs", KQLOperatorValue::not_startswith_cs},
};

void rebuildSubqueryForInOperator(ASTPtr & node, bool useLowerCase)
{
    //A sub-query for in operator in kql can have multiple columns, but only takes the first column.
    //A sub-query for in operator in ClickHouse can not have multiple columns
    //So only take the first column if there are multiple columns.
    //select * not working for subquery. (a tabular statement without project)

    const auto selectColumns = node->children[0]->children[0]->as<ASTSelectQuery>()->select();
    while (selectColumns->children.size() > 1)
        selectColumns->children.pop_back();

    if (useLowerCase)
    {
        auto args = std::make_shared<ASTExpressionList>();
        args->children.push_back(selectColumns->children[0]);
        auto func_lower = std::make_shared<ASTFunction>();
        func_lower->name = "lower";
        func_lower->children.push_back(selectColumns->children[0]);
        func_lower->arguments = args;
        if (selectColumns->children[0]->as<ASTIdentifier>())
            func_lower->alias = std::move(selectColumns->children[0]->as<ASTIdentifier>()->alias);
        else if (selectColumns->children[0]->as<ASTFunction>())
            func_lower->alias = std::move(selectColumns->children[0]->as<ASTFunction>()->alias);

        auto funcs = std::make_shared<ASTExpressionList>();
        funcs->children.push_back(func_lower);
        selectColumns->children[0] = std::move(funcs);
    }
}

}

String KQLOperators::genHasAnyAllOpExpr(std::vector<String> & tokens, IParser::Pos & token_pos, String kql_op, String ch_op)
{
    String new_expr;
    Expected expected;
    ParserToken s_lparen(TokenType::OpeningRoundBracket);

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    auto haystack = tokens.back();

    String logic_op = (kql_op == "has_all") ? " and " : " or ";

    while (isValidKQLPos(token_pos) && token_pos->type != TokenType::PipeMark && token_pos->type != TokenType::Semicolon)
    {
        auto tmp_arg = IParserKQLFunction::getExpression(token_pos);
        if (token_pos->type == TokenType::Comma)
            new_expr = new_expr + logic_op;
        else
            new_expr = new_expr + ch_op + "(" + haystack + "," + tmp_arg + ")";

        ++token_pos;
        if (token_pos->type == TokenType::ClosingRoundBracket)
            break;
    }

    tokens.pop_back();
    return new_expr;
}

String genEqOpExprCis(std::vector<String> & tokens, IParser::Pos & token_pos, const String & ch_op)
{
    String tmp_arg(token_pos->begin, token_pos->end);

    if (tokens.empty() || tmp_arg != "~")
        return tmp_arg;

    String new_expr;
    new_expr += "lower(" + tokens.back() + ")" + " ";
    new_expr += ch_op + " ";
    ++token_pos;

    if (token_pos->type == TokenType::StringLiteral || token_pos->type == TokenType::QuotedIdentifier)
        new_expr += "lower('" + IParserKQLFunction::escapeSingleQuotes(String(token_pos->begin + 1, token_pos->end - 1)) + "')";
    else
        new_expr += "lower(" + IParserKQLFunction::getExpression(token_pos) + ")";

    tokens.pop_back();
    return new_expr;
}

String genInOpExprCis(std::vector<String> & tokens, IParser::Pos & token_pos, const String & kql_op, const String & ch_op)
{
    ParserKQLTableFunction kqlfun_p;
    ParserToken s_lparen(TokenType::OpeningRoundBracket);

    ASTPtr select;
    Expected expected;
    String new_expr;

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    if (tokens.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    new_expr = "lower(" + tokens.back() + ") ";
    tokens.pop_back();
    auto pos = token_pos;
    if (kqlfun_p.parse(pos, select, expected))
    {
        rebuildSubqueryForInOperator(select, true);
        new_expr += ch_op + " (" + serializeAST(*select) + ")";
        token_pos = pos;
        return new_expr;
    }

    --token_pos;
    --token_pos;

    new_expr += ch_op;
    while (isValidKQLPos(token_pos) && token_pos->type != TokenType::PipeMark && token_pos->type != TokenType::Semicolon)
    {
        auto tmp_arg = String(token_pos->begin, token_pos->end);
        if (token_pos->type != TokenType::Comma && token_pos->type != TokenType::ClosingRoundBracket
            && token_pos->type != TokenType::OpeningRoundBracket && token_pos->type != TokenType::OpeningSquareBracket
            && token_pos->type != TokenType::ClosingSquareBracket && tmp_arg != "~" && tmp_arg != "dynamic")
        {
            if (token_pos->type == TokenType::StringLiteral || token_pos->type == TokenType::QuotedIdentifier)
                new_expr += "lower('" + IParserKQLFunction::escapeSingleQuotes(String(token_pos->begin + 1, token_pos->end - 1)) + "')";
            else
                new_expr += "lower(" + tmp_arg + ")";
        }
        else if (tmp_arg != "~" && tmp_arg != "dynamic" && tmp_arg != "[" && tmp_arg != "]")
            new_expr += tmp_arg;

        if (token_pos->type == TokenType::ClosingRoundBracket)
            break;
        ++token_pos;
    }
    return new_expr;
}

std::string genInOpExpr(IParser::Pos & token_pos, const std::string & kql_op, const std::string & ch_op)
{
    ParserKQLTableFunction kqlfun_p;
    ParserToken s_lparen(TokenType::OpeningRoundBracket);

    ASTPtr select;
    Expected expected;

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    auto pos = token_pos;
    if (kqlfun_p.parse(pos, select, expected))
    {
        rebuildSubqueryForInOperator(select, false);
        auto new_expr = ch_op + " (" + serializeAST(*select) + ")";
        token_pos = pos;
        return new_expr;
    }

    --token_pos;
    --token_pos;
    return ch_op;
}

String KQLOperators::genHaystackOpExpr(
    std::vector<String> & tokens, IParser::Pos & token_pos, String kql_op, String ch_op, WildcardsPos wildcards_pos, WildcardsPos space_pos)
{
    String new_expr, left_wildcards, right_wildcards, left_space, right_space;

    switch (wildcards_pos)
    {
        case WildcardsPos::none:
            break;

        case WildcardsPos::left:
            left_wildcards = "%";
            break;

        case WildcardsPos::right:
            right_wildcards = "%";
            break;

        case WildcardsPos::both:
            left_wildcards = "%";
            right_wildcards = "%";
            break;
    }

    switch (space_pos)
    {
        case WildcardsPos::none:
            break;

        case WildcardsPos::left:
            left_space = " ";
            break;

        case WildcardsPos::right:
            right_space = " ";
            break;

        case WildcardsPos::both:
            left_space = " ";
            right_space = " ";
            break;
    }

    ++token_pos;

    if (!tokens.empty() && ((token_pos)->type == TokenType::StringLiteral || token_pos->type == TokenType::QuotedIdentifier))
        new_expr = ch_op + "(" + tokens.back() + ", '" + left_wildcards + left_space + String(token_pos->begin + 1, token_pos->end - 1)
            + right_space + right_wildcards + "')";
    else if (!tokens.empty() && ((token_pos)->type == TokenType::BareWord))
    {
        auto tmp_arg = IParserKQLFunction::getExpression(token_pos);
        new_expr = ch_op + "(" + tokens.back() + ", concat('" + left_wildcards + left_space + "', " + tmp_arg + ", '" + right_space
            + right_wildcards + "'))";
    }
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);
    tokens.pop_back();
    return new_expr;
}

bool KQLOperators::convert(std::vector<String> & tokens, IParser::Pos & pos)
{
    auto begin = pos;

    if (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        KQLOperatorValue op_value = KQLOperatorValue::none;

        auto token = String(pos->begin, pos->end);

        String op = token;
        if (token == "!")
        {
            ++pos;
            if (!isValidKQLPos(pos) || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid negative operator");
            op = "!" + String(pos->begin, pos->end);
        }
        else if (token == "matches")
        {
            ++pos;
            if (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
            {
                if (String(pos->begin, pos->end) == "regex")
                    op += " regex";
                else
                    --pos;
            }
        }
        else
        {
            op = token;
        }

        ++pos;
        if (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            if (String(pos->begin, pos->end) == "~")
                op += "~";
            else
                --pos;
        }
        else
            --pos;

        if (KQLOperator.find(op) == KQLOperator.end())
        {
            pos = begin;
            return false;
        }

        op_value = KQLOperator.at(op);

        String new_expr;

        if (op_value == KQLOperatorValue::none)
            tokens.push_back(op);
        else
        {
            if (tokens.empty())
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", op);

            auto last_op = tokens.back();
            auto last_pos = pos;

            switch (op_value)
            {
                case KQLOperatorValue::contains:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "ilike", WildcardsPos::both);
                    break;

                case KQLOperatorValue::not_contains:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not ilike", WildcardsPos::both);
                    break;

                case KQLOperatorValue::contains_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "like", WildcardsPos::both);
                    break;

                case KQLOperatorValue::not_contains_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not like", WildcardsPos::both);
                    break;

                case KQLOperatorValue::endswith:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "ilike", WildcardsPos::left);
                    break;

                case KQLOperatorValue::not_endswith:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not ilike", WildcardsPos::left);
                    break;

                case KQLOperatorValue::endswith_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "endsWith", WildcardsPos::none);
                    break;

                case KQLOperatorValue::not_endswith_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not endsWith", WildcardsPos::none);
                    break;

                case KQLOperatorValue::equal:
                    new_expr = genEqOpExprCis(tokens, pos, "==");
                    break;

                case KQLOperatorValue::not_equal:
                    new_expr = genEqOpExprCis(tokens, pos, "!=");
                    break;

                case KQLOperatorValue::equal_cs:
                    new_expr = "==";
                    break;

                case KQLOperatorValue::not_equal_cs:
                    new_expr = "!=";
                    break;
                case KQLOperatorValue::has:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "hasTokenCaseInsensitive", WildcardsPos::none);
                    break;

                case KQLOperatorValue::not_has:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not hasTokenCaseInsensitive", WildcardsPos::none);
                    break;

                case KQLOperatorValue::has_all:
                    new_expr = genHasAnyAllOpExpr(tokens, pos, "has_all", "hasTokenCaseInsensitive");
                    break;

                case KQLOperatorValue::has_any:
                    new_expr = genHasAnyAllOpExpr(tokens, pos, "has_any", "hasTokenCaseInsensitive");
                    break;

                case KQLOperatorValue::has_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "hasToken", WildcardsPos::none);
                    break;

                case KQLOperatorValue::not_has_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not hasToken", WildcardsPos::none);
                    break;

                case KQLOperatorValue::hasprefix:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "ilike", WildcardsPos::right);
                    new_expr += " or ";
                    tokens.push_back(last_op);
                    new_expr += genHaystackOpExpr(tokens, last_pos, op, "ilike", WildcardsPos::both, WildcardsPos::left);
                    break;

                case KQLOperatorValue::not_hasprefix:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not ilike", WildcardsPos::right);
                    new_expr += " and ";
                    tokens.push_back(last_op);
                    new_expr += genHaystackOpExpr(tokens, last_pos, op, "not ilike", WildcardsPos::both, WildcardsPos::left);
                    break;

                case KQLOperatorValue::hasprefix_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "startsWith", WildcardsPos::none);
                    new_expr += " or ";
                    tokens.push_back(last_op);
                    new_expr += genHaystackOpExpr(tokens, last_pos, op, "like", WildcardsPos::both, WildcardsPos::left);
                    break;

                case KQLOperatorValue::not_hasprefix_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not startsWith", WildcardsPos::none);
                    new_expr += " and  ";
                    tokens.push_back(last_op);
                    new_expr += genHaystackOpExpr(tokens, last_pos, op, "not like", WildcardsPos::both, WildcardsPos::left);
                    break;

                case KQLOperatorValue::hassuffix:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "ilike", WildcardsPos::left);
                    new_expr += " or ";
                    tokens.push_back(last_op);
                    new_expr += genHaystackOpExpr(tokens, last_pos, op, "ilike", WildcardsPos::both, WildcardsPos::right);
                    break;

                case KQLOperatorValue::not_hassuffix:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not ilike", WildcardsPos::left);
                    new_expr += " and ";
                    tokens.push_back(last_op);
                    new_expr += genHaystackOpExpr(tokens, last_pos, op, "not ilike", WildcardsPos::both, WildcardsPos::right);
                    break;

                case KQLOperatorValue::hassuffix_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "endsWith", WildcardsPos::none);
                    new_expr += " or ";
                    tokens.push_back(last_op);
                    new_expr += genHaystackOpExpr(tokens, last_pos, op, "like", WildcardsPos::both, WildcardsPos::right);
                    break;

                case KQLOperatorValue::not_hassuffix_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not endsWith", WildcardsPos::none);
                    new_expr += " and  ";
                    tokens.push_back(last_op);
                    new_expr += genHaystackOpExpr(tokens, last_pos, op, "not like", WildcardsPos::both, WildcardsPos::right);
                    break;

                case KQLOperatorValue::in_cs:
                    new_expr = genInOpExpr(pos, op, "in");
                    break;

                case KQLOperatorValue::not_in_cs:
                    new_expr = genInOpExpr(pos, op, "not in");
                    break;

                case KQLOperatorValue::in:
                    new_expr = genInOpExprCis(tokens, pos, op, "in");
                    break;

                case KQLOperatorValue::not_in:
                    new_expr = genInOpExprCis(tokens, pos, op, "not in");
                    break;

                case KQLOperatorValue::matches_regex:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "match", WildcardsPos::none);
                    break;

                case KQLOperatorValue::startswith:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "ilike", WildcardsPos::right);
                    break;

                case KQLOperatorValue::not_startswith:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not ilike", WildcardsPos::right);
                    break;

                case KQLOperatorValue::startswith_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "startsWith", WildcardsPos::none);
                    break;

                case KQLOperatorValue::not_startswith_cs:
                    new_expr = genHaystackOpExpr(tokens, pos, op, "not startsWith", WildcardsPos::none);
                    break;

                default:
                    break;
            }

            tokens.push_back(new_expr);
        }
        return true;
    }
    pos = begin;
    return false;
}

}
