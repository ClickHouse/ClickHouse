#include "ParserKQLOperators.h"
#include <Interpreters/ITokenExtractor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/formatAST.h>
#include "KustoFunctions/IParserKQLFunction.h"
#include "ParserKQLQuery.h"
#include "ParserKQLStatement.h"

#include <format>
#include <unordered_map>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int SYNTAX_ERROR;
}

namespace
{
enum class WildcardsPos : uint8_t
{
    none,
    left,
    right,
    both
};

enum class KQLOperatorValue : uint16_t
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

const std::unordered_map<String, KQLOperatorValue> KQLOperator = {
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

void rebuildSubqueryForInOperator(DB::ASTPtr & node, bool useLowerCase)
{
    //A sub-query for in operator in kql can have multiple columns, but only takes the first column.
    //A sub-query for in operator in ClickHouse can not have multiple columns
    //So only take the first column if there are multiple columns.
    //select * not working for subquery. (a tabular statement without project)

    const auto selectColumns = node->children[0]->children[0]->as<DB::ASTSelectQuery>()->select();
    while (selectColumns->children.size() > 1)
        selectColumns->children.pop_back();

    if (useLowerCase)
    {
        auto args = std::make_shared<DB::ASTExpressionList>();
        args->children.push_back(selectColumns->children[0]);
        auto func_lower = std::make_shared<DB::ASTFunction>();
        func_lower->name = "lower";
        func_lower->children.push_back(selectColumns->children[0]);
        func_lower->arguments = args;
        if (selectColumns->children[0]->as<DB::ASTIdentifier>())
            func_lower->alias = std::move(selectColumns->children[0]->as<DB::ASTIdentifier>()->alias);
        else if (selectColumns->children[0]->as<DB::ASTFunction>())
            func_lower->alias = std::move(selectColumns->children[0]->as<DB::ASTFunction>()->alias);

        auto funcs = std::make_shared<DB::ASTExpressionList>();
        funcs->children.push_back(func_lower);
        selectColumns->children[0] = std::move(funcs);
    }
}

std::string applyFormatString(const std::string_view format_string, const std::string & haystack, const std::string & needle)
{
    return std::vformat(format_string, std::make_format_args(haystack, needle));
}

std::string constructHasOperatorTranslation(const KQLOperatorValue kql_op, const std::string & haystack, const std::string & needle)
{
    if (kql_op != KQLOperatorValue::has && kql_op != KQLOperatorValue::not_has && kql_op != KQLOperatorValue::has_cs
        && kql_op != KQLOperatorValue::not_has_cs && kql_op != KQLOperatorValue::has_all && kql_op != KQLOperatorValue::has_any)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected operator: {}", magic_enum::enum_name(kql_op));

    const auto tokens = std::invoke([&needle] {
        std::vector<std::string_view> result;
        size_t pos = 0;
        size_t start = 0;
        size_t length = 0;
        DB::SplitTokenExtractor token_extractor;
        while (pos < needle.length() && token_extractor.nextInString(needle.c_str(), needle.length(), &pos, &start, &length))
            result.emplace_back(needle.c_str() + start, length);

        return result;
    });

    const auto is_case_sensitive = kql_op == KQLOperatorValue::has_cs || kql_op == KQLOperatorValue::not_has_cs;
    const auto has_token_suffix = is_case_sensitive ? "" : "CaseInsensitive";
    const auto has_all_tokens
        = std::accumulate(tokens.cbegin(), tokens.cend(), std::string(), [&has_token_suffix, &haystack](auto acc, const auto & token) {
              return std::move(acc) + std::format("hasToken{}({}, '{}') and ", has_token_suffix, haystack, token);
          });

    const auto is_negation = kql_op == KQLOperatorValue::not_has || kql_op == KQLOperatorValue::not_has_cs;
    return std::format(
        "{4}ifNull(hasToken{3}OrNull({0}, {1}), {2} position{3}({0}, {1}) > 0)",
        haystack,
        needle,
        has_all_tokens,
        has_token_suffix,
        is_negation ? "not " : "");
}
}

String genHasAnyAllOpExpr(
    std::vector<std::string> & tokens,
    DB::IParser::Pos & token_pos,
    const std::string & kql_op,
    const std::function<std::string(const std::string &, const std::string &)> & translate)
{
    std::string new_expr;
    DB::Expected expected;
    DB::ParserToken s_lparen(DB::TokenType::OpeningRoundBracket);

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw DB::Exception(DB::ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    auto haystack = tokens.back();
    const auto * const logic_op = (kql_op == "has_all") ? " and " : " or ";
    while (!token_pos->isEnd() && token_pos->type != DB::TokenType::PipeMark && token_pos->type != DB::TokenType::Semicolon)
    {
        auto tmp_arg = DB::IParserKQLFunction::getExpression(token_pos);
        if (token_pos->type == DB::TokenType::Comma)
            new_expr += logic_op;
        else
            new_expr += translate(haystack, tmp_arg);

        ++token_pos;
        if (token_pos->type == DB::TokenType::ClosingRoundBracket)
            break;
    }

    tokens.pop_back();
    return new_expr;
}

String genEqOpExprCis(std::vector<String> & tokens, DB::IParser::Pos & token_pos, const DB::String & ch_op)
{
    DB::String tmp_arg(token_pos->begin, token_pos->end);

    if (tokens.empty() || tmp_arg != "~")
        return tmp_arg;

    DB::String new_expr;
    new_expr += "lower(" + tokens.back() + ")" + " ";
    new_expr += ch_op + " ";
    ++token_pos;

    if (token_pos->type == DB::TokenType::StringLiteral || token_pos->type == DB::TokenType::QuotedIdentifier)
        new_expr += "lower('" + DB::IParserKQLFunction::escapeSingleQuotes(String(token_pos->begin + 1, token_pos->end - 1)) + "')";
    else
        new_expr += "lower(" + DB::IParserKQLFunction::getExpression(token_pos) + ")";

    tokens.pop_back();
    return new_expr;
}

String genBetweenOpExpr(std::vector<std::string> & tokens, DB::IParser::Pos & token_pos, const String & ch_op)
{
    DB::String new_expr;
    new_expr += ch_op + "(";
    new_expr += tokens.back() + ",";
    tokens.pop_back();
    ++token_pos;

    DB::BracketCount bracket_count;
    bracket_count.count(token_pos);

    ++token_pos;

    while (!token_pos->isEnd())
    {
        if ((token_pos->type == DB::TokenType::PipeMark || token_pos->type == DB::TokenType::Semicolon))
            break;
        if (token_pos->type == DB::TokenType::Dot)
            break;
        new_expr += DB::IParserKQLFunction::getExpression(token_pos);
        ++token_pos;
    }
    new_expr += ",";

    DB::ParserToken dot_token(DB::TokenType::Dot);

    if (dot_token.ignore(token_pos) && dot_token.ignore(token_pos))
    {
        while (!token_pos->isEnd())
        {
            bracket_count.count(token_pos);
            if ((token_pos->type == DB::TokenType::PipeMark || token_pos->type == DB::TokenType::Semicolon) && bracket_count.isZero())
                break;
            new_expr += DB::IParserKQLFunction::getExpression(token_pos);

            if (token_pos->type == DB::TokenType::ClosingRoundBracket && bracket_count.isZero())
            {
                break;
            }
            ++token_pos;
        }
    }
    else
        throw DB::Exception(DB::ErrorCodes::SYNTAX_ERROR, "Syntax error, number of dots do not match.");

    return new_expr;
}

String genInOpExprCis(std::vector<String> & tokens, DB::IParser::Pos & token_pos, const DB::String & kql_op, const DB::String & ch_op)
{
    DB::ParserKQLTableFunction kqlfun_p;
    DB::ParserToken s_lparen(DB::TokenType::OpeningRoundBracket);

    DB::ASTPtr select;
    DB::Expected expected;
    DB::String new_expr;

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw DB::Exception(DB::ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    if (tokens.empty())
        throw DB::Exception(DB::ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

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
    while (!token_pos->isEnd() && token_pos->type != DB::TokenType::PipeMark && token_pos->type != DB::TokenType::Semicolon)
    {
        auto tmp_arg = DB::String(token_pos->begin, token_pos->end);
        if (token_pos->type != DB::TokenType::Comma && token_pos->type != DB::TokenType::ClosingRoundBracket
            && token_pos->type != DB::TokenType::OpeningRoundBracket && token_pos->type != DB::TokenType::OpeningSquareBracket
            && token_pos->type != DB::TokenType::ClosingSquareBracket && tmp_arg != "~" && tmp_arg != "dynamic")
        {
            if (token_pos->type == DB::TokenType::StringLiteral || token_pos->type == DB::TokenType::QuotedIdentifier)
                new_expr += "lower('" + DB::IParserKQLFunction::escapeSingleQuotes(String(token_pos->begin + 1, token_pos->end - 1)) + "')";
            else
                new_expr += "lower(" + tmp_arg + ")";
        }
        else if (tmp_arg != "~" && tmp_arg != "dynamic" && tmp_arg != "[" && tmp_arg != "]")
            new_expr += tmp_arg;

        if (token_pos->type == DB::TokenType::ClosingRoundBracket)
            break;
        ++token_pos;
    }
    return new_expr;
}

std::string genInOpExpr(DB::IParser::Pos & token_pos, const std::string & kql_op, const std::string & ch_op)
{
    DB::ParserKQLTableFunction kqlfun_p;
    DB::ParserToken s_lparen(DB::TokenType::OpeningRoundBracket);

    DB::ASTPtr select;
    DB::Expected expected;

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw DB::Exception(DB::ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

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

std::string genHaystackOpExpr(
    std::vector<std::string> & tokens,
    DB::IParser::Pos & token_pos,
    const std::string & kql_op,
    const std::function<std::string(const std::string &, const std::string &)> & translate,
    WildcardsPos wildcards_pos,
    WildcardsPos space_pos = WildcardsPos::none)
{
    std::string new_expr, left_wildcards, right_wildcards, left_space, right_space;

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    auto haystack = tokens.back();

    String logic_op = (kql_op == "has_all") ? " and " : " or ";

    while (!token_pos->isEnd() && token_pos->type != TokenType::PipeMark && token_pos->type != TokenType::Semicolon)
    {
        auto tmp_arg = String(token_pos->begin, token_pos->end);
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

String KQLOperators::genInOpExpr(IParser::Pos &token_pos, String kql_op, String ch_op)
{
    String new_expr;

    ParserToken s_lparen(TokenType::OpeningRoundBracket);

    ASTPtr select;
    Expected expected;

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    --token_pos;
    --token_pos;
    return ch_op;

}

String KQLOperators::genHaystackOpExpr(std::vector<String> &tokens,IParser::Pos &token_pos,String kql_op, String ch_op, WildcardsPos wildcards_pos, WildcardsPos space_pos)
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

    if (!tokens.empty() && (token_pos->type == DB::TokenType::StringLiteral || token_pos->type == DB::TokenType::QuotedIdentifier))
        new_expr = translate(
            tokens.back(),
            "'" + left_wildcards + left_space + DB::IParserKQLFunction::escapeSingleQuotes(String(token_pos->begin + 1, token_pos->end - 1))
                + right_space + right_wildcards + "'");
    else if (!tokens.empty() && token_pos->type == DB::TokenType::BareWord)
    {
        auto tmp_arg = DB::IParserKQLFunction::getExpression(token_pos);
        new_expr = translate(
            tokens.back(), "concat('" + left_wildcards + left_space + "', " + tmp_arg + ", '" + right_space + right_wildcards + "')");
    }
    else
        throw DB::Exception(DB::ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", kql_op);

    tokens.pop_back();
    return new_expr;
}

namespace DB
{
bool KQLOperators::convert(std::vector<String> & tokens, IParser::Pos & pos)
{
    if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        return false;

    auto begin = pos;
    auto token = String(pos->begin, pos->end);

    String op = token;
    if (token == "!")
    {
        ++pos;
        if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid negative operator");
        op = "!" + String(pos->begin, pos->end);
    }
    else if (token == "matches")
    {
        ++pos;
        if (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            if (String(pos->begin, pos->end) == "regex")
                op += " regex";
            else
                --pos;
        }
    }

    ++pos;
    if (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (String(pos->begin, pos->end) == "~")
            op += "~";
        else
            --pos;
    }
    else
        --pos;

    const auto op_it = KQLOperator.find(op);
    if (op_it == KQLOperator.end())
    {
        pos = begin;
        return false;
    }

    String new_expr;

    const auto & op_value = op_it->second;
    if (op_value == KQLOperatorValue::none)
    {
        tokens.push_back(op);
        return true;
    }

    if (tokens.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Syntax error near {}", op);

    auto last_op = tokens.back();
    auto last_pos = pos;

    switch (op_value)
    {
        case KQLOperatorValue::contains:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "ilike({0}, {1})"), WildcardsPos::both);
            break;

        case KQLOperatorValue::not_contains:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "not ilike({0}, {1})"), WildcardsPos::both);
            break;

        case KQLOperatorValue::contains_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "like({0}, {1})"), WildcardsPos::both);
            break;

        case KQLOperatorValue::not_contains_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "not like({0}, {1})"), WildcardsPos::both);
            break;

        case KQLOperatorValue::endswith:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "ilike({0}, {1})"), WildcardsPos::left);
            break;

        case KQLOperatorValue::not_endswith:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "not ilike({0}, {1})"), WildcardsPos::left);
            break;

        case KQLOperatorValue::endswith_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "endsWith({0}, {1})"), WildcardsPos::none);
            break;

        case KQLOperatorValue::not_endswith_cs:
            new_expr
                = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "not endsWith({0}, {1})"), WildcardsPos::none);
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
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&constructHasOperatorTranslation, op_value), WildcardsPos::none);
            break;

        case KQLOperatorValue::not_has:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&constructHasOperatorTranslation, op_value), WildcardsPos::none);
            break;

        case KQLOperatorValue::has_all:
        case KQLOperatorValue::has_any:
            new_expr = genHasAnyAllOpExpr(tokens, pos, op, std::bind_front(&constructHasOperatorTranslation, op_value));
            break;

        case KQLOperatorValue::has_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&constructHasOperatorTranslation, op_value), WildcardsPos::none);
            break;

        case KQLOperatorValue::not_has_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&constructHasOperatorTranslation, op_value), WildcardsPos::none);
            break;

        case KQLOperatorValue::hasprefix:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "ilike({0}, {1})"), WildcardsPos::right);
            new_expr += " or ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(
                tokens, last_pos, op, std::bind_front(&applyFormatString, "ilike({0}, {1})"), WildcardsPos::both, WildcardsPos::left);
            break;

        case KQLOperatorValue::not_hasprefix:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "not ilike({0}, {1})"), WildcardsPos::right);
            new_expr += " and ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(
                tokens, last_pos, op, std::bind_front(&applyFormatString, "not ilike({0}, {1})"), WildcardsPos::both, WildcardsPos::left);
            break;

        case KQLOperatorValue::hasprefix_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "startsWith({0}, {1})"), WildcardsPos::none);
            new_expr += " or ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(
                tokens, last_pos, op, std::bind_front(&applyFormatString, "like({0}, {1})"), WildcardsPos::both, WildcardsPos::left);
            break;

        case KQLOperatorValue::not_hasprefix_cs:
            new_expr
                = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "not startsWith({0}, {1})"), WildcardsPos::none);
            new_expr += " and  ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(
                tokens, last_pos, op, std::bind_front(&applyFormatString, "not like({0}, {1})"), WildcardsPos::both, WildcardsPos::left);
            break;

        case KQLOperatorValue::hassuffix:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "ilike({0}, {1})"), WildcardsPos::left);
            new_expr += " or ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(
                tokens, last_pos, op, std::bind_front(&applyFormatString, "ilike({0}, {1})"), WildcardsPos::both, WildcardsPos::right);
            break;

        case KQLOperatorValue::not_hassuffix:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "not ilike({0}, {1})"), WildcardsPos::left);
            new_expr += " and ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(
                tokens, last_pos, op, std::bind_front(&applyFormatString, "not ilike({0}, {1})"), WildcardsPos::both, WildcardsPos::right);
            break;

        case KQLOperatorValue::hassuffix_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "endsWith({0}, {1})"), WildcardsPos::none);
            new_expr += " or ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(
                tokens, last_pos, op, std::bind_front(&applyFormatString, "like({0}, {1})"), WildcardsPos::both, WildcardsPos::right);
            break;

        case KQLOperatorValue::not_hassuffix_cs:
            new_expr
                = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "not endsWith({0}, {1})"), WildcardsPos::none);
            new_expr += " and  ";
            tokens.push_back(last_op);
            new_expr += genHaystackOpExpr(
                tokens, last_pos, op, std::bind_front(&applyFormatString, "not like({0}, {1})"), WildcardsPos::both, WildcardsPos::right);
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
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "match({0}, {1})"), WildcardsPos::none);
            break;

        case KQLOperatorValue::startswith:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "ilike({0}, {1})"), WildcardsPos::right);
            break;

        case KQLOperatorValue::not_startswith:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "not ilike({0}, {1})"), WildcardsPos::right);
            break;

        case KQLOperatorValue::startswith_cs:
            new_expr = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "startsWith({0}, {1})"), WildcardsPos::none);
            break;

        case KQLOperatorValue::not_startswith_cs:
            new_expr
                = genHaystackOpExpr(tokens, pos, op, std::bind_front(&applyFormatString, "not startsWith({0}, {1})"), WildcardsPos::none);
            break;

        case KQLOperatorValue::between:
            new_expr = genBetweenOpExpr(tokens, pos, "kql_between");
            break;

        case KQLOperatorValue::not_between:
            new_expr = genBetweenOpExpr(tokens, pos, "not kql_between");
            break;

        default:
            break;
    }

    tokens.push_back(new_expr);
    return true;
}
}

