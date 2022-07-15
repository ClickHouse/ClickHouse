#include <Parsers/ASTLiteral.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

String KQLOperators::genHasAnyAllOpExpr(std::vector<String> &tokens,IParser::Pos &token_pos,String kql_op, String ch_op)
{
    String new_expr;
    Expected expected;
    ParserToken s_lparen(TokenType::OpeningRoundBracket);

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw Exception("Syntax error near " + kql_op, ErrorCodes::SYNTAX_ERROR);

    auto haystack = tokens.back();

    String logic_op = (kql_op == "has_all") ? " and " : " or ";

    while (!token_pos->isEnd() && token_pos->type != TokenType::PipeMark && token_pos->type != TokenType::Semicolon)
    {
        String tmp_arg = String(token_pos->begin, token_pos->end);
        if (token_pos->type == TokenType::BareWord )
        {
            String new_arg;
            auto fun = KQLFunctionFactory::get(tmp_arg);
            if (fun && fun->convert(new_arg,token_pos))
                tmp_arg = new_arg;
        }

        if (token_pos->type == TokenType::Comma )
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

String KQLOperators::genInOpExpr(IParser::Pos &token_pos,String kql_op, String ch_op)
{
    ParserKQLTaleFunction kqlfun_p;
    String new_expr;

    ParserToken s_lparen(TokenType::OpeningRoundBracket);

    ASTPtr select;
    Expected expected;

    ++token_pos;
    if (!s_lparen.ignore(token_pos, expected))
        throw Exception("Syntax error near " + kql_op, ErrorCodes::SYNTAX_ERROR);

    auto pos = token_pos;
    if (kqlfun_p.parse(pos,select,expected))
    {
        new_expr = ch_op + " kql";
        auto tmp_pos = token_pos;
        while (tmp_pos != pos) 
        {
            new_expr = new_expr + " " + String(tmp_pos->begin,tmp_pos->end);
            ++tmp_pos;
        }

        if (pos->type != TokenType::ClosingRoundBracket)
            throw Exception("Syntax error near " + kql_op, ErrorCodes::SYNTAX_ERROR);

        token_pos = pos;
        return new_expr;
    }

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
            left_wildcards ="%";
            break;

        case WildcardsPos::right:
            right_wildcards = "%";
            break;

        case WildcardsPos::both:
            left_wildcards ="%";
            right_wildcards = "%";
            break;
    }

    switch (space_pos)
    {
        case WildcardsPos::none:
            break;

        case WildcardsPos::left:
            left_space =" ";
            break;

        case WildcardsPos::right:
            right_space = " ";
            break;

        case WildcardsPos::both:
            left_space =" ";
            right_space = " ";
            break;
    }

    ++token_pos;

    if (!tokens.empty() && ((token_pos)->type == TokenType::StringLiteral || token_pos->type == TokenType::QuotedIdentifier))
        new_expr = ch_op +"(" + tokens.back() +", '"+left_wildcards + left_space + String(token_pos->begin + 1,token_pos->end - 1) + right_space + right_wildcards + "')";
    else if (!tokens.empty() && ((token_pos)->type == TokenType::BareWord))
    {
        String tmp_arg = String(token_pos->begin,token_pos->end);
        if (token_pos->type == TokenType::BareWord )
        {
            String new_arg;
            auto fun = KQLFunctionFactory::get(tmp_arg);
            if (fun && fun->convert(new_arg,token_pos))
                tmp_arg = new_arg;
        }
        new_expr = ch_op +"(" + tokens.back() +", concat('" + left_wildcards + left_space + "', " + tmp_arg +", '"+ right_space + right_wildcards + "'))";
    }
    else
        throw Exception("Syntax error near " + kql_op, ErrorCodes::SYNTAX_ERROR);
    tokens.pop_back();
    return new_expr;
}

bool KQLOperators::convert(std::vector<String> &tokens,IParser::Pos &pos)
{
    auto begin = pos;

    if (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        KQLOperatorValue op_value = KQLOperatorValue::none;

        auto token =  String(pos->begin,pos->end);

        String op = token;
        if (token == "!")
        {
            ++pos;
            if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
                throw Exception("Invalid negative operator", ErrorCodes::SYNTAX_ERROR);
            op ="!"+String(pos->begin,pos->end);
        }
        else if (token == "matches")
        {
            ++pos;
            if (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
            {
                if (String(pos->begin,pos->end) == "regex")
                    op +=" regex";
                else
                    --pos;
            }
        }
        else
        {
            op = token;
        }

        ++pos;
        if (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            if (String(pos->begin,pos->end) == "~")
                op +="~";
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

        op_value = KQLOperator[op];

        String new_expr;

        if (op_value == KQLOperatorValue::none)
            tokens.push_back(op);
        else
        {
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
                break;

            case KQLOperatorValue::not_equal:
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
                new_expr = genHasAnyAllOpExpr(tokens,pos,"has_all", "hasTokenCaseInsensitive");
                break;

            case KQLOperatorValue::has_any:
                new_expr = genHasAnyAllOpExpr(tokens,pos,"has_any", "hasTokenCaseInsensitive");
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
                new_expr = genInOpExpr(pos,op,"in");
                break;

            case KQLOperatorValue::not_in_cs:
                new_expr = genInOpExpr(pos,op,"not in");
                break;

            case KQLOperatorValue::in:
                break;

            case KQLOperatorValue::not_in:
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
