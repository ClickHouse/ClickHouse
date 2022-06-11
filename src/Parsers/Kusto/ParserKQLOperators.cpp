#include <Parsers/ASTLiteral.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLOperators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

String KQLOperators::genHaystackOpExpr(std::vector<String> &tokens,IParser::Pos &token_pos,String kql_op, String ch_op, WildcardsPos wildcards_pos)
{
    String new_expr, left_wildcards, right_wildcards;

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

    if (!tokens.empty() && ((++token_pos)->type == TokenType::StringLiteral || token_pos->type == TokenType::QuotedIdentifier))
       new_expr = ch_op +"(" + tokens.back() +", '"+left_wildcards + String(token_pos->begin + 1,token_pos->end - 1 ) + right_wildcards + "')";
    else
        throw Exception("Syntax error near " + kql_op, ErrorCodes::SYNTAX_ERROR);
    tokens.pop_back();
    return new_expr;
}

String KQLOperators::getExprFromToken(IParser::Pos pos)
{
    String res;
    std::vector<String> tokens;

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        KQLOperatorValue op_value = KQLOperatorValue::none;

        auto token =  String(pos->begin,pos->end);

        String op = token;
        if ( token == "!" )
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

        if (KQLOperator.find(op) != KQLOperator.end())
           op_value = KQLOperator[op];

        String new_expr;
        if (op_value == KQLOperatorValue::none)
            tokens.push_back(op);
        else
        {
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
                break;

            case KQLOperatorValue::has_any:
                break;

            case KQLOperatorValue::has_cs:
                new_expr = genHaystackOpExpr(tokens, pos, op, "hasToken", WildcardsPos::none);
                break;

            case KQLOperatorValue::not_has_cs:
                new_expr = genHaystackOpExpr(tokens, pos, op, "not hasToken", WildcardsPos::none);
                break;

            case KQLOperatorValue::hasprefix:
                break;

            case KQLOperatorValue::not_hasprefix:
                break;

            case KQLOperatorValue::hasprefix_cs:
                break;

            case KQLOperatorValue::not_hasprefix_cs:
                break;

            case KQLOperatorValue::hassuffix:
                break;

            case KQLOperatorValue::not_hassuffix:
                break;

            case KQLOperatorValue::hassuffix_cs:
                break;

            case KQLOperatorValue::not_hassuffix_cs:
                break;

            case KQLOperatorValue::in_cs:
                new_expr = "in";
                break;

            case KQLOperatorValue::not_in_cs:
                new_expr = "not in";
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
        ++pos;
    }

    for (auto & token : tokens)
        res = res + token + " ";

    return res;
}

}
