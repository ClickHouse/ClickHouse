#include <Parsers/ASTLiteral.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLOperators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

String KQLOperators::genHaystackOpExpr(std::vector<String> &tokens,IParser::Pos &tokenPos,String KQLOp, String CHOp, WildcardsPos wildcardsPos)
{
    String new_expr, leftWildcards= "", rightWildcards="";

    switch (wildcardsPos)
    {
        case WildcardsPos::none:
            break;

        case WildcardsPos::left:
            leftWildcards ="%";
            break;

        case WildcardsPos::right:
            rightWildcards = "%";
            break;

        case WildcardsPos::both:
            leftWildcards ="%";
            rightWildcards = "%";
            break;
    }

    if (!tokens.empty() && ((++tokenPos)->type == TokenType::StringLiteral || tokenPos->type == TokenType::QuotedIdentifier))
       new_expr = CHOp +"(" + tokens.back() +", '"+leftWildcards + String(tokenPos->begin + 1,tokenPos->end - 1 ) + rightWildcards + "')";
    else
        throw Exception("Syntax error near " + KQLOp, ErrorCodes::SYNTAX_ERROR);
    tokens.pop_back();
    return new_expr;
}

String KQLOperators::getExprFromToken(IParser::Pos pos)
{
    String res;
    std::vector<String> tokens;

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        KQLOperatorValue opValue = KQLOperatorValue::none;

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

        if (KQLOperator.find(op) != KQLOperator.end())
           opValue = KQLOperator[op];

        String new_expr;
        if (opValue == KQLOperatorValue::none)
            tokens.push_back(op);
        else
        {
            switch (opValue)
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

    for (auto it=tokens.begin(); it!=tokens.end(); ++it)
        res = res + *it + " ";

    return res;
}

}
