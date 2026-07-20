#include <Parsers/ParserCase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Core/Field.h>

namespace DB
{

bool ParserCase::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_case{Keyword::CASE};
    ParserKeyword s_when{Keyword::WHEN};
    ParserKeyword s_then{Keyword::THEN};
    ParserKeyword s_else{Keyword::ELSE};
    ParserKeyword s_end{ Keyword::END};
    ParserExpressionWithOptionalAlias p_expr{false};

    if (!s_case.ignore(pos, expected))
        return false;

    auto old_pos = pos;
    bool has_case_expr = !s_when.ignore(pos, expected);
    pos = old_pos;

    ASTs args;

    auto parse_branches = [&]()
    {
        bool has_branch = false;
        while (s_when.ignore(pos, expected))
        {
            has_branch = true;

            ASTPtr expr_when;
            if (!p_expr.parse(pos, expr_when, expected))
                return false;
            args.push_back(expr_when);

            if (!s_then.ignore(pos, expected))
                return false;

            ASTPtr expr_then;
            if (!p_expr.parse(pos, expr_then, expected))
                return false;
            args.push_back(expr_then);
        }

        if (!has_branch)
            return false;

        ASTPtr expr_else;
        if (s_else.ignore(pos, expected))
        {
            if (!p_expr.parse(pos, expr_else, expected))
                return false;
        }
        else
        {
            Field field_with_null;
            ASTLiteral null_literal(field_with_null);
            expr_else = std::make_shared<ASTLiteral>(null_literal);
        }
        args.push_back(expr_else);

        return s_end.ignore(pos, expected);
    };

    if (has_case_expr)
    {
        ASTPtr case_expr;
        if (!p_expr.parse(pos, case_expr, expected))
            return false;
        args.push_back(case_expr);

        if (!parse_branches())
            return false;

        auto function_args = std::make_shared<ASTExpressionList>();
        function_args->children = std::move(args);

        auto function = std::make_shared<ASTFunction>();
        function->name = "caseWithExpression";
        function->arguments = function_args;
        function->children.push_back(function->arguments);

        node = function;
    }
    else
    {
        if (!parse_branches())
            return false;

        auto function_args = std::make_shared<ASTExpressionList>();
        function_args->children = std::move(args);

        auto function = std::make_shared<ASTFunction>();
        function->name = "multiIf";
        function->arguments = function_args;
        function->children.push_back(function->arguments);

        node = function;
    }

    return true;
}

}
