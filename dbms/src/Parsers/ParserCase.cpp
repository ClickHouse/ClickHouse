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
    ParserKeyword s_case{"CASE"};
    ParserKeyword s_when{"WHEN"};
    ParserKeyword s_then{"THEN"};
    ParserKeyword s_else{"ELSE"};
    ParserKeyword s_end{ "END"};
    ParserExpressionWithOptionalAlias p_expr{false};

    if (!s_case.parse(pos, node, expected))
    {
        /// Parse as a simple ASTFunction.
        return ParserFunction{}.parse(pos, node, expected);
    }

    auto old_pos = pos;
    bool has_case_expr = !s_when.parse(pos, node, expected);
    pos = old_pos;

    ASTs args;

    auto parse_branches = [&]()
    {
        bool has_branch = false;
        while (s_when.parse(pos, node, expected))
        {
            has_branch = true;

            ASTPtr expr_when;
            if (!p_expr.parse(pos, expr_when, expected))
                return false;
            args.push_back(expr_when);

            if (!s_then.parse(pos, node, expected))
                return false;

            ASTPtr expr_then;
            if (!p_expr.parse(pos, expr_then, expected))
                return false;
            args.push_back(expr_then);
        }

        if (!has_branch)
            return false;

        if (!s_else.parse(pos, node, expected))
            return false;

        ASTPtr expr_else;
        if (!p_expr.parse(pos, expr_else, expected))
            return false;
        args.push_back(expr_else);

        if (!s_end.parse(pos, node, expected))
            return false;

        return true;
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
