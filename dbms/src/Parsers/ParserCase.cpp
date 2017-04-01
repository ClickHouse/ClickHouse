#include <Parsers/ParserCase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Core/Field.h>

namespace DB
{

bool ParserCase::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserWhiteSpaceOrComments ws;
    ParserString s_case{"CASE", true, true};
    ParserString s_when{"WHEN", true, true};
    ParserString s_then{"THEN", true, true};
    ParserString s_else{"ELSE", true, true};
    ParserString s_end{ "END",  true, true};
    ParserExpressionWithOptionalAlias p_expr{false};

    if (!s_case.parse(pos, end, node, max_parsed_pos, expected))
    {
        /// Parse as a simple ASTFunction.
        return ParserFunction{}.parse(pos = begin, end, node, max_parsed_pos, expected);
    }

    ws.ignore(pos, end);

    bool has_case_expr = false;

    auto old_pos = pos;
    has_case_expr = !s_when.parse(pos, end, node, max_parsed_pos, expected);
    pos = old_pos;

    ASTs args;

    auto parse_branches = [&]()
    {
        bool has_branch = false;
        while (s_when.parse(pos, end, node, max_parsed_pos, expected))
        {
            has_branch = true;

            ws.ignore(pos, end);

            ASTPtr expr_when;
            if (!p_expr.parse(pos, end, expr_when, max_parsed_pos, expected))
                return false;
            args.push_back(expr_when);

            ws.ignore(pos, end);

            if (!s_then.parse(pos, end, node, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);

            ASTPtr expr_then;
            if (!p_expr.parse(pos, end, expr_then, max_parsed_pos, expected))
                return false;
            args.push_back(expr_then);

            ws.ignore(pos, end);
        }

        if (!has_branch)
            return false;

        ws.ignore(pos, end);

        if (!s_else.parse(pos, end, node, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        ASTPtr expr_else;
        if (!p_expr.parse(pos, end, expr_else, max_parsed_pos, expected))
            return false;
        args.push_back(expr_else);

        ws.ignore(pos, end);

        if (!s_end.parse(pos, end, node, max_parsed_pos, expected))
            return false;

        return true;
    };

    if (has_case_expr)
    {
        ASTPtr case_expr;
        if (!p_expr.parse(pos, end, case_expr, max_parsed_pos, expected))
            return false;
        args.push_back(case_expr);

        ws.ignore(pos, end);

        if (!parse_branches())
            return false;

        auto function_args = std::make_shared<ASTExpressionList>(StringRange{begin, pos});
        function_args->children = std::move(args);

        auto function = std::make_shared<ASTFunction>(StringRange{begin, pos});
        function->name = "caseWithExpr";
        function->arguments = function_args;
        function->children.push_back(function->arguments);

        node = function;
    }
    else
    {
        if (!parse_branches())
            return false;

        auto function_args = std::make_shared<ASTExpressionList>(StringRange{begin, pos});
        function_args->children = std::move(args);

        auto function = std::make_shared<ASTFunction>(StringRange{begin, pos});
        function->name = "caseWithoutExpr";
        function->arguments = function_args;
        function->children.push_back(function->arguments);

        node = function;
    }

    return true;
}

}
