#include <string_view>

#include <Parsers/ExpressionListParsers.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserUnionQueryElement.h>
#include <Parsers/parseIntervalKind.h>
#include <Common/StringUtils/StringUtils.h>

#include <Parsers/ParserSelectWithUnionQuery.h>

#include <Common/logger_useful.h>
#include <Parsers/queryToString.h>

using namespace std::literals;


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

const char * ParserMultiplicativeExpression::operators[] =
{
    "*",     "multiply",
    "/",     "divide",
    "%",     "modulo",
    "MOD",   "modulo",
    "DIV",   "intDiv",
    nullptr
};

const char * ParserUnaryExpression::operators[] =
{
    "-",     "negate",
    "NOT",   "not",
    nullptr
};

const char * ParserAdditiveExpression::operators[] =
{
    "+",     "plus",
    "-",     "minus",
    nullptr
};

const char * ParserComparisonExpression::operators[] =
{
    "==",            "equals",
    "!=",            "notEquals",
    "<>",            "notEquals",
    "<=",            "lessOrEquals",
    ">=",            "greaterOrEquals",
    "<",             "less",
    ">",             "greater",
    "=",             "equals",
    "LIKE",          "like",
    "ILIKE",         "ilike",
    "NOT LIKE",      "notLike",
    "NOT ILIKE",     "notILike",
    "IN",            "in",
    "NOT IN",        "notIn",
    "GLOBAL IN",     "globalIn",
    "GLOBAL NOT IN", "globalNotIn",
    nullptr
};

const char * ParserComparisonExpression::overlapping_operators_to_skip[] =
{
    "IN PARTITION",
    nullptr
};

const char * ParserLogicalNotExpression::operators[] =
{
    "NOT", "not",
    nullptr
};

const char * ParserArrayElementExpression::operators[] =
{
    "[", "arrayElement",
    nullptr
};

const char * ParserTupleElementExpression::operators[] =
{
    ".", "tupleElement",
    nullptr
};


bool ParserList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTs elements;

    auto parse_element = [&]
    {
        ASTPtr element;
        if (!elem_parser->parse(pos, element, expected))
            return false;

        elements.push_back(element);
        return true;
    };

    if (!parseUtil(pos, expected, parse_element, *separator_parser, allow_empty))
        return false;

    auto list = std::make_shared<ASTExpressionList>(result_separator);
    list->children = std::move(elements);
    node = list;

    return true;
}

bool ParserUnionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserUnionQueryElement elem_parser;
    ParserKeyword s_union_parser("UNION");
    ParserKeyword s_all_parser("ALL");
    ParserKeyword s_distinct_parser("DISTINCT");
    ParserKeyword s_except_parser("EXCEPT");
    ParserKeyword s_intersect_parser("INTERSECT");
    ASTs elements;

    auto parse_element = [&]
    {
        ASTPtr element;
        if (!elem_parser.parse(pos, element, expected))
            return false;

        elements.push_back(element);
        return true;
    };

    /// Parse UNION type
    auto parse_separator = [&]
    {
        if (s_union_parser.ignore(pos, expected))
        {
            // SELECT ... UNION ALL SELECT ...
            if (s_all_parser.check(pos, expected))
            {
                union_modes.push_back(SelectUnionMode::ALL);
            }
            // SELECT ... UNION DISTINCT SELECT ...
            else if (s_distinct_parser.check(pos, expected))
            {
                union_modes.push_back(SelectUnionMode::DISTINCT);
            }
            // SELECT ... UNION SELECT ...
            else
            {
                union_modes.push_back(SelectUnionMode::Unspecified);
            }
            return true;
        }
        else if (s_except_parser.check(pos, expected))
        {
            union_modes.push_back(SelectUnionMode::EXCEPT);
            return true;
        }
        else if (s_intersect_parser.check(pos, expected))
        {
            union_modes.push_back(SelectUnionMode::INTERSECT);
            return true;
        }
        return false;
    };

    if (!parseUtil(pos, parse_element, parse_separator))
        return false;

    auto list = std::make_shared<ASTExpressionList>();
    list->children = std::move(elements);
    node = list;
    return true;
}

static bool parseOperator(IParser::Pos & pos, const char * op, Expected & expected)
{
    if (isWordCharASCII(*op))
    {
        return ParserKeyword(op).ignore(pos, expected);
    }
    else
    {
        if (strlen(op) == pos->size() && 0 == memcmp(op, pos->begin, pos->size()))
        {
            ++pos;
            return true;
        }
        return false;
    }
}

enum class SubqueryFunctionType
{
    NONE,
    ANY,
    ALL
};

static bool modifyAST(ASTPtr ast, SubqueryFunctionType type)
{
    /* Rewrite in AST:
     *  = ANY --> IN
     * != ALL --> NOT IN
     *  = ALL --> IN (SELECT singleValueOrNull(*) FROM subquery)
     * != ANY --> NOT IN (SELECT singleValueOrNull(*) FROM subquery)
    **/

    auto * function = assert_cast<ASTFunction *>(ast.get());
    String operator_name = function->name;

    auto function_equals = operator_name == "equals";
    auto function_not_equals = operator_name == "notEquals";

    String aggregate_function_name;
    if (function_equals || function_not_equals)
    {
        if (operator_name == "notEquals")
            function->name = "notIn";
        else
            function->name = "in";

        if ((type == SubqueryFunctionType::ANY && function_equals)
            || (type == SubqueryFunctionType::ALL && function_not_equals))
        {
            return true;
        }

        aggregate_function_name = "singleValueOrNull";
    }
    else if (operator_name == "greaterOrEquals" || operator_name == "greater")
    {
        aggregate_function_name = (type == SubqueryFunctionType::ANY ? "min" : "max");
    }
    else if (operator_name == "lessOrEquals" || operator_name == "less")
    {
        aggregate_function_name = (type == SubqueryFunctionType::ANY ? "max" : "min");
    }
    else
        return false;

    /// subquery --> (SELECT aggregate_function(*) FROM subquery)
    auto aggregate_function = makeASTFunction(aggregate_function_name, std::make_shared<ASTAsterisk>());
    auto subquery_node = function->children[0]->children[1];

    auto table_expression = std::make_shared<ASTTableExpression>();
    table_expression->subquery = std::move(subquery_node);
    table_expression->children.push_back(table_expression->subquery);

    auto tables_in_select_element = std::make_shared<ASTTablesInSelectQueryElement>();
    tables_in_select_element->table_expression = std::move(table_expression);
    tables_in_select_element->children.push_back(tables_in_select_element->table_expression);

    auto tables_in_select = std::make_shared<ASTTablesInSelectQuery>();
    tables_in_select->children.push_back(std::move(tables_in_select_element));

    auto select_exp_list = std::make_shared<ASTExpressionList>();
    select_exp_list->children.push_back(aggregate_function);

    auto select_query = std::make_shared<ASTSelectQuery>();
    select_query->children.push_back(select_exp_list);
    select_query->children.push_back(tables_in_select);

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_exp_list);
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables_in_select);

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->list_of_selects->children.push_back(std::move(select_query));
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

    auto new_subquery = std::make_shared<ASTSubquery>();
    new_subquery->children.push_back(select_with_union_query);
    ast->children[0]->children.back() = std::move(new_subquery);

    return true;
}

bool ParserLeftAssociativeBinaryOperatorList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool first = true;

    auto current_depth = pos.depth;
    while (true)
    {
        if (first)
        {
            ASTPtr elem;
            if (!first_elem_parser->parse(pos, elem, expected))
                return false;

            node = elem;
            first = false;
        }
        else
        {
            /// try to find any of the valid operators

            const char ** it;
            Expected stub;
            for (it = overlapping_operators_to_skip; *it; ++it)
                if (ParserKeyword{*it}.checkWithoutMoving(pos, stub))
                    break;

            if (*it)
                break;

            for (it = operators; *it; it += 2)
                if (parseOperator(pos, *it, expected))
                    break;

            if (!*it)
                break;

            /// the function corresponding to the operator
            auto function = std::make_shared<ASTFunction>();

            /// function arguments
            auto exp_list = std::make_shared<ASTExpressionList>();

            ASTPtr elem;
            SubqueryFunctionType subquery_function_type = SubqueryFunctionType::NONE;

            if (comparison_expression)
            {
                if (ParserKeyword("ANY").ignore(pos, expected))
                    subquery_function_type = SubqueryFunctionType::ANY;
                else if (ParserKeyword("ALL").ignore(pos, expected))
                    subquery_function_type = SubqueryFunctionType::ALL;
            }

            if (subquery_function_type != SubqueryFunctionType::NONE && !ParserSubquery().parse(pos, elem, expected))
                subquery_function_type = SubqueryFunctionType::NONE;

            if (subquery_function_type == SubqueryFunctionType::NONE
                && !(remaining_elem_parser ? remaining_elem_parser : first_elem_parser)->parse(pos, elem, expected))
                return false;

            /// the first argument of the function is the previous element, the second is the next one
            function->name = it[1];
            function->arguments = exp_list;
            function->children.push_back(exp_list);

            exp_list->children.push_back(node);
            exp_list->children.push_back(elem);

            if (comparison_expression && subquery_function_type != SubqueryFunctionType::NONE && !modifyAST(function, subquery_function_type))
                return false;

            /** special exception for the access operator to the element of the array `x[y]`, which
              * contains the infix part '[' and the suffix ''] '(specified as' [')
              */
            if (it[0] == "["sv)
            {
                if (pos->type != TokenType::ClosingSquareBracket)
                    return false;
                ++pos;
            }

            /// Left associative operator chain is parsed as a tree: ((((1 + 1) + 1) + 1) + 1)...
            /// We must account it's depth - otherwise we may end up with stack overflow later - on destruction of AST.
            pos.increaseDepth();
            node = function;
        }
    }

    pos.depth = current_depth;
    return true;
}


bool ParserVariableArityOperatorList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr arguments;

    if (!elem_parser->parse(pos, node, expected))
        return false;

    while (true)
    {
        if (!parseOperator(pos, infix, expected))
            break;

        if (!arguments)
        {
            node = makeASTFunction(function_name, node);
            arguments = node->as<ASTFunction &>().arguments;
        }

        ASTPtr elem;
        if (!elem_parser->parse(pos, elem, expected))
            return false;

        arguments->children.push_back(elem);
    }

    return true;
}

bool ParserBetweenExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// For the expression (subject [NOT] BETWEEN left AND right)
    /// create an AST the same as for (subject >= left AND subject <= right).

    ParserKeyword s_not("NOT");
    ParserKeyword s_between("BETWEEN");
    ParserKeyword s_and("AND");

    ASTPtr subject;
    ASTPtr left;
    ASTPtr right;

    if (!elem_parser.parse(pos, subject, expected))
        return false;

    bool negative = s_not.ignore(pos, expected);

    if (!s_between.ignore(pos, expected))
    {
        if (negative)
            --pos;

        /// No operator was parsed, just return element.
        node = subject;
    }
    else
    {
        if (!elem_parser.parse(pos, left, expected))
            return false;

        if (!s_and.ignore(pos, expected))
            return false;

        if (!elem_parser.parse(pos, right, expected))
            return false;

        auto f_combined_expression = std::make_shared<ASTFunction>();
        auto args_combined_expression = std::make_shared<ASTExpressionList>();

        /// [NOT] BETWEEN left AND right
        auto f_left_expr = std::make_shared<ASTFunction>();
        auto args_left_expr = std::make_shared<ASTExpressionList>();

        auto f_right_expr = std::make_shared<ASTFunction>();
        auto args_right_expr = std::make_shared<ASTExpressionList>();

        args_left_expr->children.emplace_back(subject);
        args_left_expr->children.emplace_back(left);

        args_right_expr->children.emplace_back(subject);
        args_right_expr->children.emplace_back(right);

        if (negative)
        {
            /// NOT BETWEEN
            f_left_expr->name = "less";
            f_right_expr->name = "greater";
            f_combined_expression->name = "or";
        }
        else
        {
            /// BETWEEN
            f_left_expr->name = "greaterOrEquals";
            f_right_expr->name = "lessOrEquals";
            f_combined_expression->name = "and";
        }

        f_left_expr->arguments = args_left_expr;
        f_left_expr->children.emplace_back(f_left_expr->arguments);

        f_right_expr->arguments = args_right_expr;
        f_right_expr->children.emplace_back(f_right_expr->arguments);

        args_combined_expression->children.emplace_back(f_left_expr);
        args_combined_expression->children.emplace_back(f_right_expr);

        f_combined_expression->arguments = args_combined_expression;
        f_combined_expression->children.emplace_back(f_combined_expression->arguments);

        node = f_combined_expression;
    }

    return true;
}

bool ParserTernaryOperatorExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken symbol1(TokenType::QuestionMark);
    ParserToken symbol2(TokenType::Colon);

    ASTPtr elem_cond;
    ASTPtr elem_then;
    ASTPtr elem_else;

    if (!elem_parser.parse(pos, elem_cond, expected))
        return false;

    if (!symbol1.ignore(pos, expected))
        node = elem_cond;
    else
    {
        if (!elem_parser.parse(pos, elem_then, expected))
            return false;

        if (!symbol2.ignore(pos, expected))
            return false;

        if (!elem_parser.parse(pos, elem_else, expected))
            return false;

        /// the function corresponding to the operator
        auto function = std::make_shared<ASTFunction>();

        /// function arguments
        auto exp_list = std::make_shared<ASTExpressionList>();

        function->name = "if";
        function->arguments = exp_list;
        function->children.push_back(exp_list);

        exp_list->children.push_back(elem_cond);
        exp_list->children.push_back(elem_then);
        exp_list->children.push_back(elem_else);

        node = function;
    }

    return true;
}


bool ParserLambdaExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken arrow(TokenType::Arrow);
    ParserToken open(TokenType::OpeningRoundBracket);
    ParserToken close(TokenType::ClosingRoundBracket);

    Pos begin = pos;

    do
    {
        ASTPtr inner_arguments;
        ASTPtr expression;

        bool was_open = false;

        if (open.ignore(pos, expected))
        {
            was_open = true;
        }

        if (!ParserList(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma)).parse(pos, inner_arguments, expected))
            break;

        if (was_open)
        {
            if (!close.ignore(pos, expected))
                break;
        }

        if (!arrow.ignore(pos, expected))
            break;

        if (!elem_parser.parse(pos, expression, expected))
            return false;

        /// lambda(tuple(inner_arguments), expression)

        auto lambda = std::make_shared<ASTFunction>();
        node = lambda;
        lambda->name = "lambda";

        auto outer_arguments = std::make_shared<ASTExpressionList>();
        lambda->arguments = outer_arguments;
        lambda->children.push_back(lambda->arguments);

        auto tuple = std::make_shared<ASTFunction>();
        outer_arguments->children.push_back(tuple);
        tuple->name = "tuple";
        tuple->arguments = inner_arguments;
        tuple->children.push_back(inner_arguments);

        outer_arguments->children.push_back(expression);

        return true;
    }
    while (false);

    pos = begin;
    return elem_parser.parse(pos, node, expected);
}


ASTPtr makeBetweenOperator(bool negative, ASTs arguments)
{
    // subject = arguments[0], left = arguments[1], right = arguments[2]
    auto f_combined_expression = std::make_shared<ASTFunction>();
    auto args_combined_expression = std::make_shared<ASTExpressionList>();

    /// [NOT] BETWEEN left AND right
    auto f_left_expr = std::make_shared<ASTFunction>();
    auto args_left_expr = std::make_shared<ASTExpressionList>();

    auto f_right_expr = std::make_shared<ASTFunction>();
    auto args_right_expr = std::make_shared<ASTExpressionList>();

    args_left_expr->children.emplace_back(arguments[0]);
    args_left_expr->children.emplace_back(arguments[1]);

    args_right_expr->children.emplace_back(arguments[0]);
    args_right_expr->children.emplace_back(arguments[2]);

    if (negative)
    {
        /// NOT BETWEEN
        f_left_expr->name = "less";
        f_right_expr->name = "greater";
        f_combined_expression->name = "or";
    }
    else
    {
        /// BETWEEN
        f_left_expr->name = "greaterOrEquals";
        f_right_expr->name = "lessOrEquals";
        f_combined_expression->name = "and";
    }

    f_left_expr->arguments = args_left_expr;
    f_left_expr->children.emplace_back(f_left_expr->arguments);

    f_right_expr->arguments = args_right_expr;
    f_right_expr->children.emplace_back(f_right_expr->arguments);

    args_combined_expression->children.emplace_back(f_left_expr);
    args_combined_expression->children.emplace_back(f_right_expr);

    f_combined_expression->arguments = args_combined_expression;
    f_combined_expression->children.emplace_back(f_combined_expression->arguments);

    return f_combined_expression;
}


bool ParserTableFunctionExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (ParserTableFunctionView().parse(pos, node, expected))
        return true;
    return elem_parser.parse(pos, node, expected);
}


bool ParserPrefixUnaryOperatorExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// try to find any of the valid operators
    const char ** it;
    for (it = operators; *it; it += 2)
    {
        if (parseOperator(pos, *it, expected))
            break;
    }

    /// Let's parse chains of the form `NOT NOT x`. This is hack.
    /** This is done, because among the unary operators there is only a minus and NOT.
      * But for a minus the chain of unary operators does not need to be supported.
      */
    size_t count = 1;
    if (it[0] && 0 == strncmp(it[0], "NOT", 3))
    {
        while (true)
        {
            const char ** jt;
            for (jt = operators; *jt; jt += 2)
                if (parseOperator(pos, *jt, expected))
                    break;

            if (!*jt)
                break;

            ++count;
        }
    }

    ASTPtr elem;
    if (!elem_parser->parse(pos, elem, expected))
        return false;

    if (!*it)
        node = elem;
    else
    {
        for (size_t i = 0; i < count; ++i)
        {
            /// the function corresponding to the operator
            auto function = std::make_shared<ASTFunction>();

            /// function arguments
            auto exp_list = std::make_shared<ASTExpressionList>();

            function->name = it[1];
            function->arguments = exp_list;
            function->children.push_back(exp_list);

            if (node)
                exp_list->children.push_back(node);
            else
                exp_list->children.push_back(elem);

            node = function;
        }
    }

    return true;
}


bool ParserUnaryExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// As an exception, negative numbers should be parsed as literals, and not as an application of the operator.

    if (pos->type == TokenType::Minus)
    {
        Pos begin = pos;
        if (ParserCastOperator().parse(pos, node, expected))
            return true;

        pos = begin;
        if (ParserLiteral().parse(pos, node, expected))
            return true;

        pos = begin;
    }

    return operator_parser.parse(pos, node, expected);
}


bool ParserCastExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr expr_ast;
    if (!elem_parser->parse(pos, expr_ast, expected))
        return false;

    ASTPtr type_ast;
    if (ParserToken(TokenType::DoubleColon).ignore(pos, expected)
        && ParserDataType().parse(pos, type_ast, expected))
    {
        node = createFunctionCast(expr_ast, type_ast);
    }
    else
    {
        node = expr_ast;
    }

    return true;
}


bool ParserArrayElementExpression::parseImpl(Pos & pos, ASTPtr & node, Expected &expected)
{
    return ParserLeftAssociativeBinaryOperatorList{
        operators,
        std::make_unique<ParserCastExpression>(std::make_unique<ParserExpressionElement>()),
        std::make_unique<ParserExpressionWithOptionalAlias>(false)
    }.parse(pos, node, expected);
}


bool ParserTupleElementExpression::parseImpl(Pos & pos, ASTPtr & node, Expected &expected)
{
    return ParserLeftAssociativeBinaryOperatorList{
        operators,
        std::make_unique<ParserCastExpression>(std::make_unique<ParserArrayElementExpression>()),
        std::make_unique<ParserUnsignedInteger>()
    }.parse(pos, node, expected);
}


ParserExpressionWithOptionalAlias::ParserExpressionWithOptionalAlias(bool allow_alias_without_as_keyword, bool is_table_function)
    : impl(std::make_unique<ParserWithOptionalAlias>(
        is_table_function ? ParserPtr(std::make_unique<ParserTableFunctionExpression>()) : ParserPtr(std::make_unique<ParserExpression>()),
        allow_alias_without_as_keyword))
{
}


bool ParserExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(
        std::make_unique<ParserExpressionWithOptionalAlias>(allow_alias_without_as_keyword, is_table_function),
        std::make_unique<ParserToken>(TokenType::Comma))
        .parse(pos, node, expected);
}


bool ParserNotEmptyExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return nested_parser.parse(pos, node, expected) && !node->children.empty();
}

bool ParserNotEmptyExpressionList2::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return nested_parser.parse(pos, node, expected) && !node->children.empty();
}

bool ParserOrderByExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserOrderByElement>(), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

bool ParserGroupingSetsExpressionListElements::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto command_list = std::make_shared<ASTExpressionList>();
    node = command_list;

    ParserToken s_comma(TokenType::Comma);
    ParserToken s_open(TokenType::OpeningRoundBracket);
    ParserToken s_close(TokenType::ClosingRoundBracket);
    ParserExpressionWithOptionalAlias p_expression(false);
    ParserList p_command(std::make_unique<ParserExpressionWithOptionalAlias>(false),
                          std::make_unique<ParserToken>(TokenType::Comma), true);

    do
    {
        Pos begin = pos;
        ASTPtr command;
        if (!s_open.ignore(pos, expected))
        {
            pos = begin;
            if (!p_expression.parse(pos, command, expected))
            {
                return false;
            }
            auto list = std::make_shared<ASTExpressionList>(',');
            list->children.push_back(command);
            command = std::move(list);
        }
        else
        {
            if (!p_command.parse(pos, command, expected))
                return false;

            if (!s_close.ignore(pos, expected))
                break;
        }

        command_list->children.push_back(command);
    }
    while (s_comma.ignore(pos, expected));

    return true;
}

bool ParserGroupingSetsExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserGroupingSetsExpressionListElements grouping_sets_elements;
    return grouping_sets_elements.parse(pos, node, expected);

}

bool ParserInterpolateExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserInterpolateElement>(), std::make_unique<ParserToken>(TokenType::Comma), true)
        .parse(pos, node, expected);
}


bool ParserTTLExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserTTLElement>(), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}


bool ParserNullityChecking::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr node_comp;
    if (!elem_parser.parse(pos, node_comp, expected))
        return false;

    ParserKeyword s_is{"IS"};
    ParserKeyword s_not{"NOT"};
    ParserKeyword s_null{"NULL"};

    if (s_is.ignore(pos, expected))
    {
        bool is_not = false;
        if (s_not.ignore(pos, expected))
            is_not = true;

        if (!s_null.ignore(pos, expected))
            return false;

        auto args = std::make_shared<ASTExpressionList>();
        args->children.push_back(node_comp);

        auto function = std::make_shared<ASTFunction>();
        function->name = is_not ? "isNotNull" : "isNull";
        function->arguments = args;
        function->children.push_back(function->arguments);

        node = function;
    }
    else
        node = node_comp;

    return true;
}

bool ParserDateOperatorExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;

    /// If no DATE keyword, go to the nested parser.
    if (!ParserKeyword("DATE").ignore(pos, expected))
        return next_parser.parse(pos, node, expected);

    ASTPtr expr;
    if (!ParserStringLiteral().parse(pos, expr, expected))
    {
        pos = begin;
        return next_parser.parse(pos, node, expected);
    }

    /// the function corresponding to the operator
    auto function = std::make_shared<ASTFunction>();

    /// function arguments
    auto exp_list = std::make_shared<ASTExpressionList>();

    /// the first argument of the function is the previous element, the second is the next one
    function->name = "toDate";
    function->arguments = exp_list;
    function->children.push_back(exp_list);

    exp_list->children.push_back(expr);

    node = function;
    return true;
}

bool ParserTimestampOperatorExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;

    /// If no TIMESTAMP keyword, go to the nested parser.
    if (!ParserKeyword("TIMESTAMP").ignore(pos, expected))
        return next_parser.parse(pos, node, expected);

    ASTPtr expr;
    if (!ParserStringLiteral().parse(pos, expr, expected))
    {
        pos = begin;
        return next_parser.parse(pos, node, expected);
    }

    /// the function corresponding to the operator
    auto function = std::make_shared<ASTFunction>();

    /// function arguments
    auto exp_list = std::make_shared<ASTExpressionList>();

    /// the first argument of the function is the previous element, the second is the next one
    function->name = "toDateTime";
    function->arguments = exp_list;
    function->children.push_back(exp_list);

    exp_list->children.push_back(expr);

    node = function;
    return true;
}

bool ParserIntervalOperatorExpression::parseArgumentAndIntervalKind(
    Pos & pos, ASTPtr & expr, IntervalKind & interval_kind, Expected & expected)
{
    auto begin = pos;
    auto init_expected = expected;
    ASTPtr string_literal;
    //// A String literal followed INTERVAL keyword,
    /// the literal can be a part of an expression or
    /// include Number and INTERVAL TYPE at the same time
    if (ParserStringLiteral{}.parse(pos, string_literal, expected))
    {
        String literal;
        if (string_literal->as<ASTLiteral &>().value.tryGet(literal))
        {
            Tokens tokens(literal.data(), literal.data() + literal.size());
            Pos token_pos(tokens, 0);
            Expected token_expected;

            if (!ParserNumber{}.parse(token_pos, expr, token_expected))
                return false;
            else
            {
                /// case: INTERVAL '1' HOUR
                /// back to begin
                if (!token_pos.isValid())
                {
                    pos = begin;
                    expected = init_expected;
                }
                else
                    /// case: INTERVAL '1 HOUR'
                    return parseIntervalKind(token_pos, token_expected, interval_kind);
            }
        }
    }
    // case: INTERVAL expr HOUR
    if (!ParserExpressionWithOptionalAlias(false).parse(pos, expr, expected))
        return false;
    return parseIntervalKind(pos, expected, interval_kind);
}

bool ParserIntervalOperatorExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;

    /// If no INTERVAL keyword, go to the nested parser.
    if (!ParserKeyword("INTERVAL").ignore(pos, expected))
        return next_parser.parse(pos, node, expected);

    ASTPtr expr;
    IntervalKind interval_kind;
    if (!parseArgumentAndIntervalKind(pos, expr, interval_kind, expected))
    {
        pos = begin;
        return next_parser.parse(pos, node, expected);
    }

    /// the function corresponding to the operator
    auto function = std::make_shared<ASTFunction>();

    /// function arguments
    auto exp_list = std::make_shared<ASTExpressionList>();

    /// the first argument of the function is the previous element, the second is the next one
    function->name = interval_kind.toNameOfFunctionToIntervalDataType();
    function->arguments = exp_list;
    function->children.push_back(exp_list);

    exp_list->children.push_back(expr);

    node = function;
    return true;
}

bool ParserKeyValuePair::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier id_parser;
    ParserLiteral literal_parser;
    ParserFunction func_parser;

    ASTPtr identifier;
    ASTPtr value;
    bool with_brackets = false;
    if (!id_parser.parse(pos, identifier, expected))
        return false;

    /// If it's neither literal, nor identifier, nor function, than it's possible list of pairs
    if (!func_parser.parse(pos, value, expected) && !literal_parser.parse(pos, value, expected) && !id_parser.parse(pos, value, expected))
    {
        ParserKeyValuePairsList kv_pairs_list;
        ParserToken open(TokenType::OpeningRoundBracket);
        ParserToken close(TokenType::ClosingRoundBracket);

        if (!open.ignore(pos))
            return false;

        if (!kv_pairs_list.parse(pos, value, expected))
            return false;

        if (!close.ignore(pos))
            return false;

        with_brackets = true;
    }

    auto pair = std::make_shared<ASTPair>(with_brackets);
    pair->first = Poco::toLower(identifier->as<ASTIdentifier>()->name());
    pair->set(pair->second, value);
    node = pair;
    return true;
}

bool ParserKeyValuePairsList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserList parser(std::make_unique<ParserKeyValuePair>(), std::make_unique<ParserNothing>(), true, 0);
    return parser.parse(pos, node, expected);
}


enum class Action
{
    OPERAND,
    OPERATOR
};

/** Operator types are needed for special handling of certain operators.
  * Operators can be grouped into some type if they have similar behaviour.
  * Certain operators are unique in terms of their behaviour, so they are assigned a separate type.
  */
enum class OperatorType
{
    None,
    Comparison,
    Mergeable,
    ArrayElement,
    TupleElement,
    IsNull,
    StartBetween,
    StartNotBetween,
    FinishBetween,
    StartIf,
    FinishIf,
    Cast,
    Lambda
};

/** Operator class stores parameters of the operator:
  *  - function_name  name of the function that operator will create
  *  - priority       priority of the operator relative to the other operators
  *  - arity          the amount of arguments that operator will consume
  *  - type           type of the operator that defines its behaviour
  */
class Operator
{
public:
    Operator() = default;

    Operator(String function_name_,
             Int32 priority_,
             Int32 arity_ = 2,
             OperatorType type_ = OperatorType::None) : type(type_), priority(priority_), arity(arity_), function_name(function_name_)
    {
    }

    OperatorType type;
    Int32 priority;
    Int32 arity;
    String function_name;
};

/** Layer is a class that represents context for parsing certain element,
  *  that consists of other elements e.g. f(x1, x2, x3)
  *
  *  - Manages operands and operators for the future elements (arguments)
  *  - Combines operands and operator into one element
  *  - Parsers separators and endings
  *  - Combines resulting arguments into a function
  */

class Layer
{
public:
    virtual ~Layer() = default;

    bool popOperator(Operator & op)
    {
        if (operators.empty())
            return false;

        op = std::move(operators.back());
        operators.pop_back();

        return true;
    }

    void pushOperator(Operator op)
    {
        operators.push_back(std::move(op));
    }

    bool popOperand(ASTPtr & op)
    {
        if (operands.empty())
            return false;

        op = std::move(operands.back());
        operands.pop_back();

        return true;
    }

    void pushOperand(ASTPtr op)
    {
        operands.push_back(std::move(op));
    }

    void pushResult(ASTPtr op)
    {
        result.push_back(std::move(op));
    }

    virtual bool getResult(ASTPtr & op)
    {
        if (result.size() == 1)
        {
            op = std::move(result[0]);
            return true;
        }

        return false;
    }

    virtual bool parse(IParser::Pos & /*pos*/, Expected & /*expected*/, Action & /*action*/)
    {
        return true;
    }

    bool isFinished() const
    {
        return finished;
    }

    int previousPriority() const
    {
        if (operators.empty())
            return 0;

        return operators.back().priority;
    }

    OperatorType previousType() const
    {
        if (operators.empty())
            return OperatorType::None;

        return operators.back().type;
    }

    int empty() const
    {
        return operators.empty() && operands.empty();
    }

    bool popLastNOperands(ASTs & asts, size_t n)
    {
        if (n > operands.size())
            return false;

        asts.reserve(asts.size() + n);

        auto start = operands.begin() + operands.size() - n;
        asts.insert(asts.end(), std::make_move_iterator(start), std::make_move_iterator(operands.end()));
        operands.erase(start, operands.end());

        return true;
    }

    /// Merge operators and operands into a single element (column), then push it to 'result' vector.
    ///  Operators are previously sorted in ascending order of priority
    ///  (operator with priority 1 has higher priority than operator with priority 2),
    ///  so we can just merge them with operands starting from the end.
    ///
    /// If we fail here it means that the query was incorrect and we should return an error.
    ///
    bool mergeElement(bool push_to_result = true)
    {
        Operator cur_op;
        while (popOperator(cur_op))
        {
            ASTPtr function;

            // Special case of ternary operator
            if (cur_op.type == OperatorType::StartIf)
                return false;

            if (cur_op.type == OperatorType::FinishIf)
            {
                Operator tmp;
                if (!popOperator(tmp) || tmp.type != OperatorType::StartIf)
                    return false;
            }

            // Special case of a BETWEEN b AND c operator
            if (cur_op.type == OperatorType::StartBetween || cur_op.type == OperatorType::StartNotBetween)
                return false;

            if (cur_op.type == OperatorType::FinishBetween)
            {
                Operator tmp_op;
                if (!popOperator(tmp_op))
                    return false;

                if (tmp_op.type != OperatorType::StartBetween && tmp_op.type != OperatorType::StartNotBetween)
                    return false;

                bool negative = tmp_op.type == OperatorType::StartNotBetween;

                ASTs arguments;
                if (!popLastNOperands(arguments, 3))
                    return false;

                function = makeBetweenOperator(negative, arguments);
            }
            else
            {
                function = makeASTFunction(cur_op.function_name);

                if (!popLastNOperands(function->children[0]->children, cur_op.arity))
                    return false;
            }

            pushOperand(function);
        }

        ASTPtr node;
        if (!popOperand(node))
            return false;

        bool res = empty();

        if (push_to_result)
            pushResult(node);
        else
            pushOperand(node);

        return res;
    }

    bool parseLambda()
    {
        // 0. If empty - create function tuple with 0 args
        if (empty())
        {
            auto function = makeASTFunction("tuple");
            pushOperand(function);
            return true;
        }

        if (operands.size() != 1 || !operators.empty() || !mergeElement())
            return false;

        /// 1. If there is already tuple do nothing
        if (tryGetFunctionName(result.back()) == "tuple")
        {
            pushOperand(result.back());
            result.pop_back();
        }
        /// 2. Put all result in a single tuple
        else
        {
            auto function = makeASTFunction("tuple", result);
            result.clear();
            pushOperand(function);
        }
        return true;
    }

    /// Put 'node' identifier into the last operand as its alias
    bool insertAlias(ASTPtr node)
    {
        if (!mergeElement(false))
            return false;

        if (operands.empty())
            return false;

        if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(operands.back().get()))
            tryGetIdentifierNameInto(node, ast_with_alias->alias);
        else
            return false;

        return true;
    }

    void addBetween()
    {
        ++open_between;
    }

    void subBetween()
    {
        --open_between;
    }

    bool hasBetween() const
    {
        return open_between > 0;
    }

protected:
    std::vector<Operator> operators;
    ASTs operands;
    ASTs result;
    bool finished = false;
    int state = 0;

    /// 'AND' in operator '... BETWEEN ... AND ...'  mirrors logical operator 'AND'.
    ///  In order to distinguish them we keep a counter of BETWEENs without matching ANDs.
    int open_between = 0;

    // bool allow_alias = true;
    // bool allow_alias_without_as_keyword = true;
};


/// Basic layer for a function with certain separator and end tokens:
///  1. If we parse a separator we should merge current operands and operators
///     into one element and push in to 'result' vector.
///  2. If we parse an ending token, we should merge everything as in (1) and
///     also set 'finished' flag.
template <TokenType separator, TokenType end>
class BaseLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        if (ParserToken(separator).ignore(pos, expected))
        {
            action = Action::OPERAND;
            return mergeElement();
        }

        if (ParserToken(end).ignore(pos, expected))
        {
            action = Action::OPERATOR;

            if (!empty() || !result.empty())
                if (!mergeElement())
                    return false;

            finished = true;
        }

        return true;
    }
};

/// General function layer
class FunctionLayer : public Layer
{
public:
    explicit FunctionLayer(String function_name_) : function_name(function_name_)
    {
    }

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        ///   | 0 |      1      |     2    |
        ///  f(ALL ...)(ALL ...) FILTER ...
        ///
        /// 0. Parse ALL and DISTINCT qualifiers (-> 1)
        /// 1. Parse all the arguments and ending token (-> 2), possibly with parameters list (-> 1)
        /// 2. Create function, possibly parse FILTER and OVER window definitions (finished)

        if (state == 0)
        {
            state = 1;

            auto pos_after_bracket = pos;
            auto old_expected = expected;

            ParserKeyword all("ALL");
            ParserKeyword distinct("DISTINCT");

            if (all.ignore(pos, expected))
                has_all = true;

            if (distinct.ignore(pos, expected))
                has_distinct = true;

            if (!has_all && all.ignore(pos, expected))
                has_all = true;

            if (has_all && has_distinct)
                return false;

            if (has_all || has_distinct)
            {
                /// case f(ALL), f(ALL, x), f(DISTINCT), f(DISTINCT, x), ALL and DISTINCT should be treat as identifier
                if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
                {
                    pos = pos_after_bracket;
                    expected = old_expected;
                    has_all = false;
                    has_distinct = false;
                }
            }

            contents_begin = pos->begin;
        }

        if (state == 1)
        {
            if (ParserToken(TokenType::Comma).ignore(pos, expected))
            {
                action = Action::OPERAND;
                return mergeElement();
            }

            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                action = Action::OPERATOR;

                if (!empty() || !result.empty())
                    if (!mergeElement())
                        return false;

                contents_end = pos->begin;

                /** Check for a common error case - often due to the complexity of quoting command-line arguments,
                 *  an expression of the form toDate(2014-01-01) appears in the query instead of toDate('2014-01-01').
                 * If you do not report that the first option is an error, then the argument will be interpreted as 2014 - 01 - 01 - some number,
                 *  and the query silently returns an unexpected result.
                 */
                if (function_name == "toDate"
                    && contents_end - contents_begin == strlen("2014-01-01")
                    && contents_begin[0] >= '2' && contents_begin[0] <= '3'
                    && contents_begin[1] >= '0' && contents_begin[1] <= '9'
                    && contents_begin[2] >= '0' && contents_begin[2] <= '9'
                    && contents_begin[3] >= '0' && contents_begin[3] <= '9'
                    && contents_begin[4] == '-'
                    && contents_begin[5] >= '0' && contents_begin[5] <= '9'
                    && contents_begin[6] >= '0' && contents_begin[6] <= '9'
                    && contents_begin[7] == '-'
                    && contents_begin[8] >= '0' && contents_begin[8] <= '9'
                    && contents_begin[9] >= '0' && contents_begin[9] <= '9')
                {
                    std::string contents_str(contents_begin, contents_end - contents_begin);
                    throw Exception("Argument of function toDate is unquoted: toDate(" + contents_str + "), must be: toDate('" + contents_str + "')"
                        , ErrorCodes::SYNTAX_ERROR);
                }

                if (ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
                {
                    parameters = std::make_shared<ASTExpressionList>();
                    std::swap(parameters->children, result);
                    action = Action::OPERAND;

                    /// Parametric aggregate functions cannot have DISTINCT in parameters list.
                    if (has_distinct)
                        return false;

                    auto pos_after_bracket = pos;
                    auto old_expected = expected;

                    ParserKeyword all("ALL");
                    ParserKeyword distinct("DISTINCT");

                    if (all.ignore(pos, expected))
                        has_all = true;

                    if (distinct.ignore(pos, expected))
                        has_distinct = true;

                    if (!has_all && all.ignore(pos, expected))
                        has_all = true;

                    if (has_all && has_distinct)
                        return false;

                    if (has_all || has_distinct)
                    {
                        /// case f(ALL), f(ALL, x), f(DISTINCT), f(DISTINCT, x), ALL and DISTINCT should be treat as identifier
                        if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
                        {
                            pos = pos_after_bracket;
                            expected = old_expected;
                            has_distinct = false;
                        }
                    }
                }
                else
                {
                    state = 2;
                }
            }
        }

        if (state == 2)
        {
            if (has_distinct)
                function_name += "Distinct";

            auto function_node = makeASTFunction(function_name, std::move(result));

            if (parameters)
            {
                function_node->parameters = parameters;
                function_node->children.push_back(function_node->parameters);
            }

            ParserKeyword filter("FILTER");
            ParserKeyword over("OVER");

            if (filter.ignore(pos, expected))
            {
                // We are slightly breaking the parser interface by parsing the window
                // definition into an existing ASTFunction. Normally it would take a
                // reference to ASTPtr and assign it the new node. We only have a pointer
                // of a different type, hence this workaround with a temporary pointer.
                ASTPtr function_node_as_iast = function_node;

                // Recursion
                ParserFilterClause filter_parser;
                if (!filter_parser.parse(pos, function_node_as_iast, expected))
                    return false;
            }

            if (over.ignore(pos, expected))
            {
                function_node->is_window_function = true;

                ASTPtr function_node_as_iast = function_node;

                // Recursion
                ParserWindowReference window_reference;
                if (!window_reference.parse(pos, function_node_as_iast, expected))
                    return false;
            }

            result = {function_node};
            finished = true;
        }

        return true;
    }

private:
    bool has_all = false;
    bool has_distinct = false;

    const char * contents_begin;
    const char * contents_end;

    String function_name;
    ASTPtr parameters;
};

/// Layer for priority brackets and tuple function
class RoundBracketsLayer : public Layer
{
public:
    bool getResult(ASTPtr & op) override
    {
        // Round brackets can mean priority operator as well as function tuple()
        if (!is_tuple && result.size() == 1)
            op = std::move(result[0]);
        else
            op = makeASTFunction("tuple", std::move(result));

        return true;
    }

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        if (ParserToken(TokenType::Comma).ignore(pos, expected))
        {
            action = Action::OPERAND;
            is_tuple = true;
            if (!mergeElement())
                return false;
        }

        if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
        {
            action = Action::OPERATOR;

            if (!empty())
                if (!mergeElement())
                    return false;

            // Special case for (('a', 'b')) -> tuple(('a', 'b'))
            if (!is_tuple && result.size() == 1)
                if (auto * literal = result[0]->as<ASTLiteral>())
                    if (literal->value.getType() == Field::Types::Tuple)
                        is_tuple = true;

            finished = true;
        }

        return true;
    }
private:
    bool is_tuple = false;
};

/// Layer for array square brackets operator
class ArrayLayer : public BaseLayer<TokenType::Comma, TokenType::ClosingSquareBracket>
{
public:
    bool getResult(ASTPtr & op) override
    {
        op = makeASTFunction("array", std::move(result));
        return true;
    }

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        return BaseLayer::parse(pos, expected, action);
    }
};

/// Layer for arrayElement square brackets operator
///  This layer does not create a function, it is only needed to parse closing token
///  and return only one element.
class ArrayElementLayer : public BaseLayer<TokenType::Comma, TokenType::ClosingSquareBracket>
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        return BaseLayer::parse(pos, expected, action);
    }
};

class CastLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// CAST(x [AS alias1], T [AS alias2]) or CAST(x [AS alias1] AS T)
        ///
        /// 0. Parse all the cases (-> 1)
        /// 1. Parse closing token (finished)

        ParserKeyword as_keyword_parser("AS");
        ASTPtr alias;

        /// expr AS type
        if (state == 0)
        {
            ASTPtr type_node;

            if (as_keyword_parser.ignore(pos, expected))
            {
                auto old_pos = pos;

                if (ParserIdentifier().parse(pos, alias, expected) &&
                    as_keyword_parser.ignore(pos, expected) &&
                    ParserDataType().parse(pos, type_node, expected) &&
                    ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                {
                    if (!insertAlias(alias))
                        return false;

                    if (!mergeElement())
                        return false;

                    result = {createFunctionCast(result[0], type_node)};
                    finished = true;
                    return true;
                }

                pos = old_pos;

                if (ParserIdentifier().parse(pos, alias, expected) &&
                    ParserToken(TokenType::Comma).ignore(pos, expected))
                {
                    action = Action::OPERAND;
                    if (!insertAlias(alias))
                        return false;

                    if (!mergeElement())
                        return false;

                    state = 1;
                    return true;
                }

                pos = old_pos;

                if (ParserDataType().parse(pos, type_node, expected) &&
                    ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                {
                    if (!mergeElement())
                        return false;

                    result = {createFunctionCast(result[0], type_node)};
                    finished = true;
                    return true;
                }

                return false;
            }

            if (ParserToken(TokenType::Comma).ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!mergeElement())
                    return false;

                state = 1;
                return true;
            }
        }
        if (state == 1)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                result = {makeASTFunction("CAST", result[0], result[1])};
                finished = true;
                return true;
            }
        }

        return true;
    }
};

class ExtractLayer : public BaseLayer<TokenType::Comma, TokenType::ClosingRoundBracket>
{
public:
    bool getResult(ASTPtr & op) override
    {
        if (state == 2)
        {
            if (result.empty())
                return false;

            op = makeASTFunction(interval_kind.toNameOfFunctionExtractTimePart(), result[0]);
        }
        else
        {
            op = makeASTFunction("extract", std::move(result));
        }

        return true;
    }

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// extract(haystack, pattern) or EXTRACT(DAY FROM Date)
        ///
        /// 0. If we parse interval_kind and 'FROM' keyword (-> 2), otherwise (-> 1)
        /// 1. Basic parser
        /// 2. Parse closing bracket (finished)

        if (state == 0)
        {
            IParser::Pos begin = pos;
            ParserKeyword s_from("FROM");

            if (parseIntervalKind(pos, expected, interval_kind) && s_from.ignore(pos, expected))
            {
                state = 2;
                return true;
            }
            else
            {
                state = 1;
                pos = begin;
            }
        }

        if (state == 1)
        {
            return BaseLayer::parse(pos, expected, action);
        }

        if (state == 2)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                finished = true;
                return true;
            }
        }

        return true;
    }

private:
    IntervalKind interval_kind;
};

class SubstringLayer : public Layer
{
public:
    bool getResult(ASTPtr & op) override
    {
        op = makeASTFunction("substring", std::move(result));
        return true;
    }

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// Either SUBSTRING(expr FROM start [FOR length]) or SUBSTRING(expr, start, length)
        ///
        /// 0: Parse first separator: FROM or comma (-> 1)
        /// 1: Parse second separator: FOR or comma (-> 2)
        /// 1 or 2: Parse closing bracket (finished)

        if (state == 0)
        {
            if (ParserToken(TokenType::Comma).ignore(pos, expected) ||
                ParserKeyword("FROM").ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!mergeElement())
                    return false;

                state = 1;
            }
        }

        if (state == 1)
        {
            if (ParserToken(TokenType::Comma).ignore(pos, expected) ||
                ParserKeyword("FOR").ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!mergeElement())
                    return false;

                state = 2;
            }
        }

        if (state == 1 || state == 2)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                finished = true;
            }
        }

        return true;
    }
};

class PositionLayer : public Layer
{
public:
    bool getResult(ASTPtr & op) override
    {
        if (state == 2)
            std::swap(result[1], result[0]);

        op = makeASTFunction("position", std::move(result));
        return true;
    }

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// position(haystack, needle[, start_pos]) or position(needle IN haystack)
        ///
        /// 0: Parse separator: comma (-> 1) or IN (-> 2)
        /// 1: Parse second separator: comma
        /// 1 or 2: Parse closing bracket (finished)

        if (state == 0)
        {
            if (ParserToken(TokenType::Comma).ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!mergeElement())
                    return false;

                state = 1;
            }
            if (ParserKeyword("IN").ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!mergeElement())
                    return false;

                state = 2;
            }
        }

        if (state == 1)
        {
            if (ParserToken(TokenType::Comma).ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!mergeElement())
                    return false;
            }
        }

        if (state == 1 || state == 2)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                finished = true;
            }
        }

        return true;
    }
};


class ExistsLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & /*action*/) override
    {
        ASTPtr node;

        // Recursion
        if (!ParserSelectWithUnionQuery().parse(pos, node, expected))
            return false;

        if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            return false;

        auto subquery = std::make_shared<ASTSubquery>();
        subquery->children.push_back(node);
        result = {makeASTFunction("exists", subquery)};

        finished = true;

        return true;
    }
};

class TrimLayer : public Layer
{
public:
    TrimLayer(bool trim_left_, bool trim_right_) : trim_left(trim_left_), trim_right(trim_right_)
    {
    }

    bool getResult(ASTPtr & op) override
    {
        op = makeASTFunction(function_name, std::move(result));
        return true;
    }

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// Handles all possible TRIM/LTRIM/RTRIM call variants
        ///
        /// 0: If flags 'trim_left' and 'trim_right' are set (-> 2).
        ///    If not, try to parse 'BOTH', 'LEADING', 'TRAILING' keywords,
        ///    then if char_override (-> 1), else (-> 2)
        /// 1. Parse 'FROM' keyword (-> 2)
        /// 2. Parse closing token, choose name, add arguments (finished)

        if (state == 0)
        {
            if (!trim_left && !trim_right)
            {
                if (ParserKeyword("BOTH").ignore(pos, expected))
                {
                    trim_left = true;
                    trim_right = true;
                    char_override = true;
                }
                else if (ParserKeyword("LEADING").ignore(pos, expected))
                {
                    trim_left = true;
                    char_override = true;
                }
                else if (ParserKeyword("TRAILING").ignore(pos, expected))
                {
                    trim_right = true;
                    char_override = true;
                }
                else
                {
                    trim_left = true;
                    trim_right = true;
                }

                if (char_override)
                    state = 1;
                else
                    state = 2;
            }
            else
            {
                state = 2;
            }
        }

        if (state == 1)
        {
            if (ParserKeyword("FROM").ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!mergeElement())
                    return false;

                to_remove = makeASTFunction("regexpQuoteMeta", result[0]);
                result.clear();
                state = 2;
            }
        }

        if (state == 2)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                ASTPtr pattern_node;

                if (char_override)
                {
                    auto pattern_func_node = std::make_shared<ASTFunction>();
                    auto pattern_list_args = std::make_shared<ASTExpressionList>();
                    if (trim_left && trim_right)
                    {
                        pattern_list_args->children = {
                            std::make_shared<ASTLiteral>("^["),
                            to_remove,
                            std::make_shared<ASTLiteral>("]+|["),
                            to_remove,
                            std::make_shared<ASTLiteral>("]+$")
                        };
                        function_name = "replaceRegexpAll";
                    }
                    else
                    {
                        if (trim_left)
                        {
                            pattern_list_args->children = {
                                std::make_shared<ASTLiteral>("^["),
                                to_remove,
                                std::make_shared<ASTLiteral>("]+")
                            };
                        }
                        else
                        {
                            /// trim_right == false not possible
                            pattern_list_args->children = {
                                std::make_shared<ASTLiteral>("["),
                                to_remove,
                                std::make_shared<ASTLiteral>("]+$")
                            };
                        }
                        function_name = "replaceRegexpOne";
                    }

                    pattern_func_node->name = "concat";
                    pattern_func_node->arguments = std::move(pattern_list_args);
                    pattern_func_node->children.push_back(pattern_func_node->arguments);

                    pattern_node = std::move(pattern_func_node);
                }
                else
                {
                    if (trim_left && trim_right)
                    {
                        function_name = "trimBoth";
                    }
                    else
                    {
                        if (trim_left)
                            function_name = "trimLeft";
                        else
                            function_name = "trimRight";
                    }
                }

                if (char_override)
                {
                    result.push_back(pattern_node);
                    result.push_back(std::make_shared<ASTLiteral>(""));
                }

                finished = true;
            }
        }

        return true;
    }
private:
    bool trim_left;
    bool trim_right;
    bool char_override = false;

    ASTPtr to_remove;
    String function_name;
};


class DateAddLayer : public BaseLayer<TokenType::Comma, TokenType::ClosingRoundBracket>
{
public:
    explicit DateAddLayer(const char * function_name_) : function_name(function_name_)
    {
    }

    bool getResult(ASTPtr & op) override
    {
        if (parsed_interval_kind)
        {
            result[0] = makeASTFunction(interval_kind.toNameOfFunctionToIntervalDataType(), result[0]);
            op = makeASTFunction(function_name, result[1], result[0]);
        }
        else
            op = makeASTFunction(function_name, std::move(result));

        return true;
    }


    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// DATEADD(YEAR, 1, date) or DATEADD(INTERVAL 1 YEAR, date);
        ///
        /// 0. Try to parse interval_kind (-> 1)
        /// 1. Basic parser

        if (state == 0)
        {
            if (parseIntervalKind(pos, expected, interval_kind))
            {
                if (!ParserToken(TokenType::Comma).ignore(pos, expected))
                    return false;

                action = Action::OPERAND;
                parsed_interval_kind = true;
            }

            state = 1;
        }

        if (state == 1)
        {
            return BaseLayer::parse(pos, expected, action);
        }

        return true;
    }

private:
    IntervalKind interval_kind;
    const char * function_name;
    bool parsed_interval_kind = false;
};


class DateDiffLayer : public BaseLayer<TokenType::Comma, TokenType::ClosingRoundBracket>
{
public:
    bool getResult(ASTPtr & op) override
    {
        if (parsed_interval_kind)
        {
            if (result.size() == 2)
                op = makeASTFunction("dateDiff", std::make_shared<ASTLiteral>(interval_kind.toDateDiffUnit()), result[0], result[1]);
            else if (result.size() == 3)
                op = makeASTFunction("dateDiff", std::make_shared<ASTLiteral>(interval_kind.toDateDiffUnit()), result[0], result[1], result[2]);
            else
                return false;
        }
        else
        {
            op = makeASTFunction("dateDiff", std::move(result));
        }
        return true;
    }

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// 0. Try to parse interval_kind (-> 1)
        /// 1. Basic parser

        if (state == 0)
        {
            if (parseIntervalKind(pos, expected, interval_kind))
            {
                parsed_interval_kind = true;

                if (!ParserToken(TokenType::Comma).ignore(pos, expected))
                    return false;
            }

            state = 1;
        }

        if (state == 1)
        {
            return BaseLayer::parse(pos, expected, action);
        }

        return true;
    }

private:
    IntervalKind interval_kind;
    bool parsed_interval_kind = false;
};


class IntervalLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & /*action*/) override
    {
        /// INTERVAL 1 HOUR or INTERVAL expr HOUR
        ///
        /// 0. Try to parse interval_kind (-> 1)
        /// 1. Basic parser

        if (state == 0)
        {
            auto begin = pos;
            auto init_expected = expected;
            ASTPtr string_literal;
            //// A String literal followed INTERVAL keyword,
            /// the literal can be a part of an expression or
            /// include Number and INTERVAL TYPE at the same time
            if (ParserStringLiteral{}.parse(pos, string_literal, expected))
            {
                String literal;
                if (string_literal->as<ASTLiteral &>().value.tryGet(literal))
                {
                    Tokens tokens(literal.data(), literal.data() + literal.size());
                    IParser::Pos token_pos(tokens, 0);
                    Expected token_expected;
                    ASTPtr expr;

                    if (!ParserNumber{}.parse(token_pos, expr, token_expected))
                    {
                        return false;
                    }
                    else
                    {
                        /// case: INTERVAL '1' HOUR
                        /// back to begin
                        if (!token_pos.isValid())
                        {
                            pos = begin;
                            expected = init_expected;
                        }
                        else
                        {
                            /// case: INTERVAL '1 HOUR'
                            if (!parseIntervalKind(token_pos, token_expected, interval_kind))
                                return false;

                            result = {makeASTFunction(interval_kind.toNameOfFunctionToIntervalDataType(), expr)};
                            finished = true;
                            return true;
                        }
                    }
                }
            }
            state = 1;
            return true;
        }

        if (state == 1)
        {
            if (parseIntervalKind(pos, expected, interval_kind))
            {
                if (!mergeElement())
                    return false;

                result = {makeASTFunction(interval_kind.toNameOfFunctionToIntervalDataType(), result)};
                finished = true;
            }
        }

        return true;
    }

private:
    IntervalKind interval_kind;
};


class CaseLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// CASE [x] WHEN expr THEN expr [WHEN expr THEN expr [...]] [ELSE expr] END
        ///
        /// 0. Check if we have case expression [x] (-> 1)
        /// 1. Parse keywords: WHEN (-> 2), ELSE (-> 3), END (finished)
        /// 2. Parse THEN keyword (-> 1)
        /// 3. Parse END keyword (finished)

        if (state == 0)
        {
            auto old_pos = pos;
            has_case_expr = !ParserKeyword("WHEN").ignore(pos, expected);
            pos = old_pos;

            state = 1;
        }

        if (state == 1)
        {
            if (ParserKeyword("WHEN").ignore(pos, expected))
            {
                if ((has_case_expr || !result.empty()) && !mergeElement())
                    return false;

                action = Action::OPERAND;
                state = 2;
            }
            else if (ParserKeyword("ELSE").ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                action = Action::OPERAND;
                state = 3;
            }
            else if (ParserKeyword("END").ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                Field field_with_null;
                ASTLiteral null_literal(field_with_null);
                result.push_back(std::make_shared<ASTLiteral>(null_literal));

                if (has_case_expr)
                    result = {makeASTFunction("caseWithExpression", result)};
                else
                    result = {makeASTFunction("multiIf", result)};
                finished = true;
            }
        }

        if (state == 2)
        {
            if (ParserKeyword("THEN").ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                action = Action::OPERAND;
                state = 1;
            }
        }

        if (state == 3)
        {
            if (ParserKeyword("END").ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                if (has_case_expr)
                    result = {makeASTFunction("caseWithExpression", result)};
                else
                    result = {makeASTFunction("multiIf", result)};

                finished = true;
            }
        }

        return true;
    }

private:
    bool has_case_expr;
};


bool ParseCastExpression(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    IParser::Pos begin = pos;

    if (ParserCastOperator().parse(pos, node, expected))
        return true;

    pos = begin;

    /// As an exception, negative numbers should be parsed as literals, and not as an application of the operator.
    if (pos->type == TokenType::Minus)
    {
        if (ParserLiteral().parse(pos, node, expected))
            return true;
    }
    return false;
}

bool ParseDateOperatorExpression(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;

    /// If no DATE keyword, go to the nested parser.
    if (!ParserKeyword("DATE").ignore(pos, expected))
        return false;

    ASTPtr expr;
    if (!ParserStringLiteral().parse(pos, expr, expected))
    {
        pos = begin;
        return false;
    }

    node = makeASTFunction("toDate", expr);
    return true;
}

bool ParseTimestampOperatorExpression(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;

    /// If no TIMESTAMP keyword, go to the nested parser.
    if (!ParserKeyword("TIMESTAMP").ignore(pos, expected))
        return false;

    ASTPtr expr;
    if (!ParserStringLiteral().parse(pos, expr, expected))
    {
        pos = begin;
        return false;
    }

    node = makeASTFunction("toDateTime", expr);

    return true;
}

bool ParserExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    static std::vector<std::pair<const char *, Operator>> op_table({
        {"+",             Operator("plus",            11)},
        {"-",             Operator("minus",           11)},
        {"*",             Operator("multiply",        12)},
        {"/",             Operator("divide",          12)},
        {"%",             Operator("modulo",          12)},
        {"MOD",           Operator("modulo",          12)},
        {"DIV",           Operator("intDiv",          12)},
        {"==",            Operator("equals",          9,  2, OperatorType::Comparison)},
        {"!=",            Operator("notEquals",       9,  2, OperatorType::Comparison)},
        {"<>",            Operator("notEquals",       9,  2, OperatorType::Comparison)},
        {"<=",            Operator("lessOrEquals",    9,  2, OperatorType::Comparison)},
        {">=",            Operator("greaterOrEquals", 9,  2, OperatorType::Comparison)},
        {"<",             Operator("less",            9,  2, OperatorType::Comparison)},
        {">",             Operator("greater",         9,  2, OperatorType::Comparison)},
        {"=",             Operator("equals",          9,  2, OperatorType::Comparison)},
        {"AND",           Operator("and",             4,  2, OperatorType::Mergeable)},
        {"OR",            Operator("or",              3,  2, OperatorType::Mergeable)},
        {"||",            Operator("concat",          10, 2, OperatorType::Mergeable)},
        {".",             Operator("tupleElement",    14, 2, OperatorType::TupleElement)},
        {"IS NULL",       Operator("isNull",          8,  1, OperatorType::IsNull)},
        {"IS NOT NULL",   Operator("isNotNull",       8,  1, OperatorType::IsNull)},
        {"LIKE",          Operator("like",            9)},
        {"ILIKE",         Operator("ilike",           9)},
        {"NOT LIKE",      Operator("notLike",         9)},
        {"NOT ILIKE",     Operator("notILike",        9)},
        {"IN",            Operator("in",              9)},
        {"NOT IN",        Operator("notIn",           9)},
        {"GLOBAL IN",     Operator("globalIn",        9)},
        {"GLOBAL NOT IN", Operator("globalNotIn",     9)},
        {"?",             Operator("",                2,  0, OperatorType::StartIf)},
        {":",             Operator("if",              3,  3, OperatorType::FinishIf)},
        {"BETWEEN",       Operator("",                6,  0, OperatorType::StartBetween)},
        {"NOT BETWEEN",   Operator("",                6,  0, OperatorType::StartNotBetween)},
        {"[",             Operator("arrayElement",    14, 2, OperatorType::ArrayElement)},
        {"::",            Operator("CAST",            14, 2, OperatorType::Cast)},
        {"->",            Operator("lambda",          1,  2, OperatorType::Lambda)}
    });

    static std::vector<std::pair<const char *, Operator>> op_table_unary({
        {"NOT",           Operator("not",             5,  1)},
        {"-",             Operator("negate",          13, 1)}
    });

    auto finish_between_operator = Operator("",       7, 0, OperatorType::FinishBetween);

    ParserCompoundIdentifier identifier_parser(false, true);
    ParserNumber number_parser;
    ParserAsterisk asterisk_parser;
    ParserLiteral literal_parser;
    ParserTupleOfLiterals tuple_literal_parser;
    ParserArrayOfLiterals array_literal_parser;
    ParserSubstitution substitution_parser;
    ParserMySQLGlobalVariable mysql_global_variable_parser;

    ParserKeyword any_parser("ANY");
    ParserKeyword all_parser("ALL");

    // Recursion
    ParserQualifiedAsterisk qualified_asterisk_parser;
    ParserColumnsMatcher columns_matcher_parser;
    ParserSubquery subquery_parser;

    Action next = Action::OPERAND;

    std::vector<std::unique_ptr<Layer>> layers;
    layers.push_back(std::make_unique<Layer>());

    while (pos.isValid())
    {
        if (!layers.back()->parse(pos, expected, next))
            return false;

        if (layers.back()->isFinished())
        {
            next = Action::OPERATOR;

            ASTPtr res;
            if (!layers.back()->getResult(res))
                return false;

            layers.pop_back();
            layers.back()->pushOperand(res);
            continue;
        }

        if (next == Action::OPERAND)
        {
            next = Action::OPERATOR;
            ASTPtr tmp;

            /// Special case for cast expression
            if (layers.back()->previousType() != OperatorType::TupleElement &&
                ParseCastExpression(pos, tmp, expected))
            {
                layers.back()->pushOperand(std::move(tmp));
                continue;
            }

            if (layers.back()->previousType() == OperatorType::Comparison)
            {
                SubqueryFunctionType subquery_function_type = SubqueryFunctionType::NONE;

                if (any_parser.ignore(pos, expected) && subquery_parser.parse(pos, tmp, expected))
                    subquery_function_type = SubqueryFunctionType::ANY;
                else if (all_parser.ignore(pos, expected) && subquery_parser.parse(pos, tmp, expected))
                    subquery_function_type = SubqueryFunctionType::ALL;

                if (subquery_function_type != SubqueryFunctionType::NONE)
                {
                    Operator prev_op;
                    ASTPtr function, argument;

                    if (!layers.back()->popOperator(prev_op))
                        return false;
                    if (!layers.back()->popOperand(argument))
                        return false;

                    function = makeASTFunction(prev_op.function_name, argument, tmp);

                    if (!modifyAST(function, subquery_function_type))
                        return false;

                    layers.back()->pushOperand(std::move(function));
                    continue;
                }
            }

            /// Try to find any unary operators
            auto cur_op = op_table_unary.begin();
            for (; cur_op != op_table_unary.end(); ++cur_op)
            {
                if (parseOperator(pos, cur_op->first, expected))
                    break;
            }

            if (cur_op != op_table_unary.end())
            {
                next = Action::OPERAND;
                layers.back()->pushOperator(cur_op->second);
                continue;
            }

            auto old_pos = pos;
            std::unique_ptr<Layer> layer;
            if (parseOperator(pos, "INTERVAL", expected))
                layer = std::make_unique<IntervalLayer>();
            else if (parseOperator(pos, "CASE", expected))
                layer = std::make_unique<CaseLayer>();

            /// Here we check that CASE or INTERVAL is not an identifier
            /// It is needed for backwards compatibility
            if (layer)
            {
                Expected stub;

                auto stub_cur_op = op_table.begin();
                for (; stub_cur_op != op_table.end(); ++stub_cur_op)
                {
                    if (parseOperator(pos, stub_cur_op->first, stub))
                        break;
                }

                auto check_pos = pos;

                if (stub_cur_op != op_table.end() ||
                    ParserToken(TokenType::Comma).ignore(pos, stub) ||
                    ParserToken(TokenType::ClosingRoundBracket).ignore(pos, stub) ||
                    ParserToken(TokenType::ClosingSquareBracket).ignore(pos, stub) ||
                    ParserToken(TokenType::Semicolon).ignore(pos, stub) ||
                    ParserKeyword("AS").ignore(pos, stub) ||
                    ParserKeyword("FROM").ignore(pos, stub) ||
                    !pos.isValid())
                {
                    pos = old_pos;
                }
                else if (ParserAlias(true).ignore(check_pos, stub) &&
                         (ParserToken(TokenType::Comma).ignore(check_pos, stub) ||
                          ParserToken(TokenType::ClosingRoundBracket).ignore(check_pos, stub) ||
                          ParserToken(TokenType::ClosingSquareBracket).ignore(check_pos, stub) ||
                          ParserToken(TokenType::Semicolon).ignore(check_pos, stub) ||
                          ParserKeyword("FROM").ignore(check_pos, stub) ||
                          !check_pos.isValid()))
                {
                    pos = old_pos;
                }
                else
                {
                    next = Action::OPERAND;
                    layers.push_back(std::move(layer));
                    continue;
                }
            }

            if (ParseDateOperatorExpression(pos, tmp, expected) ||
                ParseTimestampOperatorExpression(pos, tmp, expected) ||
                tuple_literal_parser.parse(pos, tmp, expected) ||
                array_literal_parser.parse(pos, tmp, expected) ||
                number_parser.parse(pos, tmp, expected) ||
                literal_parser.parse(pos, tmp, expected) ||
                asterisk_parser.parse(pos, tmp, expected) ||
                qualified_asterisk_parser.parse(pos, tmp, expected) ||
                columns_matcher_parser.parse(pos, tmp, expected))
            {
                layers.back()->pushOperand(std::move(tmp));
            }
            else if (identifier_parser.parse(pos, tmp, expected))
            {
                if (pos->type == TokenType::OpeningRoundBracket)
                {
                    ++pos;

                    next = Action::OPERAND;

                    String function_name = getIdentifierName(tmp);
                    String function_name_lowercase = Poco::toLower(function_name);

                    if (function_name_lowercase == "cast")
                        layers.push_back(std::make_unique<CastLayer>());
                    else if (function_name_lowercase == "extract")
                        layers.push_back(std::make_unique<ExtractLayer>());
                    else if (function_name_lowercase == "substring")
                        layers.push_back(std::make_unique<SubstringLayer>());
                    else if (function_name_lowercase == "position")
                        layers.push_back(std::make_unique<PositionLayer>());
                    else if (function_name_lowercase == "exists")
                        layers.push_back(std::make_unique<ExistsLayer>());
                    else if (function_name_lowercase == "trim")
                        layers.push_back(std::make_unique<TrimLayer>(false, false));
                    else if (function_name_lowercase == "ltrim")
                        layers.push_back(std::make_unique<TrimLayer>(true, false));
                    else if (function_name_lowercase == "rtrim")
                        layers.push_back(std::make_unique<TrimLayer>(false, true));
                    else if (function_name_lowercase == "dateadd" || function_name_lowercase == "date_add"
                        || function_name_lowercase == "timestampadd" || function_name_lowercase == "timestamp_add")
                        layers.push_back(std::make_unique<DateAddLayer>("plus"));
                    else if (function_name_lowercase == "datesub" || function_name_lowercase == "date_sub"
                        || function_name_lowercase == "timestampsub" || function_name_lowercase == "timestamp_sub")
                        layers.push_back(std::make_unique<DateAddLayer>("minus"));
                    else if (function_name_lowercase == "datediff" || function_name_lowercase == "date_diff"
                        || function_name_lowercase == "timestampdiff" || function_name_lowercase == "timestamp_diff")
                        layers.push_back(std::make_unique<DateDiffLayer>());
                    else if (function_name_lowercase == "grouping")
                        layers.push_back(std::make_unique<FunctionLayer>(function_name_lowercase));
                    else
                        layers.push_back(std::make_unique<FunctionLayer>(function_name));
                }
                else
                {
                    layers.back()->pushOperand(std::move(tmp));
                }
            }
            else if (substitution_parser.parse(pos, tmp, expected))
            {
                layers.back()->pushOperand(std::move(tmp));
            }
            else if (pos->type == TokenType::OpeningRoundBracket)
            {
                if (subquery_parser.parse(pos, tmp, expected))
                {
                    layers.back()->pushOperand(std::move(tmp));
                    continue;
                }
                next = Action::OPERAND;
                layers.push_back(std::make_unique<RoundBracketsLayer>());
                ++pos;
            }
            else if (pos->type == TokenType::OpeningSquareBracket)
            {
                ++pos;

                next = Action::OPERAND;
                layers.push_back(std::make_unique<ArrayLayer>());
            }
            else if (mysql_global_variable_parser.parse(pos, tmp, expected))
            {
                layers.back()->pushOperand(std::move(tmp));
            }
            else
            {
                break;
            }
        }
        else
        {
            next = Action::OPERAND;
            ASTPtr tmp;

            /// ParserExpression can be called in this part of the query:
            ///  ALTER TABLE partition_all2 CLEAR INDEX [ p ] IN PARTITION ALL
            ///
            /// 'IN PARTITION' here is not an 'IN' operator, so we should stop parsing immediately
            Expected stub;
            if (ParserKeyword("IN PARTITION").checkWithoutMoving(pos, stub))
                break;

            /// Try to find operators from 'op_table'
            auto cur_op = op_table.begin();
            for (; cur_op != op_table.end(); ++cur_op)
            {
                if (parseOperator(pos, cur_op->first, expected))
                    break;
            }

            if (cur_op != op_table.end())
            {
                auto op = cur_op->second;

                if (op.type == OperatorType::Lambda)
                {
                    if (!layers.back()->parseLambda())
                        return false;

                    layers.back()->pushOperator(op);
                    continue;
                }

                // 'AND' can be both boolean function and part of the '... BETWEEN ... AND ...' operator
                if (op.function_name == "and" && layers.back()->hasBetween())
                {
                    layers.back()->subBetween();
                    op = finish_between_operator;
                }

                while (layers.back()->previousPriority() >= op.priority)
                {
                    ASTPtr function;
                    Operator prev_op;
                    layers.back()->popOperator(prev_op);

                    /// Mergeable operators are operators that are merged into one function:
                    /// For example: 'a OR b OR c' -> 'or(a, b, c)' and not 'or(or(a,b), c)'
                    if (prev_op.type == OperatorType::Mergeable && op.function_name == prev_op.function_name)
                    {
                        op.arity += prev_op.arity - 1;
                        break;
                    }

                    if (prev_op.type == OperatorType::FinishBetween)
                    {
                        Operator tmp_op;
                        if (!layers.back()->popOperator(tmp_op))
                            return false;

                        if (tmp_op.type != OperatorType::StartBetween && tmp_op.type != OperatorType::StartNotBetween)
                            return false;

                        bool negative = tmp_op.type == OperatorType::StartNotBetween;

                        ASTs arguments;
                        if (!layers.back()->popLastNOperands(arguments, 3))
                            return false;

                        function = makeBetweenOperator(negative, arguments);
                    }
                    else
                    {
                        function = makeASTFunction(prev_op.function_name);

                        if (!layers.back()->popLastNOperands(function->children[0]->children, prev_op.arity))
                            return false;
                    }

                    layers.back()->pushOperand(function);
                }
                layers.back()->pushOperator(op);

                if (op.type == OperatorType::ArrayElement)
                    layers.push_back(std::make_unique<ArrayElementLayer>());

                // isNull & isNotNull is postfix unary operator
                if (op.type == OperatorType::IsNull)
                    next = Action::OPERATOR;

                if (op.type == OperatorType::StartBetween || op.type == OperatorType::StartNotBetween)
                    layers.back()->addBetween();

                if (op.type == OperatorType::Cast)
                {
                    next = Action::OPERATOR;

                    ASTPtr type_ast;
                    if (!ParserDataType().parse(pos, type_ast, expected))
                        return false;

                    layers.back()->pushOperand(std::make_shared<ASTLiteral>(queryToString(type_ast)));
                }
            }
            else if (layers.size() > 1 && ParserAlias(true).parse(pos, tmp, expected))
            {
                if (!layers.back()->insertAlias(tmp))
                    return false;
            }
            else if (pos->type == TokenType::Comma)
            {
                if (layers.size() == 1)
                    break;
            }
            else
            {
                break;
            }
        }
    }

    // When we exit the loop we should be on the 1st level
    if (layers.size() > 1)
        return false;

    if (!layers.back()->mergeElement())
        return false;

    if (!layers.back()->getResult(node))
        return false;

    return true;
}

}
