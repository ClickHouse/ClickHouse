#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>


namespace DB
{


const char * ParserMultiplicativeExpression::operators[] =
{
    "*",     "multiply",
    "/",     "divide",
    "%",     "modulo",
    nullptr
};

const char * ParserUnaryMinusExpression::operators[] =
{
    "-",     "negate",
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
    "==",         "equals",
    "!=",         "notEquals",
    "<>",         "notEquals",
    "<=",         "lessOrEquals",
    ">=",         "greaterOrEquals",
    "<",         "less",
    ">",         "greater",
    "=",         "equals",
    "LIKE",     "like",
    "NOT LIKE",    "notLike",
    "IN",        "in",
    "NOT IN",    "notIn",
    "GLOBAL IN",    "globalIn",
    "GLOBAL NOT IN","globalNotIn",
    nullptr
};

const char * ParserLogicalNotExpression::operators[] =
{
    "NOT", "not",
    nullptr
};

const char * ParserArrayElementExpression::operators[] =
{
    "[",     "arrayElement",
    nullptr
};

const char * ParserTupleElementExpression::operators[] =
{
    ".",     "tupleElement",
    nullptr
};



bool ParserList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    bool first = true;
    ParserWhiteSpaceOrComments ws;

    auto list = std::make_shared<ASTExpressionList>();
    node = list;

    while (1)
    {
        if (first)
        {
            ASTPtr elem;
            if (!elem_parser->parse(pos, end, elem, max_parsed_pos, expected))
                break;

            list->children.push_back(elem);
            first = false;
        }
        else
        {
            auto prev_pos = pos;

            ws.ignore(pos, end);
            if (!separator_parser->ignore(pos, end, max_parsed_pos, expected))
                break;
            ws.ignore(pos, end);

            ASTPtr elem;
            if (!elem_parser->parse(pos, end, elem, max_parsed_pos, expected))
            {
                pos = prev_pos;
                break;
            }

            list->children.push_back(elem);
        }
    }

    if (!allow_empty && first)
        return false;

    return true;
}


bool ParserLeftAssociativeBinaryOperatorList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    bool first = true;
    ParserWhiteSpaceOrComments ws;
    Pos begin = pos;

    while (1)
    {
        if (first)
        {
            ASTPtr elem;
            if (!first_elem_parser->parse(pos, end, elem, max_parsed_pos, expected))
                return false;

            node = elem;
            first = false;
        }
        else
        {
            ws.ignore(pos, end);

            /// try to find any of the valid operators

            const char ** it;
            for (it = operators; *it; it += 2)
            {
                ParserString op(it[0], true, true);
                if (op.ignore(pos, end, max_parsed_pos, expected))
                    break;
            }

            if (!*it)
                break;

            ws.ignore(pos, end);

            /// the function corresponding to the operator
            auto function = std::make_shared<ASTFunction>();

            /// function arguments
            auto exp_list = std::make_shared<ASTExpressionList>();

            ASTPtr elem;
            if (!(remaining_elem_parser ? remaining_elem_parser : first_elem_parser)->parse(pos, end, elem, max_parsed_pos, expected))
                return false;

            /// the first argument of the function is the previous element, the second is the next one
            function->range.first = begin;
            function->range.second = pos;
            function->name = it[1];
            function->arguments = exp_list;
            function->children.push_back(exp_list);

            exp_list->children.push_back(node);
            exp_list->children.push_back(elem);
            exp_list->range.first = begin;
            exp_list->range.second = pos;

            /** special exception for the access operator to the element of the array `x[y]`, which
                * contains the infix part '[' and the suffix ''] '(specified as' [')
                */
            if (0 == strcmp(it[0], "["))
            {
                ParserString rest_p("]");

                ws.ignore(pos, end);
                if (!rest_p.ignore(pos, end, max_parsed_pos, expected))
                    return false;
            }

            node = function;
        }
    }

    return true;
}

bool ParserVariableArityOperatorList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;

    Pos begin = pos;
    ASTPtr arguments;

    if (!elem_parser->parse(pos, end, node, max_parsed_pos, expected))
        return false;

    while (true)
    {
        ws.ignore(pos, end);

        if (!infix_parser.ignore(pos, end, max_parsed_pos, expected))
            break;

        ws.ignore(pos, end);

        if (!arguments)
        {
            node = makeASTFunction(function_name, node);
            arguments = static_cast<ASTFunction &>(*node).arguments;
        }

        ASTPtr elem;
        if (!elem_parser->parse(pos, end, elem, max_parsed_pos, expected))
            return false;

        arguments->children.push_back(elem);
    }

    if (arguments)
        arguments->range = node->range = StringRange(begin, pos);

    return true;
}

bool ParserBetweenExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    /// For the expression (subject BETWEEN left AND right)
    ///  create an AST the same as for (subject> = left AND subject <= right).

    ParserWhiteSpaceOrComments ws;
    ParserString s_between("BETWEEN", true, true);
    ParserString s_and("AND", true, true);

    ASTPtr subject;
    ASTPtr left;
    ASTPtr right;

    Pos begin = pos;

    if (!elem_parser.parse(pos, end, subject, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (!s_between.ignore(pos, end, max_parsed_pos, expected))
        node = subject;
    else
    {
        ws.ignore(pos, end);

        if (!elem_parser.parse(pos, end, left, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (!s_and.ignore(pos, end, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (!elem_parser.parse(pos, end, right, max_parsed_pos, expected))
            return false;

        /// AND function
        auto f_and = std::make_shared<ASTFunction>();
        auto args_and = std::make_shared<ASTExpressionList>();

        /// >=
        auto f_ge = std::make_shared<ASTFunction>();
        auto args_ge = std::make_shared<ASTExpressionList>();

        /// <=
        auto f_le = std::make_shared<ASTFunction>();
        auto args_le = std::make_shared<ASTExpressionList>();

        args_ge->children.emplace_back(subject);
        args_ge->children.emplace_back(left);

        args_le->children.emplace_back(subject);
        args_le->children.emplace_back(right);

        f_ge->range.first = begin;
        f_ge->range.second = pos;
        f_ge->name = "greaterOrEquals";
        f_ge->arguments = args_ge;
        f_ge->children.emplace_back(f_ge->arguments);

        f_le->range.first = begin;
        f_le->range.second = pos;
        f_le->name = "lessOrEquals";
        f_le->arguments = args_le;
        f_le->children.emplace_back(f_le->arguments);

        args_and->children.emplace_back(f_ge);
        args_and->children.emplace_back(f_le);

        f_and->range.first = begin;
        f_and->range.second = pos;
        f_and->name = "and";
        f_and->arguments = args_and;
        f_and->children.emplace_back(f_and->arguments);

        node = f_and;
    }

    return true;
}

bool ParserTernaryOperatorExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;
    ParserString symbol1("?");
    ParserString symbol2(":");

    ASTPtr elem_cond;
    ASTPtr elem_then;
    ASTPtr elem_else;

    Pos begin = pos;

    if (!elem_parser.parse(pos, end, elem_cond, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (!symbol1.ignore(pos, end, max_parsed_pos, expected))
        node = elem_cond;
    else
    {
        ws.ignore(pos, end);

        if (!elem_parser.parse(pos, end, elem_then, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (!symbol2.ignore(pos, end, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (!elem_parser.parse(pos, end, elem_else, max_parsed_pos, expected))
            return false;

        /// the function corresponding to the operator
        auto function = std::make_shared<ASTFunction>();

        /// function arguments
        auto exp_list = std::make_shared<ASTExpressionList>();

        function->range.first = begin;
        function->range.second = pos;
        function->name = "if";
        function->arguments = exp_list;
        function->children.push_back(exp_list);

        exp_list->children.push_back(elem_cond);
        exp_list->children.push_back(elem_then);
        exp_list->children.push_back(elem_else);
        exp_list->range.first = begin;
        exp_list->range.second = pos;

        node = function;
    }

    return true;
}


bool ParserLambdaExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;
    ParserString arrow("->");
    ParserString open("(");
    ParserString close(")");

    Pos begin = pos;

    do
    {
        ASTPtr inner_arguments;
        ASTPtr expression;

        bool was_open = false;

        if (open.ignore(pos, end, max_parsed_pos, expected))
        {
            ws.ignore(pos, end, max_parsed_pos, expected);
            was_open = true;
        }

        if (!ParserList(std::make_unique<ParserIdentifier>(), std::make_unique<ParserString>(",")).parse(pos, end, inner_arguments, max_parsed_pos, expected))
            break;
        ws.ignore(pos, end, max_parsed_pos, expected);

        if (was_open)
        {
            if (!close.ignore(pos, end, max_parsed_pos, expected))
                break;
            ws.ignore(pos, end, max_parsed_pos, expected);
        }

        if (!arrow.ignore(pos, end, max_parsed_pos, expected))
            break;
        ws.ignore(pos, end, max_parsed_pos, expected);

        if (!elem_parser.parse(pos, end, expression, max_parsed_pos, expected))
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
    return elem_parser.parse(pos, end, node, max_parsed_pos, expected);
}


bool ParserPrefixUnaryOperatorExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;

    /// try to find any of the valid operators
    Pos begin = pos;
    const char ** it;
    for (it = operators; *it; it += 2)
    {
        ParserString op(it[0], true, true);
        if (op.ignore(pos, end, max_parsed_pos, expected))
            break;
    }

    ws.ignore(pos, end);

    /// Let's parse chains of the form `NOT NOT x`. This is hack.
    /** This is done, because among the unary operators there is only a minus and NOT.
      * But for a minus the chain of unary operators does not need to be supported.
      */
    if (it[0] && 0 == strncmp(it[0], "NOT", 3))
    {
        /// Was there an even number of NOTs.
        bool even = false;

        const char ** jt;
        while (true)
        {
            for (jt = operators; *jt; jt += 2)
            {
                ParserString op(jt[0], true, true);
                if (op.ignore(pos, end, max_parsed_pos, expected))
                    break;
            }

            if (!*jt)
                break;

            even = !even;

            ws.ignore(pos, end);
        }

        if (even)
            it = jt;    /// Zero the result of parsing the first NOT. It turns out, as if there is no `NOT` chain at all.
    }

    ASTPtr elem;
    if (!elem_parser->parse(pos, end, elem, max_parsed_pos, expected))
        return false;

    if (!*it)
        node = elem;
    else
    {
        /// the function corresponding to the operator
        auto function = std::make_shared<ASTFunction>();

        /// function arguments
        auto exp_list = std::make_shared<ASTExpressionList>();

        function->range.first = begin;
        function->range.second = pos;
        function->name = it[1];
        function->arguments = exp_list;
        function->children.push_back(exp_list);

        exp_list->children.push_back(elem);
        exp_list->range.first = begin;
        exp_list->range.second = pos;

        node = function;
    }

    return true;
}


bool ParserUnaryMinusExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    /// As an exception, negative numbers should be parsed as literals, and not as an application of the operator.

    if (pos < end && *pos == '-')
    {
        ParserLiteral lit_p;
        Pos begin = pos;

        if (lit_p.parse(pos, end, node, max_parsed_pos, expected))
            return true;

        pos = begin;
    }

    return operator_parser.parse(pos, end, node, max_parsed_pos, expected);
}


bool ParserArrayElementExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected &expected)
{
    return ParserLeftAssociativeBinaryOperatorList{
        operators,
        std::make_unique<ParserExpressionElement>(),
        std::make_unique<ParserExpressionWithOptionalAlias>(false)
    }.parse(pos, end, node, max_parsed_pos, expected);
}


bool ParserTupleElementExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected &expected)
{
    return ParserLeftAssociativeBinaryOperatorList{
        operators,
        std::make_unique<ParserArrayElementExpression>(),
        std::make_unique<ParserUnsignedInteger>()
    }.parse(pos, end, node, max_parsed_pos, expected);
}


ParserExpressionWithOptionalAlias::ParserExpressionWithOptionalAlias(bool allow_alias_without_as_keyword)
    : impl(std::make_unique<ParserWithOptionalAlias>(std::make_unique<ParserLambdaExpression>(), allow_alias_without_as_keyword))
{
}


ParserExpressionInCastExpression::ParserExpressionInCastExpression(bool allow_alias_without_as_keyword)
    : impl(std::make_unique<ParserCastExpressionWithOptionalAlias>(std::make_unique<ParserLambdaExpression>(), allow_alias_without_as_keyword))
{
}


bool ParserExpressionList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    return ParserList(
        std::make_unique<ParserExpressionWithOptionalAlias>(allow_alias_without_as_keyword),
        std::make_unique<ParserString>(","))
        .parse(pos, end, node, max_parsed_pos, expected);
}


bool ParserNotEmptyExpressionList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    return nested_parser.parse(pos, end, node, max_parsed_pos, expected)
        && !typeid_cast<ASTExpressionList &>(*node).children.empty();
}


bool ParserOrderByExpressionList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    return ParserList(std::make_unique<ParserOrderByElement>(), std::make_unique<ParserString>(","), false)
        .parse(pos, end, node, max_parsed_pos, expected);
}


bool ParserNullityChecking::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;

    ASTPtr node_comp;
    if (!ParserComparisonExpression{}.parse(pos, end, node_comp, max_parsed_pos, expected))
        return false;

    ParserString s_is{"IS", true, true};
    ParserString s_not{"NOT", true, true};
    ParserString s_null{"NULL", true, true};

    ws.ignore(pos, end);

    if (s_is.ignore(pos, end, max_parsed_pos, expected))
    {
        bool is_not = false;

        ws.ignore(pos, end);
        if (s_not.ignore(pos, end, max_parsed_pos, expected))
        {
            is_not = true;
            ws.ignore(pos, end);
        }
        if (!s_null.ignore(pos, end, max_parsed_pos, expected))
            return false;

        auto args = std::make_shared<ASTExpressionList>();
        args->children.push_back(node_comp);

        auto function = std::make_shared<ASTFunction>(StringRange{pos, end});
        function->name = is_not ? "isNotNull" : "isNull";
        function->arguments = args;
        function->children.push_back(function->arguments);

        node = function;
    }
    else
        node = node_comp;

    return true;
}


}
