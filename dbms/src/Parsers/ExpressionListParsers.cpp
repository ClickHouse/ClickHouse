#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>

#include <Common/StringUtils/StringUtils.h>


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
    "==",            "equals",
    "!=",            "notEquals",
    "<>",            "notEquals",
    "<=",            "lessOrEquals",
    ">=",            "greaterOrEquals",
    "<",             "less",
    ">",             "greater",
    "=",             "equals",
    "LIKE",          "like",
    "NOT LIKE",      "notLike",
    "IN",            "in",
    "NOT IN",        "notIn",
    "GLOBAL IN",     "globalIn",
    "GLOBAL NOT IN", "globalNotIn",
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
    bool first = true;

    auto list = std::make_shared<ASTExpressionList>();
    node = list;

    while (true)
    {
        if (first)
        {
            ASTPtr elem;
            if (!elem_parser->parse(pos, elem, expected))
                break;

            list->children.push_back(elem);
            first = false;
        }
        else
        {
            auto prev_pos = pos;

            if (!separator_parser->ignore(pos, expected))
                break;

            ASTPtr elem;
            if (!elem_parser->parse(pos, elem, expected))
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


bool ParserLeftAssociativeBinaryOperatorList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool first = true;
    Pos begin = pos;

    while (1)
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
            if (!(remaining_elem_parser ? remaining_elem_parser : first_elem_parser)->parse(pos, elem, expected))
                return false;

            /// the first argument of the function is the previous element, the second is the next one
            function->range.first = begin->begin;
            function->range.second = pos->begin;
            function->name = it[1];
            function->arguments = exp_list;
            function->children.push_back(exp_list);

            exp_list->children.push_back(node);
            exp_list->children.push_back(elem);
            exp_list->range.first = begin->begin;
            exp_list->range.second = pos->begin;

            /** special exception for the access operator to the element of the array `x[y]`, which
              * contains the infix part '[' and the suffix ''] '(specified as' [')
              */
            if (0 == strcmp(it[0], "["))
            {
                if (pos->type != TokenType::ClosingSquareBracket)
                    return false;
                ++pos;
            }

            node = function;
        }
    }

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
            arguments = static_cast<ASTFunction &>(*node).arguments;
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
    /// For the expression (subject BETWEEN left AND right)
    ///  create an AST the same as for (subject> = left AND subject <= right).

    ParserKeyword s_between("BETWEEN");
    ParserKeyword s_and("AND");

    ASTPtr subject;
    ASTPtr left;
    ASTPtr right;

    Pos begin = pos;

    if (!elem_parser.parse(pos, subject, expected))
        return false;

    if (!s_between.ignore(pos, expected))
        node = subject;
    else
    {
        if (!elem_parser.parse(pos, left, expected))
            return false;

        if (!s_and.ignore(pos, expected))
            return false;

        if (!elem_parser.parse(pos, right, expected))
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

        f_ge->range.first = begin->begin;
        f_ge->range.second = pos->begin;
        f_ge->name = "greaterOrEquals";
        f_ge->arguments = args_ge;
        f_ge->children.emplace_back(f_ge->arguments);

        f_le->range.first = begin->begin;
        f_le->range.second = pos->begin;
        f_le->name = "lessOrEquals";
        f_le->arguments = args_le;
        f_le->children.emplace_back(f_le->arguments);

        args_and->children.emplace_back(f_ge);
        args_and->children.emplace_back(f_le);

        f_and->range.first = begin->begin;
        f_and->range.second = pos->begin;
        f_and->name = "and";
        f_and->arguments = args_and;
        f_and->children.emplace_back(f_and->arguments);

        node = f_and;
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

    Pos begin = pos;

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

        function->range.first = begin->begin;
        function->range.second = pos->begin;
        function->name = "if";
        function->arguments = exp_list;
        function->children.push_back(exp_list);

        exp_list->children.push_back(elem_cond);
        exp_list->children.push_back(elem_then);
        exp_list->children.push_back(elem_else);
        exp_list->range.first = begin->begin;
        exp_list->range.second = pos->begin;

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


bool ParserPrefixUnaryOperatorExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// try to find any of the valid operators
    Pos begin = pos;
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
    if (it[0] && 0 == strncmp(it[0], "NOT", 3))
    {
        /// Was there an even number of NOTs.
        bool even = false;

        const char ** jt;
        while (true)
        {
            for (jt = operators; *jt; jt += 2)
                if (parseOperator(pos, *jt, expected))
                    break;

            if (!*jt)
                break;

            even = !even;
        }

        if (even)
            it = jt;    /// Zero the result of parsing the first NOT. It turns out, as if there is no `NOT` chain at all.
    }

    ASTPtr elem;
    if (!elem_parser->parse(pos, elem, expected))
        return false;

    if (!*it)
        node = elem;
    else
    {
        /// the function corresponding to the operator
        auto function = std::make_shared<ASTFunction>();

        /// function arguments
        auto exp_list = std::make_shared<ASTExpressionList>();

        function->range.first = begin->begin;
        function->range.second = pos->begin;
        function->name = it[1];
        function->arguments = exp_list;
        function->children.push_back(exp_list);

        exp_list->children.push_back(elem);
        exp_list->range.first = begin->begin;
        exp_list->range.second = pos->begin;

        node = function;
    }

    return true;
}


bool ParserUnaryMinusExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// As an exception, negative numbers should be parsed as literals, and not as an application of the operator.

    if (pos->type == TokenType::Minus)
    {
        ParserLiteral lit_p;
        Pos begin = pos;

        if (lit_p.parse(pos, node, expected))
            return true;

        pos = begin;
    }

    return operator_parser.parse(pos, node, expected);
}


bool ParserArrayElementExpression::parseImpl(Pos & pos, ASTPtr & node, Expected &expected)
{
    return ParserLeftAssociativeBinaryOperatorList{
        operators,
        std::make_unique<ParserExpressionElement>(),
        std::make_unique<ParserExpressionWithOptionalAlias>(false)
    }.parse(pos, node, expected);
}


bool ParserTupleElementExpression::parseImpl(Pos & pos, ASTPtr & node, Expected &expected)
{
    return ParserLeftAssociativeBinaryOperatorList{
        operators,
        std::make_unique<ParserArrayElementExpression>(),
        std::make_unique<ParserUnsignedInteger>()
    }.parse(pos, node, expected);
}


ParserExpressionWithOptionalAlias::ParserExpressionWithOptionalAlias(bool allow_alias_without_as_keyword, bool prefer_alias_to_column_name)
    : impl(std::make_unique<ParserWithOptionalAlias>(std::make_unique<ParserExpression>(),
                                                     allow_alias_without_as_keyword, prefer_alias_to_column_name))
{
}


ParserExpressionInCastExpression::ParserExpressionInCastExpression(bool allow_alias_without_as_keyword)
    : impl(std::make_unique<ParserCastExpressionWithOptionalAlias>(std::make_unique<ParserExpression>(),
                                                                   allow_alias_without_as_keyword, false))
{
}


bool ParserExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(
        std::make_unique<ParserExpressionWithOptionalAlias>(allow_alias_without_as_keyword, prefer_alias_to_column_name),
        std::make_unique<ParserToken>(TokenType::Comma))
        .parse(pos, node, expected);
}


bool ParserNotEmptyExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return nested_parser.parse(pos, node, expected)
        && !typeid_cast<ASTExpressionList &>(*node).children.empty();
}


bool ParserOrderByExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserOrderByElement>(), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}


bool ParserNullityChecking::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr node_comp;
    if (!ParserComparisonExpression{}.parse(pos, node_comp, expected))
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


bool ParserIntervalOperatorExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;

    /// If no INTERVAL keyword, go to nested parser.
    if (!ParserKeyword("INTERVAL").ignore(pos, expected))
        return next_parser.parse(pos, node, expected);

    ASTPtr expr;
    /// Any expression can be inside, because operator surrounds it.
    if (!ParserExpressionWithOptionalAlias(false).parse(pos, expr, expected))
        return false;

    const char * function_name = nullptr;

    if (ParserKeyword("SECOND").ignore(pos, expected))
        function_name = "toIntervalSecond";
    else if (ParserKeyword("MINUTE").ignore(pos, expected))
        function_name = "toIntervalMinute";
    else if (ParserKeyword("HOUR").ignore(pos, expected))
        function_name = "toIntervalHour";
    else if (ParserKeyword("DAY").ignore(pos, expected))
        function_name = "toIntervalDay";
    else if (ParserKeyword("WEEK").ignore(pos, expected))
        function_name = "toIntervalWeek";
    else if (ParserKeyword("MONTH").ignore(pos, expected))
        function_name = "toIntervalMonth";
    else if (ParserKeyword("YEAR").ignore(pos, expected))
        function_name = "toIntervalYear";
    else
        return false;

    /// the function corresponding to the operator
    auto function = std::make_shared<ASTFunction>();

    /// function arguments
    auto exp_list = std::make_shared<ASTExpressionList>();

    /// the first argument of the function is the previous element, the second is the next one
    function->range.first = begin->begin;
    function->range.second = pos->begin;
    function->name = function_name;
    function->arguments = exp_list;
    function->children.push_back(exp_list);

    exp_list->children.push_back(expr);
    exp_list->range.first = begin->begin;
    exp_list->range.second = pos->begin;

    node = function;
    return true;
}


}
