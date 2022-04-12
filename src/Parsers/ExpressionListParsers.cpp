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

#include <base/logger_useful.h>
#include <Parsers/queryToString.h>

using namespace std::literals;


namespace DB
{

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
            if (allow_any_all_operators && ParserKeyword("ANY").ignore(pos, expected))
                subquery_function_type = SubqueryFunctionType::ANY;
            else if (allow_any_all_operators && ParserKeyword("ALL").ignore(pos, expected))
                subquery_function_type = SubqueryFunctionType::ALL;
            else if (!(remaining_elem_parser ? remaining_elem_parser : first_elem_parser)->parse(pos, elem, expected))
                return false;

            if (subquery_function_type != SubqueryFunctionType::NONE && !ParserSubquery().parse(pos, elem, expected))
                return false;

            /// the first argument of the function is the previous element, the second is the next one
            function->name = it[1];
            function->arguments = exp_list;
            function->children.push_back(exp_list);

            exp_list->children.push_back(node);
            exp_list->children.push_back(elem);

            if (allow_any_all_operators && subquery_function_type != SubqueryFunctionType::NONE && !modifyAST(function, subquery_function_type))
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

////////////////////////////////////////////////////////////////////////////////////////
// class Operator:
//   - defines structure of certain operator
class Operator
{
public:
    Operator()
    {
    }

    Operator(String func_name_, Int32 priority_, Int32 arity_) : func_name(func_name_), priority(priority_), arity(arity_)
    {
    }

    String func_name;
    Int32 priority;
    Int32 arity;
};

enum Action
{
    OPERAND,
    OPERATOR
};

class Layer
{
public:
    Layer(TokenType end_bracket_ = TokenType::Whitespace, String func_name_ = "", bool layer_zero_ = false) :
        end_bracket(end_bracket_),
        func_name(func_name_),
        layer_zero(layer_zero_)
    {
    }

    virtual ~Layer() = default;

    bool popOperator(Operator & op)
    {
        if (operators.size() == 0)
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
        if (operands.size() == 0)
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

    bool getResult(ASTPtr & op)
    {
        ASTs res;
        std::swap(res, result);

        if (!func_name.empty())
        {
            // Round brackets can mean priority operator together with function tuple()
            if (func_name == "tuple" && res.size() == 1)
                op = std::move(res[0]);
            else
                op = makeASTFunction(func_name, std::move(res));

            return true;
        }

        if (res.size() == 1)
        {
            op = std::move(res[0]);
            return true;
        }

        return false;
    }

    virtual bool parse(IParser::Pos & pos, Expected & expected, Action & action)
    {
        if (!layer_zero && ParserToken(TokenType::Comma).ignore(pos, expected))
        {
            action = Action::OPERAND;
            return wrapLayer();
        }

        if (end_bracket != TokenType::Whitespace && ParserToken(end_bracket).ignore(pos, expected))
        {
            finished = true;
            return wrapLayer();
        }

        return true;
    }

    bool isFinished()
    {
        return finished;
    }

    int previousPriority()
    {
        if (operators.empty())
            return 0;

        return operators.back().priority;
    }

    int empty()
    {
        return operators.empty() && operands.empty();
    }

    bool lastNOperands(ASTs & asts, size_t n)
    {
        if (n > operands.size())
            return false;

        auto start = operands.begin() + operands.size() - n;
        asts.insert(asts.end(), std::make_move_iterator(start), std::make_move_iterator(operands.end()));
        operands.erase(start, operands.end());

        return true;
    }

    bool wrapLayer()
    {
        Operator cur_op;
        while (popOperator(cur_op))
        {
            auto func = makeASTFunction(cur_op.func_name);

            if (!lastNOperands(func->children[0]->children, cur_op.arity))
                return false;

            pushOperand(func);
        }

        ASTPtr res;
        if (!popOperand(res))
            return false;

        pushResult(res);

        return empty();
    }

protected:
    std::vector<Operator> operators;
    ASTs operands;
    ASTs result;
    TokenType end_bracket;
    String func_name;
    bool finished = false;
    bool layer_zero;
};


class CastLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// expr AS type
        if (state == 0)
        {
            if (ParserKeyword("AS").ignore(pos, expected))
            {
                if (!wrapLayer())
                    return false;

                ASTPtr type_node;

                if (ParserDataType().parse(pos, type_node, expected) && ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                {
                    result[0] = createFunctionCast(result[0], type_node);
                    finished = true;
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else if (ParserToken(TokenType::Comma).ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!wrapLayer())
                    return false;

                state = 1;
            }
        }
        if (state == 1)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!wrapLayer())
                    return false;

                result[0] = makeASTFunction("CAST", result[0], result[1]);
                result.pop_back();
                finished = true;
                return true;
            }
        }

        return true;
    }

private:
    int state = 0;
};

class ExtractLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
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
                func_name = "extract";
                end_bracket = TokenType::ClosingRoundBracket;
            }
        }

        if (state == 1)
        {
            return Layer::parse(pos, expected, action);
        }

        if (state == 2)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!wrapLayer())
                    return false;

                result[0] = makeASTFunction(interval_kind.toNameOfFunctionExtractTimePart(), result[0]);
                finished = true;
                return true;
            }
        }

        return true;
    }

private:
    int state = 0;
    IntervalKind interval_kind;
};

class SubstringLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// Either SUBSTRING(expr FROM start) or SUBSTRING(expr FROM start FOR length) or SUBSTRING(expr, start, length)
        /// The latter will be parsed normally as a function later.

        if (state == 0)
        {
            if (ParserToken(TokenType::Comma).ignore(pos, expected) ||
                ParserKeyword("FROM").ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!wrapLayer())
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

                if (!wrapLayer())
                    return false;

                state = 2;
            }
        }

        if (state == 1 || state == 2)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!wrapLayer())
                    return false;

                result = {makeASTFunction("substring", result)};
                finished = true;
                return true;
            }
        }

        return true;
    }

private:
    int state = 0;
};

class PositionLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        if (state == 0)
        {
            if (ParserToken(TokenType::Comma).ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!wrapLayer())
                    return false;

                state = 1;
            }
            if (ParserKeyword("IN").ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!wrapLayer())
                    return false;

                state = 2;
            }
        }

        if (state == 1 || 2)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!wrapLayer())
                    return false;

                if (state == 1)
                    result = {makeASTFunction("position", result)};
                else
                    result = {makeASTFunction("position", result[1], result[0])};

                finished = true;
                return true;
            }
        }

        return true;
    }

private:
    int state = 0;
};


class ExistsLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & /*action*/) override
    {
        ASTPtr node;

        // Recursion :'(
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

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// Handles all possible TRIM/LTRIM/RTRIM call variants

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

                if (!wrapLayer())
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
                if (!wrapLayer())
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
                        func_name = "replaceRegexpAll";
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
                        func_name = "replaceRegexpOne";
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
                        func_name = "trimBoth";
                    }
                    else
                    {
                        if (trim_left)
                        {
                            func_name = "trimLeft";
                        }
                        else
                        {
                            /// trim_right == false not possible
                            func_name = "trimRight";
                        }
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
    int state = 0;

    bool trim_left;
    bool trim_right;
    bool char_override = false;

    ASTPtr to_remove;
};


class DateAddLayer : public Layer
{
public:
    DateAddLayer(const char * function_name_) : function_name(function_name_)
    {
    }

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        if (state == 0)
        {
            if (parseIntervalKind(pos, expected, interval_kind))
            {
                if (!ParserToken(TokenType::Comma).ignore(pos, expected))
                    return false;

                action = Action::OPERAND;
                state = 2;
            }
            else
            {
                func_name = function_name;
                end_bracket = TokenType::ClosingRoundBracket;
                state = 1;
            }
        }

        if (state == 1)
        {
            return Layer::parse(pos, expected, action);
        }

        if (state == 2)
        {
            if (ParserToken(TokenType::Comma).ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!wrapLayer())
                    return false;

                state = 3;
            }
        }

        if (state == 3)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!wrapLayer())
                    return false;

                result[0] = makeASTFunction(interval_kind.toNameOfFunctionToIntervalDataType(), result[0]);
                result = {makeASTFunction(function_name, result[1], result[0])};
                finished = true;
            }
        }
        return true;
    }

private:
    int state = 0;
    IntervalKind interval_kind;
    const char * function_name;
};


class DateDiffLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        if (state == 0)
        {
            if (!parseIntervalKind(pos, expected, interval_kind))
            {
                func_name = "dateDiff";
                end_bracket = TokenType::ClosingRoundBracket;
                state = 1;
            }
            else
            {
                if (!ParserToken(TokenType::Comma).ignore(pos, expected))
                    return false;

                state = 2;
            }
        }

        if (state == 1)
        {
            return Layer::parse(pos, expected, action);
        }

        if (state == 2)
        {
            if (ParserToken(TokenType::Comma).ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!wrapLayer())
                    return false;

                state = 3;
            }
        }

        if (state == 3)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!wrapLayer())
                    return false;

                if (result.size() != 2)
                    return false;

                result = {makeASTFunction("dateDiff", std::make_shared<ASTLiteral>(interval_kind.toDateDiffUnit()), result[0], result[1])};
                finished = true;
            }
        }

        return true;
    }

private:
    int state = 0;
    IntervalKind interval_kind;
};


class IntervalLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & /*action*/) override
    {
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
                            if (!parseIntervalKind(token_pos, token_expected, interval_kind))
                                return false;

                            result = {makeASTFunction(interval_kind.toNameOfFunctionToIntervalDataType(), expr)};
                            finished = true;
                    }
                }
            }
            state = 1;
        }

        if (state == 1)
        {
            if (parseIntervalKind(pos, expected, interval_kind))
            {
                if (!wrapLayer())
                    return false;

                result = {makeASTFunction(interval_kind.toNameOfFunctionToIntervalDataType(), result)};
                finished = true;
            }
        }

        return true;
    }

private:
    int state = 0;
    IntervalKind interval_kind;
};


class CaseLayer : public Layer
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
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
                if ((has_case_expr || result.size() > 0) && !wrapLayer())
                    return false;

                action = Action::OPERAND;
                state = 2;
            }
            else if (ParserKeyword("ELSE").ignore(pos, expected))
            {
                if (!wrapLayer())
                    return false;

                action = Action::OPERAND;
                state = 3;
            }
            else if (ParserKeyword("END").ignore(pos, expected))
            {
                if (!wrapLayer())
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
                if (!wrapLayer())
                    return false;

                action = Action::OPERAND;
                state = 1;
            }
        }

        if (state == 3)
        {
            if (ParserKeyword("END").ignore(pos, expected))
            {
                if (!wrapLayer())
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
    int state = 0;
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

bool ParserExpression2::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    static std::vector<std::pair<const char *, Operator>> op_table({
        {"+",             Operator("plus", 20, 2)},            // Base arithmetic
        {"-",             Operator("minus", 20, 2)},
        {"*",             Operator("multiply", 30, 2)},
        {"/",             Operator("divide", 30, 2)},
        {"%",             Operator("modulo", 30, 2)},
        {"MOD",           Operator("modulo", 30, 2)},
        {"DIV",           Operator("intDiv", 30, 2)},
        {"==",            Operator("equals", 10, 2)},          // Base logic
        {"!=",            Operator("notEquals", 10, 2)},
        {"<>",            Operator("notEquals", 10, 2)},
        {"<=",            Operator("lessOrEquals", 10, 2)},
        {">=",            Operator("greaterOrEquals", 10, 2)},
        {"<",             Operator("less", 10, 2)},
        {">",             Operator("greater", 10, 2)},
        {"=",             Operator("equals", 10, 2)},
        {"AND",           Operator("and", 5, 2)},              // AND OR
        {"OR",            Operator("or", 4, 2)},
        {"||",            Operator("concat", 30, 2)},          // concat() func
        {".",             Operator("tupleElement", 40, 2)},    // tupleElement() func
        {"IS NULL",       Operator("isNull", 40, 1)},          // IS (NOT) NULL - correct priority ?
        {"IS NOT NULL",   Operator("isNotNull", 40, 1)},
        {"LIKE",          Operator("like", 10, 2)},            // LIKE funcs
        {"ILIKE",         Operator("ilike", 10, 2)},
        {"NOT LIKE",      Operator("notLike", 10, 2)},
        {"NOT ILIKE",     Operator("notILike", 10, 2)},
        {"IN",            Operator("in", 10, 2)},              // IN funcs
        {"NOT IN",        Operator("notIn", 10, 2)},
        {"GLOBAL IN",     Operator("globalIn", 10, 2)},
        {"GLOBAL NOT IN", Operator("globalNotIn", 10, 2)},
    });

    static std::vector<std::pair<const char *, Operator>> op_table_unary({
        {"-",    Operator("negate", 40, 1)},
        {"NOT",  Operator("not", 9, 1)}
    });

    ParserCompoundIdentifier identifier_parser;
    ParserNumber number_parser;
    ParserAsterisk asterisk_parser;
    ParserLiteral literal_parser;
    ParserTupleOfLiterals tuple_literal_parser;
    ParserArrayOfLiterals array_literal_parser;

    Action next = Action::OPERAND;

    std::vector<std::unique_ptr<Layer>> storage;
    storage.push_back(std::make_unique<Layer>(TokenType::Whitespace, "", true));

    while (pos.isValid())
    {
        // LOG_FATAL(&Poco::Logger::root(), "#pos: {}", String(pos->begin, pos->size()));
        if (!storage.back()->parse(pos, expected, next))
            return false;

        if (storage.back()->isFinished())
        {
            next = Action::OPERATOR;

            ASTPtr res;
            if (!storage.back()->getResult(res))
                return false;

            storage.pop_back();
            storage.back()->pushOperand(res);
            continue;
        }

        if (next == Action::OPERAND)
        {
            next = Action::OPERATOR;
            ASTPtr tmp;

            /// Special case for cast expression
            if (ParseCastExpression(pos, tmp, expected))
            {
                storage.back()->pushOperand(std::move(tmp));
                continue;
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
                storage.back()->pushOperator(cur_op->second);
            }
            else if (parseOperator(pos, "INTERVAL", expected))
            {
                next = Action::OPERAND;
                storage.push_back(std::make_unique<IntervalLayer>());
            }
            else if (parseOperator(pos, "CASE", expected))
            {
                next = Action::OPERAND; // ???
                storage.push_back(std::make_unique<CaseLayer>());
            }
            else if (ParseDateOperatorExpression(pos, tmp, expected) ||
                     ParseTimestampOperatorExpression(pos, tmp, expected) ||
                     tuple_literal_parser.parse(pos, tmp, expected) ||
                     array_literal_parser.parse(pos, tmp, expected) ||
                     number_parser.parse(pos, tmp, expected) ||
                     literal_parser.parse(pos, tmp, expected))
            {
                storage.back()->pushOperand(std::move(tmp));
            }
            else if (identifier_parser.parse(pos, tmp, expected) ||
                     asterisk_parser.parse(pos, tmp, expected))
            {
                /// If the next token is '(' then it is a plain function, '[' - arrayElement function
                if (pos->type == TokenType::OpeningRoundBracket)
                {
                    ++pos;

                    /// Special case for function with zero arguments: f()
                    if (pos->type == TokenType::ClosingRoundBracket)
                    {
                        ++pos;
                        auto function = makeASTFunction(getIdentifierName(tmp));
                        storage.back()->pushOperand(function);
                    }
                    else
                    {
                        next = Action::OPERAND;

                        String function_name = getIdentifierName(tmp);
                        String function_name_lowercase = Poco::toLower(function_name);

                        if (function_name_lowercase == "cast")
                            storage.push_back(std::make_unique<CastLayer>());
                        else if (function_name_lowercase == "extract")
                            storage.push_back(std::make_unique<ExtractLayer>());
                        else if (function_name_lowercase == "substring")
                            storage.push_back(std::make_unique<SubstringLayer>());
                        else if (function_name_lowercase == "position")
                            storage.push_back(std::make_unique<PositionLayer>());
                        else if (function_name_lowercase == "exists")
                            storage.push_back(std::make_unique<ExistsLayer>());
                        else if (function_name_lowercase == "trim")
                            storage.push_back(std::make_unique<TrimLayer>(false, false));
                        else if (function_name_lowercase == "ltrim")
                            storage.push_back(std::make_unique<TrimLayer>(true, false));
                        else if (function_name_lowercase == "rtrim")
                            storage.push_back(std::make_unique<TrimLayer>(false, true));
                        else if (function_name_lowercase == "dateadd" || function_name_lowercase == "date_add"
                            || function_name_lowercase == "timestampadd" || function_name_lowercase == "timestamp_add")
                            storage.push_back(std::make_unique<DateAddLayer>("plus"));
                        else if (function_name_lowercase == "datesub" || function_name_lowercase == "date_sub"
                            || function_name_lowercase == "timestampsub" || function_name_lowercase == "timestamp_sub")
                            storage.push_back(std::make_unique<DateAddLayer>("minus"));
                        else if (function_name_lowercase == "datediff" || function_name_lowercase == "date_diff"
                            || function_name_lowercase == "timestampdiff" || function_name_lowercase == "timestamp_diff")
                            storage.push_back(std::make_unique<DateDiffLayer>());
                        else
                            storage.push_back(std::make_unique<Layer>(TokenType::ClosingRoundBracket, function_name));
                    }
                }
                else if (pos->type == TokenType::OpeningSquareBracket)
                {
                    next = Action::OPERAND;

                    storage.back()->pushOperand(std::move(tmp));
                    storage.back()->pushOperator(Operator("arrayElement", 40, 2));
                    storage.push_back(std::make_unique<Layer>(TokenType::ClosingSquareBracket));
                    ++pos;
                }
                else
                {
                    storage.back()->pushOperand(std::move(tmp));
                }
            }
            else if (pos->type == TokenType::OpeningRoundBracket)
            {
                next = Action::OPERAND;
                storage.push_back(std::make_unique<Layer>(TokenType::ClosingRoundBracket, "tuple"));
                ++pos;
            }
            else if (pos->type == TokenType::OpeningSquareBracket)
            {
                ++pos;

                /// Special case for empty array: []
                if (pos->type == TokenType::ClosingSquareBracket)
                {
                    ++pos;
                    auto function = makeASTFunction("array");
                    storage.back()->pushOperand(function);
                }
                else
                {
                    next = Action::OPERAND;
                    storage.push_back(std::make_unique<Layer>(TokenType::ClosingSquareBracket, "array"));
                }
            }
            else
            {
                break;
            }
        }
        else
        {
            next = Action::OPERAND;

            /// Try to find operators from 'op_table'
            auto cur_op = op_table.begin();
            for (; cur_op != op_table.end(); ++cur_op)
            {
                if (parseOperator(pos, cur_op->first, expected))
                    break;
            }

            if (cur_op != op_table.end())
            {
                while (storage.back()->previousPriority() >= cur_op->second.priority)
                {
                    Operator prev_op;
                    storage.back()->popOperator(prev_op);
                    auto func = makeASTFunction(prev_op.func_name);

                    if (!storage.back()->lastNOperands(func->children[0]->children, prev_op.arity))
                        return false;

                    storage.back()->pushOperand(func);
                }
                storage.back()->pushOperator(cur_op->second);
            }
            else if (parseOperator(pos, "::", expected))
            {
                next = Action::OPERATOR;

                ASTPtr type_ast;
                if (!ParserDataType().parse(pos, type_ast, expected))
                    return false; // ???

                storage.back()->pushOperator(Operator("CAST", 50, 2));
                storage.back()->pushOperand(std::make_shared<ASTLiteral>(queryToString(type_ast)));
            }
            else if (pos->type == TokenType::Comma)
            {
                if (storage.size() == 1)
                    break;
            }
            else
            {
                break;
            }
        }
    }

    if (storage.size() > 1)
        return false;

    if (!storage.back()->wrapLayer())
        return false;

    if (!storage.back()->getResult(node))
        return false;

    return true;
}

////////////////////////////////////////////////////////////////////////////////////////

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

}
