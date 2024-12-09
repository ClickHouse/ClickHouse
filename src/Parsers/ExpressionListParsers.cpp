#include <string_view>
#include <unordered_map>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserUnionQueryElement.h>
#include <Parsers/parseIntervalKind.h>
#include <Common/assert_cast.h>
#include <Common/StringUtils.h>

#include <Parsers/ParserSelectWithUnionQuery.h>

#include <Common/logger_useful.h>
#include <Parsers/queryToString.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/Kusto/ParserKQLStatement.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

using namespace std::literals;


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTs elements;

    auto parse_element = [&]
    {
        ASTPtr element;
        if (!elem_parser->parse(pos, element, expected))
            return false;

        elements.push_back(std::move(element));
        return true;
    };

    if (!parseUtil(pos, expected, parse_element, *separator_parser, allow_empty))
        return false;

    node = std::make_shared<ASTExpressionList>(result_separator);
    node->children = std::move(elements);

    return true;
}

bool ParserUnionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserUnionQueryElement elem_parser;
    ParserKeyword s_union_parser(Keyword::UNION);
    ParserKeyword s_all_parser(Keyword::ALL);
    ParserKeyword s_distinct_parser(Keyword::DISTINCT);
    ParserKeyword s_except_parser(Keyword::EXCEPT);
    ParserKeyword s_intersect_parser(Keyword::INTERSECT);
    ASTs elements;

    auto parse_element = [&]
    {
        ASTPtr element;
        if (!elem_parser.parse(pos, element, expected))
            return false;

        elements.push_back(std::move(element));
        return true;
    };

    /// Parse UNION / INTERSECT / EXCEPT mode
    /// The mode can be DEFAULT (unspecified) / DISTINCT / ALL
    auto parse_separator = [&]
    {
        if (s_union_parser.ignore(pos, expected))
        {
            if (s_all_parser.check(pos, expected))
                union_modes.push_back(SelectUnionMode::UNION_ALL);
            else if (s_distinct_parser.check(pos, expected))
                union_modes.push_back(SelectUnionMode::UNION_DISTINCT);
            else
                union_modes.push_back(SelectUnionMode::UNION_DEFAULT);
            return true;
        }
        if (s_except_parser.check(pos, expected))
        {
            if (s_all_parser.check(pos, expected))
                union_modes.push_back(SelectUnionMode::EXCEPT_ALL);
            else if (s_distinct_parser.check(pos, expected))
                union_modes.push_back(SelectUnionMode::EXCEPT_DISTINCT);
            else
                union_modes.push_back(SelectUnionMode::EXCEPT_DEFAULT);
            return true;
        }
        if (s_intersect_parser.check(pos, expected))
        {
            if (s_all_parser.check(pos, expected))
                union_modes.push_back(SelectUnionMode::INTERSECT_ALL);
            else if (s_distinct_parser.check(pos, expected))
                union_modes.push_back(SelectUnionMode::INTERSECT_DISTINCT);
            else
                union_modes.push_back(SelectUnionMode::INTERSECT_DEFAULT);
            return true;
        }
        return false;
    };

    if (!parseUtil(pos, parse_element, parse_separator))
        return false;

    node = std::make_shared<ASTExpressionList>();
    node->children = std::move(elements);
    return true;
}

static bool parseOperator(IParser::Pos & pos, std::string_view op, Expected & expected)
{
    if (!op.empty() && isWordCharASCII(op.front()))
    {
        return ParserKeyword::createDeprecated(op).ignore(pos, expected);
    }
    if (op.length() == pos->size() && 0 == memcmp(op.data(), pos->begin, pos->size()))
    {
        ++pos;
        return true;
    }

    return false;
}

enum class SubqueryFunctionType : uint8_t
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

    auto new_subquery = std::make_shared<ASTSubquery>(std::move(select_with_union_query));
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
            if (!elem_parser->parse(pos, elem, expected))
                return false;

            node = std::move(elem);
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

            ASTPtr elem;

            if (!elem_parser->parse(pos, elem, expected))
                return false;

            /// the first argument of the function is the previous element, the second is the next one
            auto function = makeASTFunction(it[1], node, elem);

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


ASTPtr makeBetweenOperator(bool negative, ASTs arguments)
{
    // SUBJECT = arguments[0], LEFT = arguments[1], RIGHT = arguments[2]

    if (negative)
    {
        auto f_left_expr = makeASTFunction("less", arguments[0], arguments[1]);
        auto f_right_expr = makeASTFunction("greater", arguments[0], arguments[2]);
        return makeASTFunction("or", f_left_expr, f_right_expr);
    }

    auto f_left_expr = makeASTFunction("greaterOrEquals", arguments[0], arguments[1]);
    auto f_right_expr = makeASTFunction("lessOrEquals", arguments[0], arguments[2]);
    return makeASTFunction("and", f_left_expr, f_right_expr);
}

ParserExpressionWithOptionalAlias::ParserExpressionWithOptionalAlias(bool allow_alias_without_as_keyword, bool is_table_function, bool allow_trailing_commas)
    : impl(std::make_unique<ParserWithOptionalAlias>(
        is_table_function ? ParserPtr(std::make_unique<ParserTableFunctionExpression>()) : ParserPtr(std::make_unique<ParserExpression>(allow_trailing_commas)),
        allow_alias_without_as_keyword))
{
}


bool ParserExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(
        std::make_unique<ParserExpressionWithOptionalAlias>(allow_alias_without_as_keyword, is_table_function, allow_trailing_commas),
        std::make_unique<ParserToken>(TokenType::Comma))
        .parse(pos, node, expected);
}

bool ParserNotEmptyExpressionList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
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

        if (!open.ignore(pos, expected))
            return false;

        if (!kv_pairs_list.parse(pos, value, expected))
            return false;

        if (!close.ignore(pos, expected))
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

namespace
{
    /// This wrapper is needed to highlight function names differently.
    class ParserFunctionName : public IParserBase
    {
    protected:
        const char * getName() const override { return "function name"; }
        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
        {
            ParserCompoundIdentifier parser(false, true, Highlight::function);
            return parser.parse(pos, node, expected);
        }
    };
}


enum class Action : uint8_t
{
    NONE,
    OPERAND,
    OPERATOR
};

/** Operator types are needed for special handling of certain operators.
  * Operators can be grouped into some type if they have similar behaviour.
  * Certain operators are unique in terms of their behaviour, so they are assigned a separate type.
  */
enum class OperatorType : uint8_t
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
    Lambda,
    Not
};

/** Operator struct stores parameters of the operator:
  *  - function_name  name of the function that operator will create
  *  - priority       priority of the operator relative to the other operators
  *  - arity          the amount of arguments that operator will consume
  *  - type           type of the operator that defines its behaviour
  */
struct Operator
{
    Operator() = default;

    Operator(const std::string & function_name_,
             int priority_,
             int arity_,
             OperatorType type_ = OperatorType::None)
        : type(type_), priority(priority_), arity(arity_), function_name(function_name_) {}

    OperatorType type;
    int priority;
    int arity;
    std::string function_name;
};

template <typename... Args>
static std::shared_ptr<ASTFunction> makeASTFunction(Operator & op, Args &&... args)
{
    auto ast_function = makeASTFunction(op.function_name, std::forward<Args>(args)...);

    if (op.type == OperatorType::Lambda)
    {
        ast_function->is_lambda_function = true;
        ast_function->kind = ASTFunction::Kind::LAMBDA_FUNCTION;
    }
    return ast_function;
}

enum class Checkpoint : uint8_t
{
    None,
    Interval,
    Case
};

/** Layer is a class that represents context for parsing certain element,
  *  that consists of other elements e.g. f(x1, x2, x3)
  *
  *  - Manages operands and operators for the future elements (arguments)
  *  - Combines operands and operator into one element
  *  - Parsers separators and endings
  *  - Combines resulting elements into a function
  */

class Layer
{
public:
    explicit Layer(bool allow_alias_ = true, bool allow_alias_without_as_keyword_ = false) :
        allow_alias(allow_alias_), allow_alias_without_as_keyword(allow_alias_without_as_keyword_) {}

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
        elements.push_back(std::move(op));
    }

    virtual bool getResult(ASTPtr & node)
    {
        if (!finished)
            return false;

        return getResultImpl(node);
    }

    virtual bool parse(IParser::Pos & /*pos*/, Expected & /*expected*/, Action & /*action*/) = 0;

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

    int isCurrentElementEmpty() const
    {
        return operators.empty() && operands.empty();
    }

    bool popLastNOperands(ASTs & asts, size_t n)
    {
        if (n > operands.size())
            return false;

        asts.reserve(asts.size() + n);

        auto * start = operands.begin() + operands.size() - n;
        asts.insert(asts.end(), std::make_move_iterator(start), std::make_move_iterator(operands.end()));
        operands.erase(start, operands.end());

        return true;
    }

    /// Merge operators and operands into a single element (column), then push it to 'elements' vector.
    ///  Operators are previously sorted in ascending order of priority
    ///  (operator with priority 1 has higher priority than operator with priority 2),
    ///  so we can just merge them with operands starting from the end.
    ///
    /// If we fail here it means that the query was incorrect and we should return an error.
    ///
    bool mergeElement(bool push_to_elements = true)
    {
        parsed_alias = false;

        Operator cur_op;
        while (popOperator(cur_op))
        {
            ASTPtr function;

            // We should not meet the starting part of the operator while finishing an element
            if (cur_op.type == OperatorType::StartIf ||
                cur_op.type == OperatorType::StartBetween ||
                cur_op.type == OperatorType::StartNotBetween)
                return false;

            if (cur_op.type == OperatorType::FinishIf)
            {
                Operator tmp;
                if (!popOperator(tmp) || tmp.type != OperatorType::StartIf)
                    return false;
            }

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
                /// enable using subscript operator for kql_array_sort
                if (cur_op.function_name == "arrayElement" && !operands.empty())
                {
                    auto* first_arg_as_node = operands.front()->as<ASTFunction>();
                    if (first_arg_as_node)
                    {
                        if (first_arg_as_node->name == "kql_array_sort_asc" || first_arg_as_node->name == "kql_array_sort_desc")
                        {
                            cur_op.function_name = "tupleElement";
                            cur_op.type = OperatorType::TupleElement;
                        }
                        else if (first_arg_as_node->name == "arrayElement" && !first_arg_as_node->arguments->children.empty())
                        {
                            auto *arg_inside = first_arg_as_node->arguments->children[0]->as<ASTFunction>();
                            if (arg_inside && (arg_inside->name == "kql_array_sort_asc" || arg_inside->name == "kql_array_sort_desc"))
                                first_arg_as_node->name = "tupleElement";
                        }
                    }
                }

                function = makeASTFunction(cur_op);

                if (!popLastNOperands(function->children[0]->children, cur_op.arity))
                    return false;
            }

            pushOperand(function);
        }

        ASTPtr node;
        if (!popOperand(node))
            return false;

        bool res = isCurrentElementEmpty();

        if (push_to_elements)
            pushResult(node);
        else
            pushOperand(node);

        return res;
    }

    bool parseLambda()
    {
        // 1. If empty - create function tuple with 0 args
        if (isCurrentElementEmpty())
        {
            auto function = makeASTFunction("tuple");
            pushOperand(function);
            return true;
        }

        if (operands.size() != 1 || !operators.empty() || !mergeElement())
            return false;

        /// 2. If there is already tuple do nothing
        if (tryGetFunctionName(elements.back()) == "tuple")
        {
            pushOperand(std::move(elements.back()));
            elements.pop_back();
        }
        /// 3. Put all elements in a single tuple
        else
        {
            auto function = makeASTFunction("tuple", std::move(elements));
            elements.clear();
            pushOperand(function);
        }

        /// We must check that tuple arguments are identifiers
        auto * func_ptr = operands.back()->as<ASTFunction>();
        auto * args_ptr = func_ptr->arguments->as<ASTExpressionList>();

        for (const auto & child : args_ptr->children)
        {
            if (typeid_cast<ASTIdentifier *>(child.get()))
                continue;

            return false;
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
        {
            tryGetIdentifierNameInto(node, ast_with_alias->alias);
            return true;
        }

        return false;
    }

    bool is_table_function = false;

    /// 'AND' in operator '... BETWEEN ... AND ...'  mirrors logical operator 'AND'.
    ///  In order to distinguish them we keep a counter of BETWEENs without matching ANDs.
    int between_counter = 0;

    /// Flag we set when we parsed alias to avoid parsing next element as alias
    bool parsed_alias = false;

    bool allow_alias = true;
    bool allow_alias_without_as_keyword = true;

    std::optional<std::pair<IParser::Pos, Checkpoint>> saved_checkpoint;
    Checkpoint current_checkpoint = Checkpoint::None;

protected:
    virtual bool getResultImpl(ASTPtr & node)
    {
        if (elements.size() == 1)
        {
            node = std::move(elements[0]);
            return true;
        }

        return false;
    }

    std::vector<Operator> operators;
    ASTs operands;
    ASTs elements;
    bool finished = false;
    int state = 0;
};

struct ParserExpressionImpl
{
    static const std::vector<std::pair<std::string_view, Operator>> operators_table;
    static const std::vector<std::pair<std::string_view, Operator>> unary_operators_table;

    static const Operator finish_between_operator;

    ParserFunctionName function_name_parser;
    ParserCompoundIdentifier identifier_parser{false, true};
    ParserNumber number_parser;
    ParserAsterisk asterisk_parser;
    ParserLiteral literal_parser;
    ParserTupleOfLiterals tuple_literal_parser;
    ParserArrayOfLiterals array_literal_parser;
    ParserSubstitution substitution_parser;
    ParserMySQLGlobalVariable mysql_global_variable_parser;

    ParserKeyword any_parser{Keyword::ANY};
    ParserKeyword all_parser{Keyword::ALL};

    // Recursion
    ParserQualifiedAsterisk qualified_asterisk_parser;
    ParserColumnsMatcher columns_matcher_parser;
    ParserQualifiedColumnsMatcher qualified_columns_matcher_parser;
    ParserSubquery subquery_parser;

    bool parse(std::unique_ptr<Layer> start, IParser::Pos & pos, ASTPtr & node, Expected & expected);

    using Layers = std::vector<std::unique_ptr<Layer>>;

    Action tryParseOperand(Layers & layers, IParser::Pos & pos, Expected & expected);
    Action tryParseOperator(Layers & layers, IParser::Pos & pos, Expected & expected);
};

class ExpressionLayer : public Layer
{
public:

    explicit ExpressionLayer(bool is_table_function_, bool allow_trailing_commas_ = false)
        : Layer(false, false)
    {
        is_table_function = is_table_function_;
        allow_trailing_commas = allow_trailing_commas_;
    }

    bool getResult(ASTPtr & node) override
    {
        /// We can exit the main cycle outside the parse() function,
        ///  so we need to merge the element here.
        /// Because of this 'finished' flag can also not be set.
        if (!mergeElement())
            return false;

        return Layer::getResultImpl(node);
    }

    bool parse(IParser::Pos & pos, Expected & /*expected*/, Action & /*action*/) override
    {
        if (pos->type == TokenType::Comma)
        {
            finished = true;

            if (!allow_trailing_commas)
                return true;

            /// We support trailing commas at the end of the column declaration:
            ///  - SELECT a, b, c, FROM table
            ///  - SELECT 1,

            /// For this purpose we need to eliminate the following cases:
            ///  1. WITH 1 AS from SELECT 2, from
            ///  2. SELECT to, from FROM table
            ///  3. SELECT to, from AS alias FROM table
            ///  4. SELECT to, from + to, from IN [1,2,3], FROM table

            Expected test_expected;
            auto test_pos = pos;
            ++test_pos;

            /// End of query
            if (test_pos.isValid() && test_pos->type != TokenType::Semicolon)
            {
                /// If we can't parse FROM then return
                if (!ParserKeyword(Keyword::FROM).ignore(test_pos, test_expected))
                    return true;

                // If there is a comma after 'from' then the first one was a name of a column
                if (test_pos->type == TokenType::Comma)
                    return true;

                /// If we parse a second FROM then the first one was a name of a column
                if (ParserKeyword(Keyword::FROM).ignore(test_pos, test_expected))
                    return true;

                /// If we parse an explicit alias to FROM, then it was a name of a column
                if (ParserAlias(false).ignore(test_pos, test_expected))
                    return true;

                /// If we parse an operator after FROM then it was a name of a column
                auto cur_op = ParserExpressionImpl::operators_table.begin();
                for (; cur_op != ParserExpressionImpl::operators_table.end(); ++cur_op)
                {
                    if (parseOperator(test_pos, cur_op->first, test_expected))
                        break;
                }

                if (cur_op != ParserExpressionImpl::operators_table.end())
                    return true;
            }

            ++pos;
            return true;
        }

        return true;
    }

private:
    bool allow_trailing_commas;
};

/// Basic layer for a function with certain separator and end tokens:
///  1. If we parse a separator we should merge current operands and operators
///     into one element and push in to 'elements' vector.
///  2. If we parse an ending token, we should merge everything as in (1) and
///     also set 'finished' flag.
template <TokenType separator, TokenType end>
class LayerWithSeparator : public Layer
{
public:
    explicit LayerWithSeparator(bool allow_alias_ = true, bool allow_alias_without_as_keyword_ = false) :
        Layer(allow_alias_, allow_alias_without_as_keyword_) {}

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

            if (!isCurrentElementEmpty() || !elements.empty())
                if (!mergeElement())
                    return false;

            finished = true;
        }

        return true;
    }
};

/// Layer for regular and aggregate functions without syntax sugar
class FunctionLayer : public Layer
{
public:
    explicit FunctionLayer(String function_name_, bool allow_function_parameters_ = true, bool is_compound_name_ = false)
        : function_name(function_name_), allow_function_parameters(allow_function_parameters_), is_compound_name(is_compound_name_){}

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

            ParserKeyword all(Keyword::ALL);
            ParserKeyword distinct(Keyword::DISTINCT);

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

                if (!isCurrentElementEmpty() || !elements.empty())
                    if (!mergeElement())
                        return false;

                contents_end = pos->begin;

                /** Check for a common error case - often due to the complexity of quoting command-line arguments,
                 *  an expression of the form toDate(2014-01-01) appears in the query instead of toDate('2014-01-01').
                 * If you do not report that the first option is an error, then the argument will be interpreted as 2014 - 01 - 01 - some number,
                 *  and the query silently returns an unexpected elements.
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
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Argument of function toDate is unquoted: "
                        "toDate({}), must be: toDate('{}')" , contents_str, contents_str);
                }

                if (allow_function_parameters && !parameters && ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
                {
                    parameters = std::make_shared<ASTExpressionList>();
                    std::swap(parameters->children, elements);
                    action = Action::OPERAND;

                    /// Parametric aggregate functions cannot have DISTINCT in parameters list.
                    if (has_distinct)
                        return false;

                    auto pos_after_bracket = pos;
                    auto old_expected = expected;

                    ParserKeyword all(Keyword::ALL);
                    ParserKeyword distinct(Keyword::DISTINCT);

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

            auto function_node = makeASTFunction(function_name, std::move(elements));
            function_node->is_compound_name = is_compound_name;

            if (parameters)
            {
                function_node->parameters = std::move(parameters);
                function_node->children.push_back(function_node->parameters);
            }

            ParserKeyword filter(Keyword::FILTER);
            ParserKeyword over(Keyword::OVER);
            ParserKeyword respect_nulls(Keyword::RESPECT_NULLS);
            ParserKeyword ignore_nulls(Keyword::IGNORE_NULLS);

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

            if (respect_nulls.ignore(pos, expected))
                function_node->nulls_action = NullsAction::RESPECT_NULLS;
            else if (ignore_nulls.ignore(pos, expected))
                function_node->nulls_action = NullsAction::IGNORE_NULLS;

            if (over.ignore(pos, expected))
            {
                function_node->is_window_function = true;
                function_node->kind = ASTFunction::Kind::WINDOW_FUNCTION;

                ASTPtr function_node_as_iast = function_node;

                // Recursion
                ParserWindowReference window_reference;
                if (!window_reference.parse(pos, function_node_as_iast, expected))
                    return false;
            }

            elements = {std::move(function_node)};
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

    bool allow_function_parameters;
    bool is_compound_name;
};

/// Layer for priority brackets and tuple function
class RoundBracketsLayer : public Layer
{
public:
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

            if (!isCurrentElementEmpty())
                if (!mergeElement())
                    return false;

            if (!is_tuple && elements.size() == 1)
            {
                // Special case for (('a', 'b')) = tuple(('a', 'b'))
                if (auto * literal = elements[0]->as<ASTLiteral>())
                    if (literal->value.getType() == Field::Types::Tuple)
                        is_tuple = true;

                // Special case for f(x, (y) -> z) = f(x, tuple(y) -> z)
                if (pos->type == TokenType::Arrow)
                    is_tuple = true;
            }

            finished = true;
        }

        return true;
    }

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        // Round brackets can mean priority operator as well as function tuple()
        if (!is_tuple && elements.size() == 1)
            node = std::move(elements[0]);
        else
            node = makeASTFunction("tuple", std::move(elements));

        return true;
    }

private:
    bool is_tuple = false;
};

/// Layer for array square brackets operator
class ArrayLayer : public LayerWithSeparator<TokenType::Comma, TokenType::ClosingSquareBracket>
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        return LayerWithSeparator::parse(pos, expected, action);
    }

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        node = makeASTFunction("array", std::move(elements));
        return true;
    }
};

/// Layer for arrayElement square brackets operator
///  This layer does not create a function, it is only needed to parse closing token
///  and return only one element.
class ArrayElementLayer : public LayerWithSeparator<TokenType::Comma, TokenType::ClosingSquareBracket>
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        return LayerWithSeparator::parse(pos, expected, action);
    }
};

class CastLayer : public Layer
{
public:
    CastLayer() : Layer(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true) {}

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// CAST(x [AS alias1], T [AS alias2]) or CAST(x [AS alias1] AS T)
        ///
        /// 0. Parse all the cases (-> 1)
        /// 1. Parse closing token (finished)

        ParserKeyword as_keyword_parser(Keyword::AS);
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

                    elements = {createFunctionCast(elements[0], type_node)};
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

                    elements = {createFunctionCast(elements[0], type_node)};
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

                if (elements.size() != 2)
                    return false;

                elements = {makeASTFunction(toString(toStringView(Keyword::CAST)), elements[0], elements[1])};
                finished = true;
                return true;
            }
        }

        return true;
    }
};

class ExtractLayer : public LayerWithSeparator<TokenType::Comma, TokenType::ClosingRoundBracket>
{
public:
    ExtractLayer() : LayerWithSeparator(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true) {}

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
            ParserKeyword s_from(Keyword::FROM);

            if (parseIntervalKind(pos, expected, interval_kind) && s_from.ignore(pos, expected))
            {
                state = 2;
                return true;
            }

            state = 1;
            pos = begin;
        }

        if (state == 1)
        {
            return LayerWithSeparator::parse(pos, expected, action);
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

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        if (state == 2)
        {
            if (elements.empty())
                return false;

            node = makeASTFunction(interval_kind.toNameOfFunctionExtractTimePart(), elements[0]);
        }
        else
        {
            node = makeASTFunction("extract", std::move(elements));
        }

        return true;
    }


private:
    IntervalKind interval_kind;
};

class SubstringLayer : public Layer
{
public:
    SubstringLayer() : Layer(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true) {}

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
                ParserKeyword(Keyword::FROM).ignore(pos, expected))
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
                ParserKeyword(Keyword::FOR).ignore(pos, expected))
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

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        node = makeASTFunction("substring", std::move(elements));
        return true;
    }
};

class PositionLayer : public Layer
{
public:
    PositionLayer() : Layer(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true) {}

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
            if (ParserKeyword(Keyword::IN).ignore(pos, expected))
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

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        if (state == 2 && elements.size() == 2)
            std::swap(elements[1], elements[0]);

        node = makeASTFunction("position", std::move(elements));
        return true;
    }
};

class ExistsLayer : public Layer
{
public:
    ExistsLayer() : Layer(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true) {}

    bool parse(IParser::Pos & pos, Expected & expected, Action & /*action*/) override
    {
        ASTPtr node;

        // Recursion
        if (!ParserSelectWithUnionQuery().parse(pos, node, expected))
            return false;

        if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            return false;

        auto subquery = std::make_shared<ASTSubquery>(std::move(node));
        elements = {makeASTFunction("exists", subquery)};

        finished = true;

        return true;
    }
};

class TrimLayer : public Layer
{
public:
    TrimLayer(bool trim_left_, bool trim_right_)
        : Layer(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true), trim_left(trim_left_), trim_right(trim_right_) {}

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
                if (ParserKeyword(Keyword::BOTH).ignore(pos, expected))
                {
                    trim_left = true;
                    trim_right = true;
                    char_override = true;
                }
                else if (ParserKeyword(Keyword::LEADING).ignore(pos, expected))
                {
                    trim_left = true;
                    char_override = true;
                }
                else if (ParserKeyword(Keyword::TRAILING).ignore(pos, expected))
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
            if (ParserKeyword(Keyword::FROM).ignore(pos, expected))
            {
                action = Action::OPERAND;

                if (!mergeElement())
                    return false;

                /// Trimming an empty string is a no-op.
                ASTLiteral * ast_literal = typeid_cast<ASTLiteral *>(elements[0].get());
                if (ast_literal && ast_literal->value.getType() == Field::Types::String && ast_literal->value.safeGet<String>().empty())
                {
                    noop = true;
                }
                else
                {
                    to_remove = makeASTFunction("regexpQuoteMeta", elements[0]);
                    elements.clear();
                }

                state = 2;
            }
        }

        if (state == 2)
        {
            if (ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                if (noop)
                {
                    /// The operation does nothing.
                }
                if (char_override)
                {
                    ASTPtr pattern_node;

                    auto pattern_func_node = std::make_shared<ASTFunction>();
                    auto pattern_list_args = std::make_shared<ASTExpressionList>();
                    if (trim_left && trim_right)
                    {
                        pattern_list_args->children =
                        {
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
                            pattern_list_args->children =
                            {
                                std::make_shared<ASTLiteral>("^["),
                                to_remove,
                                std::make_shared<ASTLiteral>("]+")
                            };
                        }
                        else
                        {
                            /// trim_right == false not possible
                            pattern_list_args->children =
                            {
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

                    elements.push_back(pattern_node);
                    elements.push_back(std::make_shared<ASTLiteral>(""));
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

                finished = true;
            }
        }

        return true;
    }

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        if (noop)
            node = std::move(elements.at(1));
        else
            node = makeASTFunction(function_name, std::move(elements));
        return true;
    }

private:
    bool trim_left;
    bool trim_right;
    bool char_override = false;
    bool noop = false;

    ASTPtr to_remove;
    String function_name;
};

class DateAddLayer : public LayerWithSeparator<TokenType::Comma, TokenType::ClosingRoundBracket>
{
public:
    explicit DateAddLayer(const char * function_name_)
        : LayerWithSeparator(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true), function_name(function_name_) {}

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
            return LayerWithSeparator::parse(pos, expected, action);
        }

        return true;
    }

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        if (parsed_interval_kind)
        {
            if (elements.size() < 2)
                return false;

            elements[0] = makeASTFunction(interval_kind.toNameOfFunctionToIntervalDataType(), elements[0]);
            node = makeASTFunction(function_name, elements[1], elements[0]);
        }
        else
            node = makeASTFunction(function_name, std::move(elements));

        return true;
    }

private:
    IntervalKind interval_kind;
    const char * function_name;
    bool parsed_interval_kind = false;
};

class DateDiffLayer : public LayerWithSeparator<TokenType::Comma, TokenType::ClosingRoundBracket>
{
public:
    DateDiffLayer() : LayerWithSeparator(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true) {}

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
            return LayerWithSeparator::parse(pos, expected, action);
        }

        return true;
    }

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        if (parsed_interval_kind)
        {
            if (elements.size() == 2)
                node = makeASTFunction("dateDiff", std::make_shared<ASTLiteral>(interval_kind.toDateDiffUnit()), elements[0], elements[1]);
            else if (elements.size() == 3)
                node = makeASTFunction("dateDiff", std::make_shared<ASTLiteral>(interval_kind.toDateDiffUnit()), elements[0], elements[1], elements[2]);
            else
                return false;
        }
        else
        {
            node = makeASTFunction("dateDiff", std::move(elements));
        }
        return true;
    }

private:
    IntervalKind interval_kind;
    bool parsed_interval_kind = false;
};

class TupleLayer : public LayerWithSeparator<TokenType::Comma, TokenType::ClosingRoundBracket>
{
public:
    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        bool result = LayerWithSeparator::parse(pos, expected, action);

        /// Check that after the tuple() function there is no lambdas operator
        if (finished && pos->type == TokenType::Arrow)
            return false;

        return result;
    }

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        node = makeASTFunction("tuple", std::move(elements));
        return true;
    }
};


class IntervalLayer : public Layer
{
public:
    IntervalLayer() : Layer(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true) {}

    bool parse(IParser::Pos & pos, Expected & expected, Action & action) override
    {
        /// INTERVAL 1 HOUR or INTERVAL expr HOUR
        ///
        /// 0. Try to parse interval_kind (-> 1)
        /// 1. Basic parser

        if (state == 0)
        {
            state = 1;

            auto begin = pos;
            auto init_expected = expected;
            ASTPtr string_literal;
            String literal;

            //// A String literal followed INTERVAL keyword,
            /// the literal can be a part of an expression or
            /// include Number and INTERVAL TYPE at the same time
            if (ParserStringLiteral{}.parse(pos, string_literal, expected)
                && string_literal->as<ASTLiteral &>().value.tryGet(literal))
            {
                Tokens tokens(literal.data(), literal.data() + literal.size());
                IParser::Pos token_pos(tokens, pos.max_depth, pos.max_backtracks);
                Expected token_expected;
                ASTPtr expr;

                if (!ParserNumber{}.parse(token_pos, expr, token_expected))
                    return false;

                /// case: INTERVAL '1' HOUR
                /// back to begin
                if (!token_pos.isValid())
                {
                    pos = begin;
                    expected = init_expected;
                    return true;
                }

                /// case: INTERVAL '1 HOUR'
                if (!parseIntervalKind(token_pos, token_expected, interval_kind))
                    return false;

                pushResult(makeASTFunction(interval_kind.toNameOfFunctionToIntervalDataType(), expr));

                /// case: INTERVAL '1 HOUR 1 SECOND ...'
                while (token_pos.isValid())
                {
                    if (!ParserNumber{}.parse(token_pos, expr, token_expected) ||
                        !parseIntervalKind(token_pos, token_expected, interval_kind))
                        return false;

                    pushResult(makeASTFunction(interval_kind.toNameOfFunctionToIntervalDataType(), expr));
                }

                finished = true;
            }
            return true;
        }

        if (state == 1)
        {
            if (action == Action::OPERATOR && parseIntervalKind(pos, expected, interval_kind))
            {
                if (!mergeElement())
                    return false;

                elements = {makeASTFunction(interval_kind.toNameOfFunctionToIntervalDataType(), elements)};
                finished = true;
            }
        }

        return true;
    }

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        if (elements.size() == 1)
            node = elements[0];
        else
            node = makeASTFunction("tuple", std::move(elements));

        return true;
    }

private:
    IntervalKind interval_kind;
};

class CaseLayer : public Layer
{
public:
    CaseLayer() : Layer(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true) {}

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
            has_case_expr = !ParserKeyword(Keyword::WHEN).ignore(pos, expected);
            pos = old_pos;

            state = 1;
        }

        if (state == 1)
        {
            if (ParserKeyword(Keyword::WHEN).ignore(pos, expected))
            {
                if ((has_case_expr || !elements.empty()) && !mergeElement())
                    return false;

                action = Action::OPERAND;
                state = 2;
            }
            else if (ParserKeyword(Keyword::ELSE).ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                action = Action::OPERAND;
                state = 3;
            }
            else if (ParserKeyword(Keyword::END).ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                Field field_with_null;
                ASTLiteral null_literal(field_with_null);
                elements.push_back(std::make_shared<ASTLiteral>(null_literal));

                if (has_case_expr)
                    elements = {makeASTFunction("caseWithExpression", elements)};
                else
                    elements = {makeASTFunction("multiIf", elements)};
                finished = true;
            }
        }

        if (state == 2)
        {
            if (ParserKeyword(Keyword::THEN).ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                action = Action::OPERAND;
                state = 1;
            }
        }

        if (state == 3)
        {
            if (ParserKeyword(Keyword::END).ignore(pos, expected))
            {
                if (!mergeElement())
                    return false;

                if (has_case_expr)
                    elements = {makeASTFunction("caseWithExpression", elements)};
                else
                    elements = {makeASTFunction("multiIf", elements)};

                finished = true;
            }
        }

        return true;
    }

private:
    bool has_case_expr;
};

/// Layer for table function 'view' and 'viewIfPermitted'
class ViewLayer : public Layer
{
public:
    explicit ViewLayer(bool if_permitted_) : if_permitted(if_permitted_) {}

    bool parse(IParser::Pos & pos, Expected & expected, Action & /*action*/) override
    {
        /// view(SELECT ...)
        /// viewIfPermitted(SELECT ... ELSE func(...))
        ///
        /// 0. Parse the SELECT query and if 'if_permitted' parse 'ELSE' keyword (-> 1) else (finished)
        /// 1. Parse closing token

        if (state == 0)
        {
            ASTPtr query;

            bool maybe_an_subquery = pos->type == TokenType::OpeningRoundBracket;

            if (!ParserSelectWithUnionQuery().parse(pos, query, expected))
                return false;

            auto & select_ast = query->as<ASTSelectWithUnionQuery &>();
            if (select_ast.list_of_selects->children.size() == 1 && maybe_an_subquery)
            {
                // It's an subquery. Bail out.
                return false;
            }

            pushResult(query);

            if (!if_permitted)
            {
                if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                    return false;

                finished = true;
                return true;
            }

            if (!ParserKeyword{Keyword::ELSE}.ignore(pos, expected))
                return false;

            state = 1;
            return true;
        }

        if (state == 1)
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

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        if (if_permitted)
            node = makeASTFunction("viewIfPermitted", std::move(elements));
        else
            node = makeASTFunction("view", std::move(elements));

        return true;
    }

private:
    bool if_permitted;
};

/// Layer for table function 'kql'
class KustoLayer : public Layer
{
public:

    KustoLayer() : Layer(/*allow_alias*/ true, /*allow_alias_without_as_keyword*/ true) {}

    bool parse(IParser::Pos & pos, Expected & expected, Action & /*action*/) override
    {
        /// kql('table|project ...')
        /// 0. Parse the kql query
        /// 1. Parse closing token
        if (state == 0)
        {
            ASTPtr query;
            --pos;
            if (!ParserKQLTableFunction().parse(pos, query, expected))
                return false;
            --pos;
            pushResult(query);

            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                return false;

            finished = true;
            state = 1;
            return true;
        }

        if (state == 1)
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

protected:
    bool getResultImpl(ASTPtr & node) override
    {
        node = makeASTFunction("view", std::move(elements)); // reuse view function for kql
        return true;
    }
};

std::unique_ptr<Layer> getFunctionLayer(ASTPtr identifier, bool is_table_function, bool allow_function_parameters_ = true)
{
    /// Special cases for expressions that look like functions but contain some syntax sugar:

    /// CAST, EXTRACT, POSITION, EXISTS
    /// DATE_ADD, DATEADD, TIMESTAMPADD, DATE_SUB, DATESUB, TIMESTAMPSUB,
    /// DATE_DIFF, DATEDIFF, TIMESTAMPDIFF, TIMESTAMP_DIFF,
    /// SUBSTRING, TRIM, LTRIM, RTRIM, POSITION

    /// Can be parsed as a composition of functions, but the contents must be unwrapped:
    /// POSITION(x IN y) -> POSITION(in(x, y)) -> POSITION(y, x)

    /// Can be parsed as a function, but not always:
    /// CAST(x AS type) - alias has to be unwrapped
    /// CAST(x AS type(params))

    /// Can be parsed as a function, but some identifier arguments have special meanings.
    /// DATE_ADD(MINUTE, x, y) -> addMinutes(x, y)
    /// DATE_DIFF(MINUTE, x, y)

    /// Have keywords that have to processed explicitly:
    /// EXTRACT(x FROM y)
    /// TRIM(BOTH|LEADING|TRAILING x FROM y)
    /// SUBSTRING(x FROM a)
    /// SUBSTRING(x FROM a FOR b)

    String function_name = getIdentifierName(identifier);
    String function_name_lowercase = Poco::toLower(function_name);

    if (is_table_function)
    {
        if (function_name_lowercase == "view")
            return std::make_unique<ViewLayer>(false);
        if (function_name_lowercase == "viewifpermitted")
            return std::make_unique<ViewLayer>(true);
        if (function_name_lowercase == "kql")
            return std::make_unique<KustoLayer>();
    }

    if (function_name == "tuple")
        return std::make_unique<TupleLayer>();

    if (function_name_lowercase == "cast")
        return std::make_unique<CastLayer>();
    if (function_name_lowercase == "extract")
        return std::make_unique<ExtractLayer>();
    if (function_name_lowercase == "substring")
        return std::make_unique<SubstringLayer>();
    if (function_name_lowercase == "position")
        return std::make_unique<PositionLayer>();
    if (function_name_lowercase == "exists")
        return std::make_unique<ExistsLayer>();
    if (function_name_lowercase == "trim")
        return std::make_unique<TrimLayer>(false, false);
    if (function_name_lowercase == "ltrim")
        return std::make_unique<TrimLayer>(true, false);
    if (function_name_lowercase == "rtrim")
        return std::make_unique<TrimLayer>(false, true);
    if (function_name_lowercase == "dateadd" || function_name_lowercase == "date_add" || function_name_lowercase == "timestampadd"
        || function_name_lowercase == "timestamp_add")
        return std::make_unique<DateAddLayer>("plus");
    if (function_name_lowercase == "datesub" || function_name_lowercase == "date_sub" || function_name_lowercase == "timestampsub"
        || function_name_lowercase == "timestamp_sub")
        return std::make_unique<DateAddLayer>("minus");
    if (function_name_lowercase == "datediff" || function_name_lowercase == "date_diff" || function_name_lowercase == "timestampdiff"
        || function_name_lowercase == "timestamp_diff")
        return std::make_unique<DateDiffLayer>();
    if (function_name_lowercase == "grouping")
        return std::make_unique<FunctionLayer>(function_name_lowercase, allow_function_parameters_);
    return std::make_unique<FunctionLayer>(function_name, allow_function_parameters_, identifier->as<ASTIdentifier>()->compound());
}


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
    if (!ParserKeyword(Keyword::DATE).ignore(pos, expected))
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
    if (!ParserKeyword(Keyword::TIMESTAMP).ignore(pos, expected))
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
    auto start = std::make_unique<ExpressionLayer>(false, allow_trailing_commas);
    return ParserExpressionImpl().parse(std::move(start), pos, node, expected);
}

bool ParserTableFunctionExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto start = std::make_unique<ExpressionLayer>(true);
    return ParserExpressionImpl().parse(std::move(start), pos, node, expected);
}

bool ParserArray::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto start = std::make_unique<ArrayLayer>();
    return ParserToken(TokenType::OpeningSquareBracket).ignore(pos, expected)
        && ParserExpressionImpl().parse(std::move(start), pos, node, expected);
}

bool ParserFunction::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr identifier;

    if (ParserFunctionName().parse(pos, identifier, expected)
        && ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
    {
        auto start = getFunctionLayer(identifier, is_table_function, allow_function_parameters);
        start->is_table_function = is_table_function;
        return ParserExpressionImpl().parse(std::move(start), pos, node, expected);
    }

    return false;
}

bool ParserExpressionWithOptionalArguments::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier id_p;
    ParserFunction func_p;

    if (ParserFunction(false, false).parse(pos, node, expected))
        return true;

    if (ParserIdentifier().parse(pos, node, expected))
    {
        node = makeASTFunction(node->as<ASTIdentifier>()->name());
        node->as<ASTFunction &>().no_empty_args = true;
        return true;
    }

    return false;
}

const std::vector<std::pair<std::string_view, Operator>> ParserExpressionImpl::operators_table
{
    {"->",            Operator("lambda",          1,  2, OperatorType::Lambda)},
    {"?",             Operator("",                2,  0, OperatorType::StartIf)},
    {":",             Operator("if",              3,  3, OperatorType::FinishIf)},
    {toStringView(Keyword::OR),            Operator("or",              3,  2, OperatorType::Mergeable)},
    {toStringView(Keyword::AND),           Operator("and",             4,  2, OperatorType::Mergeable)},
    {toStringView(Keyword::IS_NOT_DISTINCT_FROM), Operator("isNotDistinctFrom", 6, 2)},
    {toStringView(Keyword::IS_NULL),       Operator("isNull",          6,  1, OperatorType::IsNull)},
    {toStringView(Keyword::IS_NOT_NULL),   Operator("isNotNull",       6,  1, OperatorType::IsNull)},
    {toStringView(Keyword::BETWEEN),       Operator("",                7,  0, OperatorType::StartBetween)},
    {toStringView(Keyword::NOT_BETWEEN),   Operator("",                7,  0, OperatorType::StartNotBetween)},
    {"==",            Operator("equals",          9,  2, OperatorType::Comparison)},
    {"!=",            Operator("notEquals",       9,  2, OperatorType::Comparison)},
    {"<=>",           Operator("isNotDistinctFrom", 9, 2, OperatorType::Comparison)},
    {"<>",            Operator("notEquals",       9,  2, OperatorType::Comparison)},
    {"<=",            Operator("lessOrEquals",    9,  2, OperatorType::Comparison)},
    {">=",            Operator("greaterOrEquals", 9,  2, OperatorType::Comparison)},
    {"<",             Operator("less",            9,  2, OperatorType::Comparison)},
    {">",             Operator("greater",         9,  2, OperatorType::Comparison)},
    {"=",             Operator("equals",          9,  2, OperatorType::Comparison)},
    {toStringView(Keyword::LIKE),          Operator("like",            9,  2)},
    {toStringView(Keyword::ILIKE),         Operator("ilike",           9,  2)},
    {toStringView(Keyword::NOT_LIKE),      Operator("notLike",         9,  2)},
    {toStringView(Keyword::NOT_ILIKE),     Operator("notILike",        9,  2)},
    {toStringView(Keyword::REGEXP),        Operator("match",           9,  2)},
    {toStringView(Keyword::IN),            Operator("in",              9,  2)},
    {toStringView(Keyword::NOT_IN),        Operator("notIn",           9,  2)},
    {toStringView(Keyword::GLOBAL_IN),     Operator("globalIn",        9,  2)},
    {toStringView(Keyword::GLOBAL_NOT_IN), Operator("globalNotIn",     9,  2)},
    {"||",            Operator("concat",          10, 2, OperatorType::Mergeable)},
    {"+",             Operator("plus",            11, 2)},
    {"-",             Operator("minus",           11, 2)},
    {"−",             Operator("minus",           11, 2)},
    {"*",             Operator("multiply",        12, 2)},
    {"/",             Operator("divide",          12, 2)},
    {"%",             Operator("modulo",          12, 2)},
    {toStringView(Keyword::MOD),           Operator("modulo",          12, 2)},
    {toStringView(Keyword::DIV),           Operator("intDiv",          12, 2)},
    {".",             Operator("tupleElement",    14, 2, OperatorType::TupleElement)},
    {"[",             Operator("arrayElement",    14, 2, OperatorType::ArrayElement)},
    {"::",            Operator(toString(toStringView(Keyword::CAST)),            14, 2, OperatorType::Cast)},
};

const std::vector<std::pair<std::string_view, Operator>> ParserExpressionImpl::unary_operators_table
{
    {toStringView(Keyword::NOT), Operator("not", 5, 1, OperatorType::Not)},
    {"-", Operator("negate", 13, 1)},
    {"−", Operator("negate", 13, 1)},
};

const Operator ParserExpressionImpl::finish_between_operator("", 8, 0, OperatorType::FinishBetween);


bool ParserExpressionImpl::parse(std::unique_ptr<Layer> start, IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    Action next = Action::OPERAND;

    Layers layers;
    layers.push_back(std::move(start));

    while (true)
    {
        while (pos.isValid())
        {
            if (!layers.back()->parse(pos, expected, next))
                break;

            if (layers.back()->isFinished())
            {
                if (layers.size() == 1)
                    break;

                next = Action::OPERATOR;

                ASTPtr res;
                if (!layers.back()->getResult(res))
                    break;

                layers.pop_back();
                layers.back()->pushOperand(res);
                continue;
            }

            if (next == Action::OPERAND)
                next = tryParseOperand(layers, pos, expected);
            else
                next = tryParseOperator(layers, pos, expected);

            if (next == Action::NONE)
                break;
        }

        /// When we exit the loop we should be on the 1st level
        if (layers.size() == 1 && layers.back()->getResult(node))
            return true;

        layers.pop_back();

        /// We try to check whether there was a checkpoint
        while (!layers.empty() && !layers.back()->saved_checkpoint)
            layers.pop_back();

        if (layers.empty())
            return false;

        /// Currently all checkpoints are located in operand section
        next = Action::OPERAND;

        auto saved_checkpoint = layers.back()->saved_checkpoint.value();
        layers.back()->saved_checkpoint.reset();

        pos = saved_checkpoint.first;
        layers.back()->current_checkpoint = saved_checkpoint.second;
    }
}

Action ParserExpressionImpl::tryParseOperand(Layers & layers, IParser::Pos & pos, Expected & expected)
{
    ASTPtr tmp;

    if (layers.front()->is_table_function)
    {
        if (typeid_cast<ViewLayer *>(layers.back().get()) || typeid_cast<KustoLayer *>(layers.back().get()))
        {
            if (function_name_parser.parse(pos, tmp, expected)
                && ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
            {
                layers.push_back(getFunctionLayer(tmp, layers.front()->is_table_function));
                return Action::OPERAND;
            }
            return Action::NONE;
        }

        /// Current element should be empty (there should be no other operands or operators)
        /// to parse SETTINGS in table function
        if (layers.back()->isCurrentElementEmpty())
        {
            auto old_pos = pos;
            ParserKeyword s_settings(Keyword::SETTINGS);
            if (s_settings.ignore(pos, expected))
            {
                ParserSetQuery parser_settings(true);
                if (parser_settings.parse(pos, tmp, expected))
                {
                    layers.back()->pushOperand(tmp);
                    return Action::OPERAND;
                }

                pos = old_pos;
            }
        }
    }

    /// Special case for cast expression
    if (layers.back()->previousType() != OperatorType::TupleElement &&
        ParseCastExpression(pos, tmp, expected))
    {
        layers.back()->pushOperand(std::move(tmp));
        return Action::OPERATOR;
    }

    if (layers.back()->previousType() == OperatorType::Comparison)
    {
        auto old_pos = pos;
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
                return Action::NONE;
            if (!layers.back()->popOperand(argument))
                return Action::NONE;

            function = makeASTFunction(prev_op, argument, tmp);

            if (!modifyAST(function, subquery_function_type))
                return Action::NONE;

            layers.back()->pushOperand(std::move(function));
            return Action::OPERATOR;
        }

        pos = old_pos;
    }

    /// ignore all leading plus
    while (pos->type == TokenType::Plus)
    {
        ++pos;
    }

    /// Try to find any unary operators
    auto cur_op = unary_operators_table.begin();
    for (; cur_op != unary_operators_table.end(); ++cur_op)
    {
        if (parseOperator(pos, cur_op->first, expected))
            break;
    }

    if (cur_op != unary_operators_table.end())
    {
        if (cur_op->second.type == OperatorType::Not && pos->type == TokenType::OpeningRoundBracket)
        {
            ++pos;
            auto identifier = std::make_shared<ASTIdentifier>(cur_op->second.function_name);
            layers.push_back(getFunctionLayer(identifier, layers.front()->is_table_function));
        }
        else
        {
            layers.back()->pushOperator(cur_op->second);
        }
        return Action::OPERAND;
    }

    auto old_pos = pos;
    auto current_checkpoint = layers.back()->current_checkpoint;
    layers.back()->current_checkpoint = Checkpoint::None;

    if (current_checkpoint != Checkpoint::Interval && parseOperator(pos, toStringView(Keyword::INTERVAL), expected))
    {
        layers.back()->saved_checkpoint = {old_pos, Checkpoint::Interval};
        layers.push_back(std::make_unique<IntervalLayer>());
        return Action::OPERAND;
    }
    if (current_checkpoint != Checkpoint::Case && parseOperator(pos, toStringView(Keyword::CASE), expected))
    {
        layers.back()->saved_checkpoint = {old_pos, Checkpoint::Case};
        layers.push_back(std::make_unique<CaseLayer>());
        return Action::OPERAND;
    }

    if (ParseDateOperatorExpression(pos, tmp, expected) ||
        ParseTimestampOperatorExpression(pos, tmp, expected) ||
        tuple_literal_parser.parse(pos, tmp, expected) ||
        array_literal_parser.parse(pos, tmp, expected) ||
        number_parser.parse(pos, tmp, expected) ||
        literal_parser.parse(pos, tmp, expected) ||
        asterisk_parser.parse(pos, tmp, expected) ||
        qualified_asterisk_parser.parse(pos, tmp, expected) ||
        columns_matcher_parser.parse(pos, tmp, expected) ||
        qualified_columns_matcher_parser.parse(pos, tmp, expected))
    {
        layers.back()->pushOperand(std::move(tmp));
    }
    else
    {
        old_pos = pos;
        if (function_name_parser.parse(pos, tmp, expected) && pos->type == TokenType::OpeningRoundBracket)
        {
            ++pos;
            layers.push_back(getFunctionLayer(tmp, layers.front()->is_table_function));
            return Action::OPERAND;
        }
        pos = old_pos;

        if (identifier_parser.parse(pos, tmp, expected))
        {
            layers.back()->pushOperand(std::move(tmp));
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
                return Action::OPERATOR;
            }

            ++pos;
            layers.push_back(std::make_unique<RoundBracketsLayer>());
            return Action::OPERAND;
        }
        else if (pos->type == TokenType::OpeningSquareBracket)
        {
            ++pos;
            layers.push_back(std::make_unique<ArrayLayer>());
            return Action::OPERAND;
        }
        else if (mysql_global_variable_parser.parse(pos, tmp, expected))
        {
            layers.back()->pushOperand(std::move(tmp));
        }
        else
        {
            return Action::NONE;
        }
    }

    return Action::OPERATOR;
}

Action ParserExpressionImpl::tryParseOperator(Layers & layers, IParser::Pos & pos, Expected & expected)
{
    /// ParserExpression can be called in this part of the query:
    ///  ALTER TABLE partition_all2 CLEAR INDEX [ p ] IN PARTITION ALL
    ///
    /// 'IN PARTITION' here is not an 'IN' operator, so we should stop parsing immediately
    Expected stub;
    if (ParserKeyword(Keyword::IN_PARTITION).checkWithoutMoving(pos, stub))
        return Action::NONE;

    /// Try to find operators from 'operators_table'
    auto cur_op = operators_table.begin();
    for (; cur_op != operators_table.end(); ++cur_op)
    {
        if (parseOperator(pos, cur_op->first, expected))
            break;
    }

    if (cur_op == operators_table.end())
    {
        if (!layers.back()->allow_alias || layers.back()->parsed_alias)
            return Action::NONE;

        ASTPtr alias;
        ParserAlias alias_parser(layers.back()->allow_alias_without_as_keyword);

        if (!alias_parser.parse(pos, alias, expected) || !layers.back()->insertAlias(alias))
            return Action::NONE;

        layers.back()->parsed_alias = true;
        return Action::OPERATOR;
    }

    auto op = cur_op->second;

    if (op.type == OperatorType::Lambda)
    {
        if (!layers.back()->parseLambda())
            return Action::NONE;

        layers.back()->pushOperator(op);
        return Action::OPERAND;
    }

    /// 'AND' can be both boolean function and part of the '... BETWEEN ... AND ...' operator
    if (op.function_name == "and" && layers.back()->between_counter)
    {
        --layers.back()->between_counter;
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
                return Action::NONE;

            if (tmp_op.type != OperatorType::StartBetween && tmp_op.type != OperatorType::StartNotBetween)
                return Action::NONE;

            bool negative = tmp_op.type == OperatorType::StartNotBetween;

            ASTs arguments;
            if (!layers.back()->popLastNOperands(arguments, 3))
                return Action::NONE;

            function = makeBetweenOperator(negative, arguments);
        }
        else
        {
            function = makeASTFunction(prev_op);

            if (!layers.back()->popLastNOperands(function->children[0]->children, prev_op.arity))
                return Action::NONE;
        }

        layers.back()->pushOperand(function);
    }

    /// Dot (TupleElement operator) can be a beginning of a .* or .COLUMNS expressions
    if (op.type == OperatorType::TupleElement)
    {
        ASTPtr tmp;
        if (asterisk_parser.parse(pos, tmp, expected)
            || columns_matcher_parser.parse(pos, tmp, expected))
        {
            if (auto * asterisk = tmp->as<ASTAsterisk>())
            {
                if (!layers.back()->popOperand(asterisk->expression))
                    return Action::NONE;
            }
            else if (auto * columns_list_matcher = tmp->as<ASTColumnsListMatcher>())
            {
                if (!layers.back()->popOperand(columns_list_matcher->expression))
                    return Action::NONE;
            }
            else if (auto * columns_regexp_matcher = tmp->as<ASTColumnsRegexpMatcher>())
            {
                if (!layers.back()->popOperand(columns_regexp_matcher->expression))
                    return Action::NONE;
            }

            layers.back()->pushOperand(std::move(tmp));
            return Action::OPERATOR;
        }

        /// If it is an identifier,
        /// replace it with literal, because an expression `expr().elem`
        /// should be transformed to `tupleElement(expr(), 'elem')` for query analysis,
        /// otherwise the identifier `elem` will not be found.
        if (ParserIdentifier().parse(pos, tmp, expected))
        {
            layers.back()->pushOperator(op);
            layers.back()->pushOperand(std::make_shared<ASTLiteral>(tmp->as<ASTIdentifier>()->name()));
            return Action::OPERATOR;
        }
    }

    /// isNull & isNotNull are postfix unary operators
    if (op.type == OperatorType::IsNull)
    {
        ASTPtr function = makeASTFunction(op);

        if (!layers.back()->popLastNOperands(function->children[0]->children, 1))
            return Action::NONE;

        layers.back()->pushOperand(std::move(function));
        return Action::OPERATOR;
    }

    layers.back()->pushOperator(op);

    if (op.type == OperatorType::Cast)
    {
        ASTPtr type_ast;
        if (!ParserDataType().parse(pos, type_ast, expected))
            return Action::NONE;

        layers.back()->pushOperand(std::make_shared<ASTLiteral>(queryToString(type_ast)));
        return Action::OPERATOR;
    }

    if (op.type == OperatorType::ArrayElement)
        layers.push_back(std::make_unique<ArrayElementLayer>());

    if (op.type == OperatorType::StartBetween || op.type == OperatorType::StartNotBetween)
        ++layers.back()->between_counter;

    return Action::OPERAND;
}

}
