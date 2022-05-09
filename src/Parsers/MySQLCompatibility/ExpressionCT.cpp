#include <Parsers/MySQLCompatibility/ExpressionCT.h>
#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/util.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace MySQLCompatibility
{

static String getCHFunctionName(MySQLTree::TOKEN_TYPE operation)
{
    switch (operation)
    {
        case MySQLTree::TOKEN_TYPE::PLUS_OPERATOR:
            return "plus";
        case MySQLTree::TOKEN_TYPE::MINUS_OPERATOR:
            return "minus";
        case MySQLTree::TOKEN_TYPE::MULT_OPERATOR:
            return "multiply";
        case MySQLTree::TOKEN_TYPE::DIV_OPERATOR:
            return "divide";
        case MySQLTree::TOKEN_TYPE::DIV_SYMBOL:
            return "intDiv";
        case MySQLTree::TOKEN_TYPE::MOD_OPERATOR:
            return "modulo";
        case MySQLTree::TOKEN_TYPE::MOD_SYMBOL:
            return "modulo";

        case MySQLTree::TOKEN_TYPE::GREATER_THAN_OPERATOR:
            return "greater";
        case MySQLTree::TOKEN_TYPE::GREATER_OR_EQUAL_OPERATOR:
            return "greaterOrEquals";
        case MySQLTree::TOKEN_TYPE::LESS_THAN_OPERATOR:
            return "less";
        case MySQLTree::TOKEN_TYPE::LESS_OR_EQUAL_OPERATOR:
            return "lessOrEquals";
        case MySQLTree::TOKEN_TYPE::EQUAL_OPERATOR:
            return "equals";
        case MySQLTree::TOKEN_TYPE::NOT_EQUAL_OPERATOR:
            return "notEquals";
        case MySQLTree::TOKEN_TYPE::NULL_SAFE_EQUAL_OPERATOR:
            return "nullSafeEquals";

        case MySQLTree::TOKEN_TYPE::AND_SYMBOL:
            return "and";
        case MySQLTree::TOKEN_TYPE::LOGICAL_AND_OPERATOR:
            return "and";
        case MySQLTree::TOKEN_TYPE::OR_SYMBOL:
            return "or";
        case MySQLTree::TOKEN_TYPE::LOGICAL_OR_OPERATOR:
            return "or";
        case MySQLTree::TOKEN_TYPE::XOR_SYMBOL:
            return "xor";

        case MySQLTree::TOKEN_TYPE::COUNT_SYMBOL:
            return "count";

        case MySQLTree::TOKEN_TYPE::AVG_SYMBOL:
            return "avg";
        case MySQLTree::TOKEN_TYPE::MIN_SYMBOL:
            return "min";
        case MySQLTree::TOKEN_TYPE::MAX_SYMBOL:
            return "max";
        case MySQLTree::TOKEN_TYPE::STD_SYMBOL:
            return "stddevPop";
        case MySQLTree::TOKEN_TYPE::VARIANCE_SYMBOL:
            return "varPop";
        case MySQLTree::TOKEN_TYPE::STDDEV_SAMP_SYMBOL:
            return "stddevSamp";
        case MySQLTree::TOKEN_TYPE::VAR_SAMP_SYMBOL:
            return "varSamp";
        case MySQLTree::TOKEN_TYPE::SUM_SYMBOL:
            return "sum";
        default:
            return "";
    }
}

static void spawnNullSafeEqFunction(CHPtr & ch_tree, CHPtr & x, CHPtr & y)
{
    auto value_eq = DB::makeASTFunction("equals", x, y);

    auto x_null = DB::makeASTFunction("isNull", x);
    auto y_null = DB::makeASTFunction("isNull", y);
    auto null_eq = DB::makeASTFunction("equals", x_null, y_null);

    auto cond = DB::makeASTFunction("or", x_null, y_null);
    auto if_operator = DB::makeASTFunction("if", cond, null_eq, value_eq);

    ch_tree = if_operator;
}

static void spawnCHFunction(MySQLTree::TOKEN_TYPE operation, const ConvPtr & x, const ConvPtr & y, CHPtr & ch_tree)
{
    assert(x && y);

    CHPtr first_node = nullptr;
    x->convert(first_node);

    CHPtr second_node = nullptr;
    y->convert(second_node);

    String op_name = getCHFunctionName(operation);
    if (op_name == "nullSafeEquals")
    {
        spawnNullSafeEqFunction(ch_tree, first_node, second_node);
        return;
    }
    ch_tree = DB::makeASTFunction(op_name, first_node, second_node);
}

bool ExprGenericLiteralCT::setup(String & error)
{
    const MySQLPtr & literal_node = getSourceNode();

    if (!tryExtractLiteral(literal_node, value))
    {
        error = "invalid literal";
        return false;
    }

    return true;
}

void ExprGenericLiteralCT::convert(CHPtr & ch_tree) const
{
    auto literal = std::make_shared<DB::ASTLiteral>(value);
    ch_tree = literal;
}

bool ExprIdentifierCT::setup(String & error)
{
    MySQLPtr identifier_node = getSourceNode();

    if (!tryExtractIdentifier(identifier_node, value))
    {
        error = "invalid identifier";
        return false;
    }
    return true;
}

void ExprIdentifierCT::convert(CHPtr & ch_tree) const
{
    auto identifier = std::make_shared<DB::ASTIdentifier>(value);
    ch_tree = identifier;
}

bool ExprVariableCT::setup(String & error)
{
    const MySQLPtr & var_node = getSourceNode();

    MySQLPtr sys_var_node = TreePath({"systemVariable"}).descend(var_node);

    if (sys_var_node == nullptr)
    {
        error = "only system variables are supported now";
        return false;
    }

    MySQLPtr sys_var_content = TreePath({"textOrIdentifier", "pureIdentifier"}).find(sys_var_node);

    assert(sys_var_content != nullptr);
    varname = sys_var_content->terminals[0];

    return true;
}

void ExprVariableCT::convert(CHPtr & ch_tree) const
{
    auto literal_node = std::make_shared<DB::ASTLiteral>(varname);
    ch_tree = DB::makeASTFunction("globalVariable", literal_node);
}

// TODO: full syntax
bool ExprSumCT::setup(String & error)
{
    error = "";

    const MySQLPtr sum_expr_node = getSourceNode();

    if (!getCHFunctionName(sum_expr_node->terminal_types[0]).empty())
    {
        function_type = sum_expr_node->terminal_types[0];

        MySQLPtr arg_node = TreePath({"inSumExpr", "expr"}).descend(sum_expr_node);

        expr_ct = std::make_shared<ExpressionCT>(arg_node);
        if (!expr_ct->setup(error))
        {
            expr_ct = nullptr;
            return false;
        }
        return true;
    }

    error = "unknown function";
    return false;
}

void ExprSumCT::convert(CHPtr & ch_tree) const
{
    assert(expr_ct);

    CHPtr expr_node = nullptr;
    expr_ct->convert(expr_node);

    String function_name = getCHFunctionName(function_type);

    ch_tree = DB::makeASTFunction(function_name, expr_node);
}

bool ExprSimpleCT::setup(String & error)
{
    MySQLPtr simple_expr_node = TreePath({"simpleExpr"}).find(getSourceNode());

    if (simple_expr_node == nullptr)
        return false;

    // ( subexpr )
    {
        MySQLPtr expr_list = TreePath({"exprList"}).descend(simple_expr_node);

        if (expr_list != nullptr)
        {
            // FIXME: list
            if (expr_list->children.size() != 1)
                return false;

            subexpr_ct = std::make_shared<ExpressionCT>(expr_list->children[0]);
            if (!subexpr_ct->setup(error))
            {
                subexpr_ct = nullptr;
                return false;
            }

            return true;
        }
    }

    if (!simple_expr_node->terminal_types.empty())
    {
        if (simple_expr_node->terminal_types.size() == 1)
        {
            switch (simple_expr_node->terminal_types[0])
            {
                case MySQLTree::TOKEN_TYPE::MINUS_OPERATOR:
                    negate = true;
                    subexpr_ct = std::make_shared<ExprSimpleCT>(simple_expr_node->children[0]);
                    break;
                default:
                    return false;
            }
            if (!subexpr_ct->setup(error))
            {
                subexpr_ct = nullptr;
                return false;
            }
            return true;
        }
        else
            return false;
    }

    MySQLPtr subexpr_node = nullptr;

    auto column_path = TreePath({"columnRef"});
    auto literal_path = TreePath({"literal"});
    auto var_path = TreePath({"variable"});
    auto sum_path = TreePath({"sumExpr"});

    if ((subexpr_node = column_path.descend(simple_expr_node)) != nullptr)
    {
        MySQLPtr identifier_node = TreePath({"identifier"}).find(subexpr_node);
        if (identifier_node == nullptr)
        {
            error = "invalid column name";
            return false;
        }
        subexpr_ct = std::make_shared<ExprIdentifierCT>(identifier_node);
    }
    else if ((subexpr_node = literal_path.descend(simple_expr_node)) != nullptr)
        subexpr_ct = std::make_shared<ExprGenericLiteralCT>(subexpr_node);
    else if ((subexpr_node = var_path.descend(simple_expr_node)) != nullptr)
        subexpr_ct = std::make_shared<ExprVariableCT>(subexpr_node);
    else if ((subexpr_node = sum_path.descend(simple_expr_node)) != nullptr)
        subexpr_ct = std::make_shared<ExprSumCT>(subexpr_node);
    else
    {
        error = "unknown expression parameter";
        return false;
    }

    if (!subexpr_ct->setup(error))
    {
        subexpr_ct = nullptr;
        return false;
    }

    return true;
}

void ExprSimpleCT::convert(CHPtr & ch_tree) const
{
    CHPtr subexpr = nullptr;
    subexpr_ct->convert(subexpr);

    if (negate)
        ch_tree = DB::makeASTFunction("negate", subexpr);
    else
        ch_tree = subexpr;
}

bool ExprBitCT::setup(String & error)
{
    MySQLPtr bitexpr_node = TreePath({"bitExpr"}).find(getSourceNode());

    if (bitexpr_node == nullptr)
        return false;

    MySQLPtr simple_expr_node = TreePath({"simpleExpr"}).descend(bitexpr_node);


    if (simple_expr_node != nullptr)
    {
        simple_expr_ct = std::make_shared<ExprSimpleCT>(simple_expr_node);
        if (!simple_expr_ct->setup(error))
        {
            simple_expr_ct = nullptr;
            return false;
        }
        return true;
    }

    // normal operation
    if (bitexpr_node->children.size() == 2)
    {
        auto child_expr_path = TreePath({"bitExpr"});

        MySQLPtr first = child_expr_path.descend(bitexpr_node->children[0]);
        MySQLPtr second = child_expr_path.descend(bitexpr_node->children[1]);

        operation = bitexpr_node->terminal_types[0];

        if (getCHFunctionName(operation).empty())
            return false;

        first_operand_ct = std::make_shared<ExprBitCT>(first);
        if (!first_operand_ct->setup(error))
        {
            first_operand_ct = nullptr;
            return false;
        }

        second_operand_ct = std::make_shared<ExprBitCT>(second);
        if (!second_operand_ct->setup(error))
        {
            second_operand_ct = first_operand_ct = nullptr;
            return false;
        }

        return true;
    }

    return false; // FIXME
}

void ExprBitCT::convert(CHPtr & ch_tree) const
{
    if (simple_expr_ct != nullptr)
    {
        simple_expr_ct->convert(ch_tree);
        return;
    }

    spawnCHFunction(operation, first_operand_ct, second_operand_ct, ch_tree);
}

bool ExprBoolStatementCT::setup(String & error)
{
    MySQLPtr bool_pri_node = TreePath({"boolPri"}).find(getSourceNode());

    if (bool_pri_node == nullptr)
        return false;

    if (bool_pri_node->children.size() == 1)
    {
        // FIXME
        MySQLPtr predicate_ptr = TreePath({"predicate", "bitExpr"}).descend(bool_pri_node);

        if (predicate_ptr != nullptr)
        {
            predicate_ct = std::make_shared<ExprBitCT>(predicate_ptr);
            if (!predicate_ct->setup(error))
            {
                predicate_ct = nullptr;
                return false;
            }

            return true;
        }

        return false;
    }

    if (bool_pri_node->children.size() == 3)
    {
        if (bool_pri_node->children[2]->rule_name == "predicate")
        {
            MySQLPtr first_operand = bool_pri_node->children[0];
            MySQLPtr second_operand = bool_pri_node->children[2];

            operation = bool_pri_node->children[1]->terminal_types[0];

            if (getCHFunctionName(operation).empty())
                return false;

            first_operand_ct = std::make_shared<ExprBoolStatementCT>(first_operand);
            if (!first_operand_ct->setup(error))
            {
                first_operand_ct = nullptr;
                return false;
            }

            second_operand_ct = std::make_shared<ExprBitCT>(second_operand);
            if (!second_operand_ct->setup(error))
            {
                second_operand_ct = first_operand_ct = nullptr;
                return false;
            }

            return true;
        }

        return false;
    }

    return true;
}

void ExprBoolStatementCT::convert(CHPtr & ch_tree) const
{
    if (predicate_ct)
    {
        predicate_ct->convert(ch_tree);
        return;
    }

    spawnCHFunction(operation, first_operand_ct, second_operand_ct, ch_tree);
}
bool ExpressionCT::setup(String & error)
{
    MySQLPtr expr = getSourceNode();

    if (expr->children.size() == 1)
    {
        if (expr->children[0]->rule_name == "expr")
        {
            assert(!expr->terminal_types.empty());
            assert(expr->terminal_types[0] == MySQLTree::TOKEN_TYPE::NOT_SYMBOL);

            not_rule = true;
            subexpr_ct = std::make_shared<ExpressionCT>(expr->children[0]);
            if (!subexpr_ct->setup(error))
            {
                subexpr_ct = nullptr;
                return false;
            }
            return true;
        }
        else if (expr->children[0]->rule_name == "boolPri")
        {
            MySQLPtr bool_pri_node = expr->children[0];
            subexpr_ct = std::make_shared<ExprBoolStatementCT>(bool_pri_node);
            if (!subexpr_ct->setup(error))
            {
                subexpr_ct = nullptr;
                return false;
            }
            return true;
        }

        return false;
    }

    if (expr->children.size() == 2)
    {
        if (expr->children[0]->rule_name == "expr" && expr->children[0]->rule_name == "expr")
        {
            operation = expr->terminal_types[0];

            if (getCHFunctionName(operation).empty())
                return false;

            first_operand_ct = std::make_shared<ExpressionCT>(expr->children[0]);
            if (!first_operand_ct->setup(error))
            {
                first_operand_ct = nullptr;
                return false;
            }

            second_operand_ct = std::make_shared<ExpressionCT>(expr->children[1]);
            if (!second_operand_ct->setup(error))
            {
                second_operand_ct = first_operand_ct = nullptr;
                return false;
            }

            return true;
        }

        return false;
    }

    return true;
}
void ExpressionCT::convert(CHPtr & ch_tree) const
{
    if (subexpr_ct != nullptr)
    {
        CHPtr subexpr = nullptr;
        subexpr_ct->convert(subexpr);
        ch_tree = not_rule ? DB::makeASTFunction("not", subexpr) : subexpr;
        return;
    }

    spawnCHFunction(operation, first_operand_ct, second_operand_ct, ch_tree);
}
}
