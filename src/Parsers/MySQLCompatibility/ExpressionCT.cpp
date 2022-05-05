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

bool ExprLiteralNullCT::setup(String &)
{
    MySQLPtr literal_node = TreePath({"nullLiteral"}).evaluate(_source);

    return literal_node != nullptr;
}

void ExprLiteralNullCT::convert(CHPtr & ch_tree) const
{
    auto literal_node = std::make_shared<DB::ASTLiteral>(DB::Field());
    ch_tree = literal_node;
}

bool ExprLiteralBoolCT::setup(String &)
{
    MySQLPtr literal_node = TreePath({"boolLiteral"}).evaluate(_source);

    if (literal_node == nullptr)
        return false;

    value = (literal_node->terminal_types[0] == MySQLTree::TOKEN_TYPE::TRUE_SYMBOL);
    return true;
}

void ExprLiteralBoolCT::convert(CHPtr & ch_tree) const
{
    auto literal_node = std::make_shared<DB::ASTLiteral>(value);
    ch_tree = literal_node;
}

bool ExprLiteralInt64CT::setup(String &)
{
    MySQLPtr literal_node = TreePath({"numLiteral"}).evaluate(_source);

    if (literal_node == nullptr)
        return false;

    value = std::stoi(literal_node->terminals[0]);
    return true;
}

void ExprLiteralInt64CT::convert(CHPtr & ch_tree) const
{
    auto literal_node = std::make_shared<DB::ASTLiteral>(value);
    ch_tree = literal_node;
}

bool ExprLiteralFloat64CT::setup(String &)
{
    MySQLPtr literal_node = TreePath({"numLiteral"}).evaluate(_source);

    if (literal_node == nullptr)
        return false;

    value = std::stod(literal_node->terminals[0]);
    return true;
}

void ExprLiteralFloat64CT::convert(CHPtr & ch_tree) const
{
    auto literal_node = std::make_shared<DB::ASTLiteral>(value);
    ch_tree = literal_node;
}

bool ExprLiteralNumericCT::setup(String & error)
{
    MySQLPtr numeric_node = TreePath({"numLiteral"}).evaluate(_source);

    if (numeric_node == nullptr)
        return false;

    switch (numeric_node->terminal_types[0])
    {
        // FIXME: all int types ok?
        case MySQLTree::TOKEN_TYPE::INT_NUMBER:
            numeric_ct = std::make_shared<ExprLiteralInt64CT>(numeric_node);
            break;
        case MySQLTree::TOKEN_TYPE::FLOAT_NUMBER:
        case MySQLTree::TOKEN_TYPE::DECIMAL_NUMBER:
            numeric_ct = std::make_shared<ExprLiteralFloat64CT>(numeric_node);
            break;
        default:
            return false;
    }

    if (!numeric_ct->setup(error))
    {
        numeric_ct = nullptr;
        return false;
    }

    return true;
}

void ExprLiteralNumericCT::convert(CHPtr & ch_tree) const
{
    assert(numeric_ct);
    numeric_ct->convert(ch_tree);
}

bool ExprLiteralText::setup(String &)
{
    MySQLPtr text_literal = TreePath({"textLiteral"}).evaluate(_source);

    if (text_literal == nullptr)
        return false;

    auto string_path = TreePath({"textStringLiteral"});

    value = "";
    for (const auto & child : text_literal->children)
    {
        MySQLPtr text_string_node = nullptr;
        if ((text_string_node = string_path.evaluate(child)) != nullptr)
        {
            value += removeQuotes(text_string_node->terminals[0]);
        }
    }
    return true;
}

void ExprLiteralText::convert(CHPtr & ch_tree) const
{
    auto literal_node = std::make_shared<DB::ASTLiteral>(value);
    ch_tree = literal_node;
}

bool ExprGenericLiteralCT::setup(String & error)
{
    MySQLPtr literal_node = TreePath({"literal"}).evaluate(_source);

    if (literal_node == nullptr)
        return false;

    const String & rule_name = literal_node->children[0]->rule_name;

    if (rule_name == "numLiteral")
        literal_ct = std::make_shared<ExprLiteralNumericCT>(literal_node);
    else if (rule_name == "textLiteral")
        literal_ct = std::make_shared<ExprLiteralText>(literal_node);
    else if (rule_name == "boolLiteral")
        literal_ct = std::make_shared<ExprLiteralBoolCT>(literal_node);
    else if (rule_name == "nullLiteral")
        literal_ct = std::make_shared<ExprLiteralNullCT>(literal_node);
    else
        return false;

    if (!literal_ct->setup(error))
    {
        literal_ct = nullptr;
        return false;
    }

    return true;
}

void ExprGenericLiteralCT::convert(CHPtr & ch_tree) const
{
    assert(literal_ct);
    literal_ct->convert(ch_tree);
}

bool ExprIdentifierCT::setup(String & error)
{
    MySQLPtr identifier_node = TreePath({"pureIdentifier"}).evaluate(_source);

    if (identifier_node == nullptr)
    {
        error = "invalid identifier";
        return false;
    }
    assert(!identifier_node->terminals.empty());
    value = removeQuotes(identifier_node->terminals[0]);

    return true;
}

void ExprIdentifierCT::convert(CHPtr & ch_tree) const
{
    auto identifier = std::make_shared<DB::ASTIdentifier>(value);
    ch_tree = identifier;
}

bool ExprSimpleCT::setup(String & error)
{
    MySQLPtr simple_expr_node = TreePath({"simpleExpr"}).evaluate(_source);

    if (simple_expr_node == nullptr)
        return false;

    // ( subexpr )
    {
        MySQLPtr expr_list = TreePath({"exprList"}).evaluate(simple_expr_node, true);

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

    MySQLPtr column_node = TreePath({"columnRef"}).evaluate(simple_expr_node, true);
    if (column_node != nullptr)
    {
        subexpr_ct = std::make_shared<ExprIdentifierCT>(column_node);
        if (!subexpr_ct->setup(error))
        {
            subexpr_ct = nullptr;
            return false;
        }
        return true;
    }

    MySQLPtr literal_node = TreePath({"literal"}).evaluate(simple_expr_node, true);
    if (literal_node != nullptr)
    {
        subexpr_ct = std::make_shared<ExprGenericLiteralCT>(literal_node);
        if (!subexpr_ct->setup(error))
        {
            subexpr_ct = nullptr;
            return false;
        }
        return true;
    }

    error = "unknown expression parameter";
    return false;
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
    MySQLPtr bitexpr_node = TreePath({"bitExpr"}).evaluate(_source);

    if (bitexpr_node == nullptr)
        return false;

    MySQLPtr simple_expr_node = TreePath({"simpleExpr"}).evaluate(bitexpr_node, true);


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

        MySQLPtr first = child_expr_path.evaluate(bitexpr_node->children[0], true);
        MySQLPtr second = child_expr_path.evaluate(bitexpr_node->children[1], true);

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
    MySQLPtr bool_pri_node = TreePath({"boolPri"}).evaluate(_source);

    if (bool_pri_node == nullptr)
        return false;

    if (bool_pri_node->children.size() == 1)
    {
        // FIXME
        MySQLPtr predicate_ptr = TreePath({"predicate", "bitExpr"}).evaluate(bool_pri_node, true);

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
    MySQLPtr expr = _source;
    if (expr == nullptr)
        return false;

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
    if (not_rule)
    {
        CHPtr subexpr = nullptr;
        subexpr_ct->convert(subexpr);
        ch_tree = DB::makeASTFunction("not", subexpr);
        return;
    }

    if (subexpr_ct != nullptr)
    {
        subexpr_ct->convert(ch_tree);
        return;
    }

    spawnCHFunction(operation, first_operand_ct, second_operand_ct, ch_tree);
}
}
