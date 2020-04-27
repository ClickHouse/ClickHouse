#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/ArithmeticOperationsInAgrFuncOptimize.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

static constexpr const char * min = "min";
static constexpr const char * max = "max";
static constexpr const char * mul = "multiply";
static constexpr const char * plus = "plus";
static constexpr const char * sum = "sum";

bool isConstantField(const Field & field)
{
    return field.getType() == Field::Types::Int64 ||
           field.getType() == Field::Types::UInt64 ||
           field.getType() == Field::Types::Int128 ||
           field.getType() == Field::Types::UInt128;
}

bool onlyConstsInside(ASTFunction * func_node)
{
    return !(func_node->arguments->children[0]->as<ASTFunction>()) &&
           (func_node->arguments->children.size() == 2 &&
           !(func_node->arguments->children[1]->as<ASTFunction>()));
}
bool inappropriateNameInside(ASTFunction * func_node, const char * inter_func_name)
{
    return (func_node->arguments->children[0]->as<ASTFunction>() &&
           inter_func_name != func_node->arguments->children[0]->as<ASTFunction>()->name) ||
           (func_node->arguments->children.size() == 2 &&
           func_node->arguments->children[1]->as<ASTFunction>() &&
           inter_func_name != func_node->arguments->children[1]->as<ASTFunction>()->name);
}

bool isInapprop(ASTPtr & node, const char * inter_func_name)
{
    return !node->as<ASTFunction>() || inter_func_name != node->as<ASTFunction>()->name;
}

ASTFunction * getInternalFunction(ASTFunction * f_n)
{
    return f_n->arguments->children[0]->as<ASTFunction>();
}

ASTFunction * treeFiller(ASTFunction * old_tree, ASTs & nodes_array, size_t size, const char * name)
{
    for (size_t i = 0; i < size; ++i)
    {
        old_tree->arguments->children = {};
        old_tree->arguments->children.push_back(nodes_array[i]);

        old_tree->arguments->children.push_back(makeASTFunction(name));
        old_tree = old_tree->arguments->children[1]->as<ASTFunction>();
    }
    return old_tree;
}

/// scalar values from the first level
std::pair<ASTs, ASTs> tryGetConst(const char * name, ASTs & arguments)
{
    ASTs const_num;
    ASTs not_const;

    for (const auto & arg : arguments)
    {
        if (const auto * literal = arg->as<ASTLiteral>())
        {
            if (isConstantField(literal->value))
                const_num.push_back(arg);
            else
                not_const.push_back(arg);
        }
        else
            not_const.push_back(arg);
    }

    if ((name == plus || name == mul) && const_num.size() + not_const.size() != 2)
    {
        throw Exception("Wrong number of arguments for function 'plus' or 'multiply' (" + toString(const_num.size() + not_const.size()) + " instead of 2)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    return {const_num, not_const};
}

std::pair<ASTs, ASTs> findAllConsts(ASTFunction * func_node, const char * inter_func_name)
{
    if (!func_node->arguments)
        return {};

    if (onlyConstsInside(func_node))
        return tryGetConst(func_node->name.c_str(), func_node->arguments->children);
    else if (inappropriateNameInside(func_node, inter_func_name))
    {
        if (func_node->arguments->children[0]->as<ASTLiteral>() &&
            isConstantField(func_node->arguments->children[0]->as<ASTLiteral>()->value))
            return {{func_node->arguments->children[0]}, {func_node->arguments->children[1]}};
        else if (func_node->arguments->children.size() == 2 &&
                 func_node->arguments->children[1]->as<ASTLiteral>() &&
                 isConstantField(func_node->arguments->children[1]->as<ASTLiteral>()->value))
            return {{func_node->arguments->children[1]}, {func_node->arguments->children[0]}};
        else
        {
            if (isInapprop(func_node->arguments->children[0], inter_func_name) && isInapprop(func_node->arguments->children[1], inter_func_name))
                return {{}, {func_node->arguments->children[0], func_node->arguments->children[1]}};
            else if (isInapprop(func_node->arguments->children[0], inter_func_name))
            {
                std::pair<ASTs, ASTs> ans = findAllConsts(func_node->arguments->children[1]->as<ASTFunction>(), inter_func_name);
                ans.second.push_back(func_node->arguments->children[0]);
                return ans;
            }
            else
            {
                std::pair<ASTs, ASTs> ans = findAllConsts(func_node->arguments->children[0]->as<ASTFunction>(), inter_func_name);
                ans.second.push_back(func_node->arguments->children[1]);
                return ans;
            }
        }
    }
    else
    {
        std::pair<ASTs, ASTs> fl = tryGetConst(func_node->name.c_str(), func_node->arguments->children);
        ASTs first_lvl_consts = fl.first;
        ASTs first_lvl_not_consts = fl.second;
        if (!first_lvl_not_consts[0]->as<ASTFunction>())
            return {first_lvl_consts, first_lvl_not_consts};

        std::pair<ASTs, ASTs> ans = findAllConsts(first_lvl_not_consts[0]->as<ASTFunction>(), inter_func_name);
        ASTs all_consts = ans.first;
        ASTs all_not_consts = ans.second;

        if (first_lvl_consts.size() == 1)
        {
            if (!first_lvl_not_consts[0]->as<ASTFunction>())
                all_not_consts.push_back(first_lvl_not_consts[0]);

            all_consts.push_back(first_lvl_consts[0]);
        }
        else if (first_lvl_consts.empty())
        {
            if (!first_lvl_not_consts[0]->as<ASTFunction>() || first_lvl_not_consts[0]->as<ASTFunction>()->name != inter_func_name)
                all_not_consts.push_back(first_lvl_not_consts[0]);

            if (first_lvl_not_consts.size() == 2 && (!first_lvl_not_consts[1]->as<ASTFunction>() ||
                first_lvl_not_consts[1]->as<ASTFunction>()->name != inter_func_name))
                all_not_consts.push_back(first_lvl_not_consts[1]);
        }
        else
            throw Exception("did not expect that", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
        return {all_consts, all_not_consts};
    }
}

/// rebuilds tree, all scalar values now outside the main func
void buildTree(ASTFunction * old_tree, const char * func_name, const char * intro_func, std::pair<ASTs, ASTs> & tree_comp)
{
    ASTFunction copy = *old_tree;
    ASTs cons_val = tree_comp.first;
    ASTs non_cons = tree_comp.second;

    old_tree->name = intro_func;
    old_tree = treeFiller(old_tree, cons_val, cons_val.size(), intro_func);
    old_tree->name = func_name;

    if (non_cons.size() == 1)
        old_tree->arguments->children.push_back(non_cons[0]);
    else
    {
        old_tree->arguments->children.push_back(makeASTFunction(intro_func));
        old_tree = old_tree->arguments->children[0]->as<ASTFunction>();
        old_tree = treeFiller(old_tree, non_cons, non_cons.size() - 2, intro_func);
        old_tree->arguments->children = {non_cons[non_cons.size() - 2], non_cons[non_cons.size() - 1]};
    }
}

void sumOptimize(ASTFunction * f_n)
{
    const auto * function_args = f_n->arguments->as<ASTExpressionList>();

    if (!function_args || function_args->children.size() != 1)
        throw Exception("Wrong number of arguments for function 'sum' (" + toString(function_args->children.size()) + " instead of 1)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTFunction * inter_node = getInternalFunction(f_n);
    if (inter_node && inter_node->name == mul)
    {
        std::pair<ASTs, ASTs> nodes = findAllConsts(f_n, mul);

        if (nodes.first.empty())
            return;

        buildTree(f_n, sum, mul, nodes);
    }
}

void minOptimize(ASTFunction * f_n)
{
    const auto * function_args = f_n->arguments->as<ASTExpressionList>();

    if (!function_args || function_args->children.size() != 1)
        throw Exception("Wrong number of arguments for function 'min' (" + toString(function_args->children.size()) + " instead of 1)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTFunction * inter_node = getInternalFunction(f_n);
    if (inter_node && inter_node->name == mul)
    {
        int tp = 1;
        std::pair<ASTs, ASTs> nodes = findAllConsts(f_n, mul);

        if (nodes.first.empty())
            return;

        for (const auto & arg : nodes.first)
        {
            Int128 num = applyVisitor(FieldVisitorConvertToNumber<Int128>(), arg->as<ASTLiteral>()->value);

            /// if multiplication is negative, min function becomes max

            if ((arg->as<ASTLiteral>()->value.getType() == Field::Types::Int64 ||
                 arg->as<ASTLiteral>()->value.getType() == Field::Types::Int128) && num < static_cast<Int128>(0))
                tp *= -1;
        }

        if (tp == -1)
            buildTree(f_n, max, mul, nodes);
        else
            buildTree(f_n, min, mul, nodes);
    }
    else if (inter_node && inter_node->name == plus)
    {
        std::pair<ASTs, ASTs> nodes = findAllConsts(f_n, plus);
        buildTree(f_n, min, plus, nodes);
    }
}

void maxOptimize(ASTFunction * f_n)
{
    const auto * function_args = f_n->arguments->as<ASTExpressionList>();

    if (!function_args || function_args->children.size() != 1)
        throw Exception("Wrong number of arguments for function 'max' (" + toString(function_args->children.size()) + " instead of 1)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTFunction * inter_node = getInternalFunction(f_n);
    if (inter_node && inter_node->name == mul)
    {
        int tp = 1;
        std::pair<ASTs, ASTs> nodes = findAllConsts(f_n, mul);

        if (nodes.first.empty())
            return;

        for (const auto & arg: nodes.first)
        {
            Int128 num = applyVisitor(FieldVisitorConvertToNumber<Int128>(), arg->as<ASTLiteral>()->value);

            /// if multiplication is negative, max function becomes min
            if ((arg->as<ASTLiteral>()->value.getType() == Field::Types::Int64 ||
                    arg->as<ASTLiteral>()->value.getType() == Field::Types::Int128) && num < static_cast<Int128>(0))
                tp *= -1;
        }

        if (tp == -1)
            buildTree(f_n, min, mul, nodes);
        else
            buildTree(f_n, max, mul, nodes);
    }
    else if (inter_node && inter_node->name == plus)
    {
        std::pair<ASTs, ASTs> nodes = findAllConsts(f_n, plus);
        buildTree(f_n, max, plus, nodes);
    }
}

/// optimize for min, max, sum is ready, ToDo: any, anyLast, groupBitAnd, groupBitOr, groupBitXor
void ArithmeticOperationsInAgrFuncMatcher::visit(ASTPtr & current_ast, Data & data)
{
    if (!current_ast)
        return;

    auto * function_node = current_ast->as<ASTFunction>();
    if (function_node && function_node->name == "sum")
        sumOptimize(function_node);
    else if (function_node && function_node->name == "min")
        minOptimize(function_node);
    else if (function_node && function_node->name == "max")
        maxOptimize(function_node);
    data.dont_know_why_I_need_it = true;
}

bool ArithmeticOperationsInAgrFuncMatcher::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    if (!child)
        return false;

    if (node->as<ASTTableExpression>() || node->as<ASTArrayJoin>())
        return false;

    return true;
}

}
