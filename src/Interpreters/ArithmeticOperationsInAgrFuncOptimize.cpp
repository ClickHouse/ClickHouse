#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/ArithmeticOperationsInAgrFuncOptimize.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

bool isConstantField(const Field & field)
{
    return field.getType() == Field::Types::Int64 ||
           field.getType() == Field::Types::UInt64 ||
           field.getType() == Field::Types::Int128 ||
           field.getType() == Field::Types::UInt128;
}

/// scalar values from the first level
std::pair<ASTs, ASTs> tryGetConst(std::string & name, ASTs & arguments)
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

    if ((name == "plus" || name == "multiply") && const_num.size() + not_const.size() != 2)
    {
        throw Exception("Wrong number of arguments for function 'plus' or 'multiply' (" + toString(const_num.size() + not_const.size()) + " instead of 2)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    return {const_num, not_const};
}

std::pair<ASTs, ASTs> findAllConsts(ASTFunction * func_node, std::string & inter_func_name)
{
    if (!func_node->arguments)
        return {};

    if (!(func_node->arguments->children[0]->as<ASTFunction>()) && (func_node->arguments->children.size() == 2 &&
        !(func_node->arguments->children[1]->as<ASTFunction>())))
    {
        auto arg = func_node->arguments->children;
        std::pair<ASTs, ASTs> fl = tryGetConst(func_node->name, arg);
        return fl;
    }
    else if ((func_node->arguments->children[0]->as<ASTFunction>() && inter_func_name != func_node->arguments->children[0]->as<ASTFunction>()->name) ||
            (func_node->arguments->children.size() == 2 && func_node->arguments->children[1]->as<ASTFunction>() &&
            inter_func_name != func_node->arguments->children[1]->as<ASTFunction>()->name))
    {
        if (func_node->arguments->children[0]->as<ASTFunction>())
            return {{func_node->arguments->children[1]}, {func_node->arguments->children[0]}};
        else
            return {{func_node->arguments->children[0]}, {func_node->arguments->children[1]}};
    }
    else
    {
        std::pair<ASTs, ASTs> fl = tryGetConst(func_node->name, func_node->arguments->children);
        if (!fl.second[0]->as<ASTFunction>())
        {
            std::pair<ASTs, ASTs> ans = {fl.first, fl.second};
            return ans;
        }

        std::pair<ASTs, ASTs> ans = findAllConsts(fl.second[0]->as<ASTFunction>(), inter_func_name);

        if (fl.first.size() == 1)
        {
            if (!fl.second[0]->as<ASTFunction>())
                ans.second.push_back(fl.second[0]);

            ans.first.push_back(fl.first[0]);
        }
        else if (fl.first.empty())
        {
            if (!fl.second[0]->as<ASTFunction>() || fl.second[0]->as<ASTFunction>()->name != inter_func_name)
                ans.second.push_back(fl.second[0]);

            if (fl.second.size() == 2 && (!fl.second[1]->as<ASTFunction>() || fl.second[1]->as<ASTFunction>()->name != inter_func_name))
                ans.second.push_back(fl.second[1]);
        }
        else
            throw Exception("did not expect that", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
        return ans;
    }
}

/// rebuilds tree, all scalar values now outside the main func
void buildTree(ASTFunction * old_tree, std::string& func_name, std::string& intro_func, std::pair<ASTs, ASTs> & tree_comp, std::pair<int, std::string> & flag)
{
    ASTFunction copy = *old_tree;
    ASTs cons_val = tree_comp.first;
    ASTs non_cons = tree_comp.second;

    old_tree->name = intro_func;
    for (auto & i : cons_val)
    {
        old_tree->arguments->children = {};
        old_tree->arguments->children.push_back(i);

        old_tree->arguments->children.push_back(makeASTFunction(intro_func));
        old_tree = old_tree->arguments->children[1]->as<ASTFunction>();
    }

    if (flag.first == -1)
        old_tree->name = flag.second;
    else
        old_tree->name = func_name;

    if (non_cons.empty())
        throw Exception("Aggregate function " + func_name + " requires single argument",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (non_cons.size() == 1)
        old_tree->arguments->children.push_back(non_cons[0]);
    else
    {
        size_t i = 0;

        while (i < non_cons.size() - 2)
        {
            old_tree->arguments->children = {};
            old_tree->children.push_back(non_cons[i]);

            old_tree->arguments->children.push_back(makeASTFunction(intro_func));
            old_tree = old_tree->arguments->children[1]->as<ASTFunction>();
            ++i;
        }
        old_tree->arguments->children = {non_cons[i], non_cons[i + 1]};
    }
}

void sumOptimize(ASTFunction * f_n)
{
    std::string sum = "sum";
    std::string mul = "multiply";

    const auto * function_args = f_n->arguments->as<ASTExpressionList>();

    if (!function_args || function_args->children.size() != 1)
        throw Exception("Wrong number of arguments for function 'sum' (" + toString(function_args->children.size()) + " instead of 1)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto * inter_node = f_n->arguments->children[0]->as<ASTFunction>();
    if (inter_node && inter_node->name == mul)
    {
        std::pair<ASTs, ASTs> nodes = findAllConsts(f_n, mul);

        if (nodes.first.empty())
            return;

        std::pair<int, std::string> cur = {1, "have no opposite func"};
        buildTree(f_n, sum, mul, nodes, cur);
    }
}

void minOptimize(ASTFunction * f_n)
{
    std::string min = "min";
    std::string max = "max";
    std::string mul = "multiply";
    std::string plus = "plus";

    const auto * function_args = f_n->arguments->as<ASTExpressionList>();

    if (!function_args || function_args->children.size() != 1)
        throw Exception("Wrong number of arguments for function 'min' (" + toString(function_args->children.size()) + " instead of 1)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto * inter_node = f_n->arguments->children[0]->as<ASTFunction>();
    if (inter_node && inter_node->name == mul)
    {
        int tp = 1;
        std::pair<ASTs, ASTs> nodes = findAllConsts(f_n, mul);

        if (nodes.first.empty())
            return;

        for (const auto & arg : nodes.first)
        {
            auto num = arg->as<ASTLiteral>()->value.get<Int128>();

            /// if multiplication is negative, min function becomes max

            if ((arg->as<ASTLiteral>()->value.getType() == Field::Types::Int64 ||
                 arg->as<ASTLiteral>()->value.getType() == Field::Types::Int128) && static_cast<int64_t>(num) < 0)
                tp *= -1;
        }

        std::pair<int, std::string> cur = {tp, max};
        buildTree(f_n, min, mul, nodes, cur);
    }
    else if (inter_node && inter_node->name == plus)
    {
        std::pair<ASTs, ASTs> nodes = findAllConsts(f_n, plus);
        std::pair<int, std::string> cur = {1, min};
        buildTree(f_n, min, plus, nodes, cur);
    }
}

void maxOptimize(ASTFunction * f_n)
{
    std::string max = "max";
    std::string min = "min";
    std::string mul = "multiply";
    std::string plus = "plus";

    const auto * function_args = f_n->arguments->as<ASTExpressionList>();

    if (!function_args || function_args->children.size() != 1)
        throw Exception("Wrong number of arguments for function 'max' (" + toString(function_args->children.size()) + " instead of 1)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    auto * inter_node = f_n->arguments->children[0]->as<ASTFunction>();
    if (inter_node && inter_node->name == mul)
    {
        int tp = 1;
        std::pair<ASTs, ASTs> nodes = findAllConsts(f_n, mul);

        if (nodes.first.empty())
            return;

        for (const auto &ar: nodes.first)
        {
            auto num = ar->as<ASTLiteral>()->value.get<Int128>();

            /// if multiplication is negative, max function becomes min
            if ((ar->as<ASTLiteral>()->value.getType() == Field::Types::Int64 ||
                    ar->as<ASTLiteral>()->value.getType() == Field::Types::Int128) && static_cast<int64_t>(num) < 0)
                tp *= -1;
        }

        std::pair<int, std::string> cur = {tp, min};
        buildTree(f_n, max, mul, nodes, cur);
    }
    else if (inter_node && inter_node->name == plus)
    {
        std::pair<ASTs, ASTs> nodes = findAllConsts(f_n, plus);
        std::pair<int, std::string> cur = {1, max};
        buildTree(f_n, max, plus, nodes, cur);
    }
}

/// optimize for min, max, sum is ready, ToDo: any, anyLast, groupBitAnd, groupBitOr, groupBitXor
void ArithmeticOperationsInAgrFuncVisitor::visit(ASTPtr & current_ast)
{
    if (!current_ast)
        return;

    for (ASTPtr & child : current_ast->children)
    {
        auto * function_node = child->as<ASTFunction>();

        if (!function_node || !(function_node->name == "sum" || function_node->name == "min" || function_node->name == "max"))
        {
            visit(child);
            continue;
        }

        if (function_node->name == "sum")
            sumOptimize(function_node);
        else if (function_node->name == "min")
            minOptimize(function_node);
        else if (function_node->name == "max")
            maxOptimize(function_node);
    }
}

}
