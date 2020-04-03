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
///scalar values from the first level
std::pair<ASTs, ASTs> tryToCatchConst(ASTs& argss)
{
    ASTs const_num;
    ASTs not_const;

    for (const auto &ar: argss)
    {
        if (const auto * literal = ar->as<ASTLiteral>())
        {
            if (literal->value.getType() == Field::Types::Int64 ||
            literal->value.getType() == Field::Types::UInt64 ||
            literal->value.getType() == Field::Types::Int128 ||
            literal->value.getType() == Field::Types::UInt128)
            {
                const_num.push_back(ar);
            }
            else
            {
                not_const.push_back(ar);
            }
        }
        else
        {
            not_const.push_back(ar);
        }
    }

    return {const_num, not_const};
}

std::pair<ASTs, ASTs> findAllConsts(ASTFunction * func_node, std::string& inter_func_name)
{
    if (!func_node->arguments)
    {
        return {};
    }
    else if ( !(func_node->arguments->children[0]->as<ASTFunction>()))
    {
        auto arg = func_node->arguments->children;
        std::pair<ASTs, ASTs> it = tryToCatchConst(arg);
        return it;
    }
    else if (inter_func_name != func_node->arguments->children[0]->as<ASTFunction>()->name)
    {
        return {};
    }
    else
    {
        std::pair<ASTs, ASTs> it = tryToCatchConst(func_node->arguments->children);
        if (!it.second[0]->as<ASTFunction>())
        {
            std::pair<ASTs, ASTs> ans = {it.first, it.second};
            return ans;
        }
        else
        {
            std::pair<ASTs, ASTs> ans = findAllConsts(it.second[0]->as<ASTFunction>(), inter_func_name);
            if (it.first.size() == 1)
            {
                if (!it.second[0]->as<ASTFunction>())
                {
                    ans.second.push_back(it.second[0]);
                }
                ans.first.push_back(it.first[0]);
            }
            else if (it.first.empty())
            {
                if (!it.second[0]->as<ASTFunction>() || it.second[0]->as<ASTFunction>()->name != inter_func_name)
                {
                    ans.second.push_back(it.second[0]);
                }
                if (it.second.size() == 2 && (!it.second[1]->as<ASTFunction>() || it.second[1]->as<ASTFunction>()->name != inter_func_name))
                {
                    ans.second.push_back(it.second[1]); // может быть бывают случаи когда индитификатор стоит на 0 позиции, но я таких не нашел
                }
            }
            else
            {
                std::cerr << "did not expect that";
            }
            return ans;
        }
    }

}

///rebuilds tree, all scalar values now outside the main func
void buildTree(ASTFunction * old_tree, std::string& func_name, std::string& intro_func, std::pair<ASTs, ASTs>& tree_comp, std::pair<int, std::string> flag)
{
    ASTFunction copy = *old_tree;
    ASTs cons_val = tree_comp.first;
    ASTs non_cons = tree_comp.second;

    old_tree->name = intro_func;
    for (auto& i: cons_val)
    {
        old_tree->arguments->children = {};
        old_tree->arguments->children.push_back(i);

        old_tree->arguments->children.push_back(makeASTFunction(intro_func));
        old_tree = old_tree->arguments->children[1]->as<ASTFunction>();
    }

    if (flag.first == -1)
    {
        old_tree->name = flag.second;
    }
    else
    {
        old_tree->name = func_name;
    }

    if (non_cons.empty())
    {
        old_tree = &copy;
    }
    else if (non_cons.size() == 1)
    {
        old_tree->arguments->children.push_back(non_cons[0]);
    }
    else
    {
        size_t i = 0;

        while (i < non_cons.size() - 2)
        {
            ASTPtr empty;
            auto * function_node = empty->as<ASTFunction>();
            function_node->name = intro_func;

            old_tree->arguments->children = {};
            old_tree->children.push_back(non_cons[i]);

            old_tree->arguments->children.push_back(empty);
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

    auto * inter_node = f_n->arguments->children[0]->as<ASTFunction>();
    if (inter_node && inter_node->name == mul)
    {
        std::pair<ASTs, ASTs> it = findAllConsts(f_n, mul);

        if (it.first.empty())
            return;

        buildTree(f_n, sum, mul, it, {1, "have no opposite func"});
    }
    else
    {
        return;
    }
}

void minOptimize(ASTFunction * f_n)
{
    std::string min = "min";
    std::string max = "max";
    std::string mul = "multiply";
    std::string plus = "plus";

    auto * inter_node = f_n->arguments->children[0]->as<ASTFunction>();
    if (inter_node && inter_node->name == mul)
    {
        int tp = 1;
        std::pair<ASTs, ASTs> it = findAllConsts(f_n, mul);

        if (it.first.empty())
            return;

        for (const auto &ar: it.first)
        {
            auto num = ar->as<ASTLiteral>()->value.get<Int128>();

            /// if multiplication is negative, min function becomes max

            if (num < 0)
                tp *= -1;
        }

        buildTree(f_n, min, mul, it, {tp, max});
    }
    else if (inter_node && inter_node->name == plus)
    {
        std::pair<ASTs, ASTs> it = findAllConsts(f_n, plus);
        buildTree(f_n, min, plus, it, {1, min});
    }
    else
    {
        return;
    }
}

void maxOptimize(ASTFunction * f_n)
{
    std::string max = "max";
    std::string min = "min";
    std::string mul = "multiply";
    std::string plus = "plus";

    auto * inter_node = f_n->arguments->children[0]->as<ASTFunction>();
    if (inter_node && inter_node->name == mul)
    {
        int tp = 1;
        std::pair<ASTs, ASTs> it = findAllConsts(f_n, mul);

        if (it.first.empty())
            return;

        for (const auto &ar: it.first)
        {
            auto num = ar->as<ASTLiteral>()->value.get<Int128>();

            /// if multiplication is negative, max function becomes min

            if (num < 0)
                tp *= -1;
        }

        buildTree(f_n, max, mul, it, {tp, min});
    }
    else if (inter_node && inter_node->name == plus)
    {
        std::pair<ASTs, ASTs> it = findAllConsts(f_n, plus);
        buildTree(f_n, max, plus, it, {1, max});
    }
    else
    {
        return;
    }
}

///optimize for min, max, sum is ready, ToDo: any, anyLast, groupBitAnd, groupBitOr, groupBitXor
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
        {
            sumOptimize(function_node);
            return;
        }
        else if (function_node->name == "min")
        {
            minOptimize(function_node);
        }
        else if (function_node->name == "max")
        {
            maxOptimize(function_node);
        }
    }
}

}
