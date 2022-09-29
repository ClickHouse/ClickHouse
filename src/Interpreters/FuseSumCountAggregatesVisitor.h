#pragma once

#include <unordered_map>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

struct FuseSumCountAggregates
{
    std::vector<ASTFunction *> sums{};
    std::vector<ASTFunction *> counts{};
    std::vector<ASTFunction *> avgs{};

    void addFuncNode(ASTFunction * func)
    {
        if (func->name == "sum")
            sums.push_back(func);
        else if (func->name == "count")
            counts.push_back(func);
        else
        {
            assert(func->name == "avg");
            avgs.push_back(func);
        }
    }

    bool canBeFused() const
    {
        /// Need at least two different kinds of functions to fuse.
        if (sums.empty() && counts.empty())
            return false;
        if (sums.empty() && avgs.empty())
            return false;
        if (counts.empty() && avgs.empty())
            return false;
        return true;
    }
};

struct FuseSumCountAggregatesVisitorData
{
    using TypeToVisit = ASTFunction;

    std::unordered_map<String, FuseSumCountAggregates> fuse_map;

    void visit(ASTFunction & func, ASTPtr &)
    {
        if (func.name == "sum" || func.name == "avg" || func.name == "count")
        {
            if (func.arguments->children.empty())
                return;

            /// Probably we can extend it to match count() for non-nullable argument
            /// to sum/avg with any other argument. Now we require strict match.
            const auto argument = func.arguments->children.at(0)->getColumnName();
            auto it = fuse_map.find(argument);
            if (it != fuse_map.end())
            {
                it->second.addFuncNode(&func);
            }
            else
            {
                FuseSumCountAggregates funcs{};
                funcs.addFuncNode(&func);
                fuse_map[argument] = funcs;
            }
        }
    }

    /// Replaces one avg/sum/count function with an appropriate expression with sumCount().
    static void replaceWithSumCount(String column_name, ASTFunction & func)
    {
        auto func_base = makeASTFunction("sumCount", std::make_shared<ASTIdentifier>(column_name));
        auto exp_list = std::make_shared<ASTExpressionList>();
        if (func.name == "sum" || func.name == "count")
        {
            /// Rewrite "sum" to sumCount().1, rewrite "count" to sumCount().2
            UInt8 idx = (func.name == "sum" ? 1 : 2);
            func.name = "tupleElement";
            exp_list->children.push_back(func_base);
            exp_list->children.push_back(std::make_shared<ASTLiteral>(idx));
        }
        else
        {
            /// Rewrite "avg" to sumCount().1 / sumCount().2
            auto new_arg1 = makeASTFunction("tupleElement", func_base, std::make_shared<ASTLiteral>(UInt8(1)));
            auto new_arg2 = makeASTFunction(
                "CAST",
                makeASTFunction("tupleElement", func_base, std::make_shared<ASTLiteral>(static_cast<UInt8>(2))),
                std::make_shared<ASTLiteral>("Float64"));

            func.name = "divide";
            exp_list->children.push_back(new_arg1);
            exp_list->children.push_back(new_arg2);
        }
        func.arguments = exp_list;
        func.children.push_back(func.arguments);
    }
};

using FuseSumCountAggregatesVisitor = InDepthNodeVisitor<OneTypeMatcher<FuseSumCountAggregatesVisitorData>, true>;

}
