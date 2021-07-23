#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <AggregateFunctions/AggregateFunctionQuantile.h>

namespace DB
{
static const std::unordered_map<String, String> quantile_fuse_name_mapping = {
    {NameQuantile::name, NameQuantiles::name},
    {NameQuantileDeterministic::name, NameQuantilesDeterministic::name},
    {NameQuantileExact::name, NameQuantilesExact::name},
    {NameQuantileExactLow::name, NameQuantilesExactLow::name},
    {NameQuantileExactHigh::name, NameQuantilesExactHigh::name},
    {NameQuantileExactExclusive::name, NameQuantilesExactExclusive::name},
    {NameQuantileExactInclusive::name, NameQuantilesExactInclusive::name},
    {NameQuantileExactWeighted::name, NameQuantilesExactWeighted::name},
    {NameQuantileTiming::name, NameQuantilesTiming::name},
    {NameQuantileTimingWeighted::name, NameQuantilesTimingWeighted::name},
    {NameQuantileTDigest::name, NameQuantilesTDigest::name},
    {NameQuantileTDigestWeighted::name, NameQuantilesTDigestWeighted::name},
    {NameQuantileBFloat16::name, NameQuantilesBFloat16::name}
};

/// Gather all the quantilexxx functions
class GatherFunctionQuantileData
{
public:
    struct FuseQuantileAggregatesData
    {
        std::unordered_map<String, std::vector<ASTFunction *>> arg_map_function;
        void addFuncNode(ASTFunction * func)
        {
            auto argument = func->arguments->children.at(0)->getColumnName();

            /// This functions needs two arguments.
            if (func->name == NameQuantileDeterministic::name
                || func->name == NameQuantileExactWeighted::name
                || func->name == NameQuantileTimingWeighted::name
                || func->name == NameQuantileTDigestWeighted::name)
                argument = argument + "," + func->arguments->children.at(1)->getColumnName();

            auto it = arg_map_function.find(argument);
            if (it != arg_map_function.end())
            {
                it->second.push_back(func);
            }
            else
            {
                std::vector<ASTFunction *> new_func_list;
                new_func_list.push_back(func);
                arg_map_function[argument] = new_func_list;
            }
        }
    };

    using TypeToVisit = ASTFunction;
    std::unordered_map<String, FuseQuantileAggregatesData> fuse_quantile;
    void visit(ASTFunction & function, ASTPtr &)
    {
        if (quantile_fuse_name_mapping.find(function.name) == quantile_fuse_name_mapping.end())
            return;

        auto it = fuse_quantile.find(function.name);
        if (it != fuse_quantile.end())
        {
            it->second.addFuncNode(&function);
        }
        else
        {
            FuseQuantileAggregatesData funcs{};
            funcs.addFuncNode(&function);
            fuse_quantile[function.name] = funcs;
        }
    }
};

using GatherFunctionQuantileVisitor = InDepthNodeVisitor<OneTypeMatcher<GatherFunctionQuantileData>, true>;

}
