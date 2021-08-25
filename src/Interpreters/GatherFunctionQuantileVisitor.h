#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <AggregateFunctions/AggregateFunctionQuantile.h>

namespace DB
{
/// Mapping from quantile functions for single value to plural
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

/// Gather all the `quantile*` functions
class GatherFunctionQuantileData
{
public:
    struct FuseQuantileAggregatesData
    {
        std::unordered_map<String, std::vector<ASTPtr *>> arg_map_function;
        void addFuncNode(ASTPtr & ast)
        {
            const auto * func = ast->as<ASTFunction>();
            auto argument = func->arguments->children.at(0)->getColumnName();

            /// This functions needs two arguments.
            if (func->name == NameQuantileDeterministic::name
                || func->name == NameQuantileExactWeighted::name
                || func->name == NameQuantileTimingWeighted::name
                || func->name == NameQuantileTDigestWeighted::name)
                argument = argument + "," + func->arguments->children.at(1)->getColumnName();

            arg_map_function[argument].push_back(&ast);
        }
    };

    using TypeToVisit = ASTFunction;
    std::unordered_map<String, FuseQuantileAggregatesData> fuse_quantile;

    void visit(ASTFunction & function, ASTPtr & ast)
    {
        if (quantile_fuse_name_mapping.find(function.name) == quantile_fuse_name_mapping.end())
            return;

        fuse_quantile[function.name].addFuncNode(ast);
    }
};

using GatherFunctionQuantileVisitor = InDepthNodeVisitor<OneTypeMatcher<GatherFunctionQuantileData>, true>;

}
