#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <AggregateFunctions/AggregateFunctionQuantile.h>

namespace DB
{

/// Gather all the `quantile*` functions
class GatherFunctionQuantileData
{
public:
    struct FuseQuantileAggregatesData
    {
        std::unordered_map<String, std::vector<ASTPtr *>> arg_map_function;

        void addFuncNode(ASTPtr & ast);
    };

    using TypeToVisit = ASTFunction;

    std::unordered_map<String, FuseQuantileAggregatesData> fuse_quantile;

    void visit(ASTFunction & function, ASTPtr & ast);

    static String getFusedName(const String & func_name);

    static bool needChild(const ASTPtr & node, const ASTPtr &);
};

using GatherFunctionQuantileVisitor = InDepthNodeVisitor<OneTypeMatcher<GatherFunctionQuantileData, GatherFunctionQuantileData::needChild>, true>;

}
