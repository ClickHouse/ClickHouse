#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTFunction;

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

    static String toFusedNameOrSelf(const String & func_name);

    static String getFusedName(const String & func_name);

    static bool needChild(const ASTPtr & node, const ASTPtr &);
};

using GatherFunctionQuantileVisitor = InDepthNodeVisitor<OneTypeMatcher<GatherFunctionQuantileData, GatherFunctionQuantileData::needChild>, true>;

}
