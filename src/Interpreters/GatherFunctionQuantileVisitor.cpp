#include <Interpreters/GatherFunctionQuantileVisitor.h>

#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Mapping from quantile functions for single value to plural
static const std::unordered_map<String, String> quantile_fuse_name_mapping =
{
    {"quantile", "quantiles"},
    {"quantileBFloat16", "quantilesBFloat16"},
    {"quantileBFloat16Weighted", "quantilesBFloat16Weighted"},
    {"quantileDeterministic", "quantilesDeterministic"},
    {"quantileExact", "quantilesExact"},
    {"quantileExactExclusive", "quantilesExactExclusive"},
    {"quantileExactHigh", "quantilesExactHigh"},
    {"quantileExactInclusive", "quantilesExactInclusive"},
    {"quantileExactLow", "quantilesExactLow"},
    {"quantileExactWeighted", "quantilesExactWeighted"},
    {"quantileInterpolatedWeighted", "quantilesInterpolatedWeighted"},
    {"quantileTDigest", "quantilesTDigest"},
    {"quantileTDigestWeighted", "quantilesTDigestWeighted"},
    {"quantileTiming", "quantilesTiming"},
    {"quantileTimingWeighted", "quantilesTimingWeighted"},
    {"quantileGK", "quantilesGK"},
};

String GatherFunctionQuantileData::toFusedNameOrSelf(const String & func_name)
{
    if (auto it = quantile_fuse_name_mapping.find(func_name); it != quantile_fuse_name_mapping.end())
        return it->second;
    return func_name;
}

String GatherFunctionQuantileData::getFusedName(const String & func_name)
{
    if (auto it = quantile_fuse_name_mapping.find(func_name); it != quantile_fuse_name_mapping.end())
        return it->second;
    throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Function '{}' is not quantile-family or cannot be fused", func_name);
}

void GatherFunctionQuantileData::visit(ASTFunction & function, ASTPtr & ast)
{
    if (!quantile_fuse_name_mapping.contains(function.name))
        return;

    fuse_quantile[function.name].addFuncNode(ast);
}

void GatherFunctionQuantileData::FuseQuantileAggregatesData::addFuncNode(ASTPtr & ast)
{
    const auto * func = ast->as<ASTFunction>();
    if (!func || func->parameters == nullptr)
        return;

    const auto & arguments = func->arguments->children;


    bool need_two_args = func->name == "quantileDeterministic" || func->name == "quantileExactWeighted"
        || func->name == "quantileInterpolatedWeighted" || func->name == "quantileTimingWeighted"
        || func->name == "quantileTDigestWeighted" || func->name == "quantileBFloat16Weighted";

    if (arguments.size() != (need_two_args ? 2 : 1))
        return;

    if (arguments[0]->getColumnName().find(',') != std::string::npos)
        return;
    String arg_name = arguments[0]->getColumnName();
    if (need_two_args)
    {
        if (arguments[1]->getColumnName().find(',') != std::string::npos)
            return;
        arg_name += "," + arguments[1]->getColumnName();
    }

    arg_map_function[arg_name].push_back(&ast);
}

bool GatherFunctionQuantileData::needChild(const ASTPtr & node, const ASTPtr &)
{
    /// Skip children of quantile* functions to escape cycles in further processing
    if (const auto * func = node ? node->as<ASTFunction>() : nullptr)
        return !quantile_fuse_name_mapping.contains(func->name);
    return true;
}

}
