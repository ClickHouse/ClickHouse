#include <string>
#include <Interpreters/GatherFunctionQuantileVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Mapping from quantile functions for single value to plural
static const std::unordered_map<String, String> quantile_fuse_name_mapping = {
    {NameQuantile::name, NameQuantiles::name},
    {NameQuantileBFloat16::name, NameQuantilesBFloat16::name},
    {NameQuantileBFloat16Weighted::name, NameQuantilesBFloat16Weighted::name},
    {NameQuantileDeterministic::name, NameQuantilesDeterministic::name},
    {NameQuantileExact::name, NameQuantilesExact::name},
    {NameQuantileExactExclusive::name, NameQuantilesExactExclusive::name},
    {NameQuantileExactHigh::name, NameQuantilesExactHigh::name},
    {NameQuantileExactInclusive::name, NameQuantilesExactInclusive::name},
    {NameQuantileExactLow::name, NameQuantilesExactLow::name},
    {NameQuantileExactWeighted::name, NameQuantilesExactWeighted::name},
    {NameQuantileTDigest::name, NameQuantilesTDigest::name},
    {NameQuantileTDigestWeighted::name, NameQuantilesTDigestWeighted::name},
    {NameQuantileTiming::name, NameQuantilesTiming::name},
    {NameQuantileTimingWeighted::name, NameQuantilesTimingWeighted::name},
};

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

    bool need_two_args = func->name == NameQuantileDeterministic::name
                         || func->name == NameQuantileExactWeighted::name
                         || func->name == NameQuantileTimingWeighted::name
                         || func->name == NameQuantileTDigestWeighted::name
                         || func->name == NameQuantileBFloat16Weighted::name;
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

