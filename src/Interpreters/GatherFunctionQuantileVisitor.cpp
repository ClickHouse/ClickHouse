#include <string>
#include <Interpreters/GatherFunctionQuantileVisitor.h>
#include <Common/Exception.h>
#include <common/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
    if (!func)
        return;

    const auto & arguments = func->arguments->children;

    bool need_two_args = func->name == NameQuantileDeterministic::name
                         || func->name == NameQuantileExactWeighted::name
                         || func->name == NameQuantileTimingWeighted::name
                         || func->name == NameQuantileTDigestWeighted::name;
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

}

