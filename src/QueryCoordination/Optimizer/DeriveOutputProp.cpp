#include <QueryCoordination/Optimizer/DeriveOutputProp.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>


namespace DB
{

DeriveOutputProp::DeriveOutputProp(const PhysicalProperties & required_prop_, const std::vector<PhysicalProperties> & children_prop_, ContextPtr context_)
    : required_prop(required_prop_), children_prop(children_prop_), context(context_)
{
}

PhysicalProperties DeriveOutputProp::visit(QueryPlanStepPtr step)
{
    return Base::visit(step);
}

PhysicalProperties DeriveOutputProp::visitDefault()
{
    return {.distribution = children_prop[0].distribution, .sort_description = children_prop[0].sort_description};
}

ExpressionActionsPtr buildShardingKeyExpression(const ASTPtr & sharding_key, ContextPtr context, const NamesAndTypesList & columns, bool project)
{
    ASTPtr query = sharding_key;
    auto syntax_result = TreeRewriter(context).analyze(query, columns);
    return ExpressionAnalyzer(query, syntax_result, context).getActions(project);
}

PhysicalProperties DeriveOutputProp::visit(ReadFromMergeTree & step)
{
    //    TODO sort_description by pk ?
    if (!context->getSettings().optimize_query_coordination_sharding_key)
        return PhysicalProperties{.distribution = {.type = PhysicalProperties::DistributionType::Any}};

    const auto & meta_info = context->getQueryCoordinationMetaInfo();
    const auto & storage_id = step.getStorageID();

    auto table_it = std::find(meta_info.storages.begin(), meta_info.storages.end(), storage_id);
    if (table_it == meta_info.storages.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found {}.{} in {}", storage_id.database_name, storage_id.table_name, meta_info.toString());

    /// distribute by any integer type value. TODO need to distinguish which functions have a clear distribution of data. e.g rand() is not clear, hash is clear and single column is clear
    /// TODO Enabling optimize_query_coordination_sharding_key may cause incorrect results, such as sharding key is cityHash64(xid + zid), which is not sharding with xid and zid.
    size_t table_idx = std::distance(meta_info.storages.begin(), table_it);
    const String & sharding_key = meta_info.sharding_keys[table_idx];

    if (sharding_key.empty())
        return PhysicalProperties{.distribution = {.type = PhysicalProperties::DistributionType::Any}};

    const char * begin = sharding_key.data();
    const char * end = begin + sharding_key.size();

    ParserExpression expression_parser;
    ASTPtr expression = parseQuery(expression_parser, begin, end, "expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    ExpressionActionsPtr sharding_key_expr = buildShardingKeyExpression(expression, context, step.getStorageMetadata()->columns.getAllPhysical(), false);

    if (sharding_key_expr->getRequiredColumns().empty())
        return PhysicalProperties{.distribution = {.type = PhysicalProperties::DistributionType::Any}};

    /// Suppose the columns is combined hash
    const auto & output_names = step.getOutputStream().header.getNames();
    for (const auto & key : sharding_key_expr->getRequiredColumns())
    {
        if (std::count(output_names.begin(), output_names.end(), key) != 1)
            return PhysicalProperties{.distribution = {.type = PhysicalProperties::DistributionType::Any}};
    }

    PhysicalProperties res;
    res.distribution.type = PhysicalProperties::DistributionType::Hashed;
    res.distribution.keys = sharding_key_expr->getRequiredColumns();
    return res;
}

PhysicalProperties DeriveOutputProp::visit(SortingStep & step)
{
    const auto & sort_description = step.getSortDescription();
    PhysicalProperties properties{.distribution = children_prop[0].distribution, .sort_description = sort_description};
    return properties;
}

PhysicalProperties DeriveOutputProp::visit(ExchangeDataStep & step)
{
    return {.distribution = step.getDistribution(), .sort_description = step.getSortDescription()};
}

PhysicalProperties DeriveOutputProp::visit(ExpressionStep & step)
{
    if (children_prop[0].distribution.type != PhysicalProperties::DistributionType::Hashed)
        return {.distribution = children_prop[0].distribution, .sort_description = children_prop[0].sort_description};

    /// handle alias
    const auto & input_keys = children_prop[0].distribution.keys;

    PhysicalProperties res;
    res.distribution.type = PhysicalProperties::DistributionType::Hashed;
    res.distribution.keys.resize(input_keys.size());

    FindAliasForInputName alias_finder(step.getExpression());
    for (size_t i = 0, s = input_keys.size(); i < s; ++i)
    {
        String alias;
        const auto & original_column = input_keys[i];
        const auto * alias_node = alias_finder.find(original_column);
        if (alias_node)
            res.distribution.keys[i] = alias_node->result_name;
        else
            res.distribution.keys[i] = original_column;
    }

    res.sort_description = children_prop[0].sort_description;
    return res;
}

}
