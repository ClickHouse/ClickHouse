#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <QueryCoordination/Optimizer/CalculateSortProp.h>
#include <QueryCoordination/Optimizer/DeriveOutputProp.h>


namespace DB
{

DeriveOutputProp::DeriveOutputProp(GroupNodePtr group_node_, const PhysicalProperties & required_prop_, const std::vector<PhysicalProperties> & children_prop_, ContextPtr context_)
    : group_node(group_node_), required_prop(required_prop_), children_prop(children_prop_), context(context_)
{
}

PhysicalProperties DeriveOutputProp::visit(QueryPlanStepPtr step)
{
    return Base::visit(step);
}

PhysicalProperties DeriveOutputProp::visitDefault(IQueryPlanStep & step)
{
    if (step.stepType() == StepType::Scan)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Step {} not implemented.", step.getName());

    PhysicalProperties res;
    res.distribution = children_prop[0].distribution;

    auto * transforming_step = dynamic_cast<ITransformingStep *>(group_node->getStep().get());
    if (transforming_step && transforming_step->getDataStreamTraits().preserves_sorting)
    {
        if (transforming_step->getOutputStream().sort_scope == DataStream::SortScope::Global)
            res.sort_description = transforming_step->getOutputStream().sort_description;
    }
    return res;
}

ExpressionActionsPtr buildShardingKeyExpression(const ASTPtr & sharding_key, ContextPtr context, const NamesAndTypesList & columns, bool project)
{
    ASTPtr query = sharding_key;
    auto syntax_result = TreeRewriter(context).analyze(query, columns);
    return ExpressionAnalyzer(query, syntax_result, context).getActions(project);
}

PhysicalProperties DeriveOutputProp::visit(ReadFromMergeTree & step)
{
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
    return {.distribution = children_prop[0].distribution, .sort_description = step.getSortDescription()};
}

PhysicalProperties DeriveOutputProp::visit(ExchangeDataStep & step)
{
    return {.distribution = step.getDistribution()};
}

PhysicalProperties DeriveOutputProp::visit(ExpressionStep & step)
{
    PhysicalProperties res;
    if (children_prop[0].distribution.type != PhysicalProperties::DistributionType::Hashed)
    {
        res.distribution = children_prop[0].distribution;
    }
    else
    {
        const auto & input_keys = children_prop[0].distribution.keys;
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
                return {.distribution = {.type = PhysicalProperties::DistributionType::Any}};
        }
    }

    if (step.getOutputStream().sort_scope == DataStream::SortScope::Global)
        res.sort_description = step.getOutputStream().sort_description;

//    CalculateSortProp sort_prop_calculator(step.getExpression(), children_prop[0].sort_prop);
//    res.sort_prop = sort_prop_calculator.calcSortProp();
    return res;
}

}
