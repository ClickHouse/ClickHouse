#include <QueryCoordination/Optimizer/DeriveRequiredChildProp.h>


namespace DB
{

void setSortProp(QueryPlanStepPtr step, SortProp & required_sort_prop, size_t child_index = 0)
{
    if (child_index >= step->getInputStreams().size())
        return;

    /// There are some cases where sort desc is misaligned with the header, and in this case it is not required to keep the sort prop.
    /// E.g select * from aaa_all where name in (select name from bbb_all where name like '%d%') order by id limit 13 SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1;
    /// CreatingSetsStep header is id_0, name_1 but it's sort desc is id
    const auto & header = step->getInputStreams()[child_index].header;
    for (const auto & sort_column : step->getInputStreams()[child_index].sort_description)
    {
        if (!header.has(sort_column.column_name))
            return;
    }

    if (!step->getInputStreams()[child_index].sort_description.empty())
    {
        required_sort_prop.sort_description = step->getInputStreams()[child_index].sort_description;
        required_sort_prop.sort_scope = step->getInputStreams()[child_index].sort_scope;
    }

}

AlternativeChildrenProp DeriveRequiredChildProp::visit(QueryPlanStepPtr step)
{
    if (group_node->hasRequiredChildrenProp())
    {
        return group_node->getRequiredChildrenProp();
    }

    return Base::visit(step);
}

AlternativeChildrenProp DeriveRequiredChildProp::visitDefault(IQueryPlanStep & step)
{
    if (step.stepType() == StepType::Scan)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Step {} not implemented", step.getName());

    auto * transforming_step = dynamic_cast<ITransformingStep *>(group_node->getStep().get());

    std::vector<PhysicalProperties> required_child_prop;
    for (size_t i = 0; i < group_node->childSize(); ++i)
    {
        SortProp required_child_sort_prop;
        /// If the transform preserves_sorting and the output is ordered, the child is required to be ordered.
        if (transforming_step && transforming_step->getDataStreamTraits().preserves_sorting)
            setSortProp(group_node->getStep(), required_child_sort_prop, i);

        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}, .sort_prop = required_child_sort_prop});
    }
    return {required_child_prop};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(ReadFromMergeTree & /*step*/)
{
    AlternativeChildrenProp res;
    res.emplace_back();
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(AggregatingStep & step)
{
    AlternativeChildrenProp res;

    if (!step.isPreliminaryAgg())
    {
        std::vector<PhysicalProperties> required_singleton_prop;
        required_singleton_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
        res.emplace_back(required_singleton_prop);

        std::vector<PhysicalProperties> required_hashed_prop;
        PhysicalProperties hashed_prop;
        hashed_prop.distribution.type = PhysicalProperties::DistributionType::Hashed;
        hashed_prop.distribution.keys = step.getParams().keys;
        required_hashed_prop.push_back(hashed_prop);
        res.emplace_back(required_hashed_prop);
    }
    else
    {
        std::vector<PhysicalProperties> required_child_prop;
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
        res.emplace_back(required_child_prop);
    }
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(MergingAggregatedStep & step)
{
    AlternativeChildrenProp res;
    std::vector<PhysicalProperties> required_child_prop;

    if (step.getParams().keys.empty() || !step.isFinal())
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}});
    }
    else
    {
        PhysicalProperties hashed_prop;
        hashed_prop.distribution.type = PhysicalProperties::DistributionType::Hashed;
        hashed_prop.distribution.distribution_by_buket_num = true;
        required_child_prop.push_back(hashed_prop);
    }

    res.emplace_back(required_child_prop);
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(ExpressionStep & step)
{
    SortProp required_sort_prop;
    if (step.getDataStreamTraits().preserves_sorting)
        setSortProp(group_node->getStep(), required_sort_prop);

    PhysicalProperties required_child_prop;
    required_child_prop.sort_prop = required_sort_prop;
    required_child_prop.distribution = {.type = PhysicalProperties::DistributionType::Any};
    return {{required_child_prop}};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(FilterStep & /*step*/)
{
    SortProp required_sort_prop;
    setSortProp(group_node->getStep(), required_sort_prop);

    PhysicalProperties required_child_prop;
    required_child_prop.sort_prop = required_sort_prop;
    required_child_prop.distribution = {.type = PhysicalProperties::DistributionType::Any};
    return {{required_child_prop}};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(TopNStep & step)
{
    PhysicalProperties required_child_prop;
    if (step.sortType() == SortingStep::Type::FinishSorting)
    {
        required_child_prop.sort_prop.sort_scope = DataStream::SortScope::Stream;
        required_child_prop.sort_prop.sort_description = step.getPrefixDescription();
    }
    else if (step.sortType() == SortingStep::Type::MergingSorted)
    {
        required_child_prop.sort_prop.sort_scope = DataStream::SortScope::Stream;
        required_child_prop.sort_prop.sort_description = step.getInputStreams().front().sort_description; /// step.getSortDescription(); column name may alias
    }

    if (step.getPhase() == TopNStep::Phase::Preliminary)
    {
        required_child_prop.distribution = {.type = PhysicalProperties::DistributionType::Any};
    }
    else
    {
        required_child_prop.distribution = {.type = PhysicalProperties::DistributionType::Singleton};
    }
    return {{required_child_prop}};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(SortingStep & step)
{
    PhysicalProperties required_child_prop;
    if (step.getType() == SortingStep::Type::FinishSorting)
    {
        required_child_prop.sort_prop.sort_scope = DataStream::SortScope::Stream;
        required_child_prop.sort_prop.sort_description = step.getPrefixDescription();
    }
    else if (step.getType() == SortingStep::Type::MergingSorted)
    {
        required_child_prop.sort_prop.sort_scope = DataStream::SortScope::Stream;
        required_child_prop.sort_prop.sort_description = step.getInputStreams().front().sort_description;
    }
    if (step.getPhase() == SortingStep::Phase::Preliminary)
    {
        required_child_prop.distribution = {.type = PhysicalProperties::DistributionType::Any};
    }
    else
    {
        required_child_prop.distribution = {.type = PhysicalProperties::DistributionType::Singleton};
    }
    return {{required_child_prop}};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(DistinctStep & step)
{
    PhysicalProperties required_child_prop;
    if (step.optimizeDistinctInOrder())
    {
        const SortDescription distinct_sort_desc = step.getSortDescription();
        if (!distinct_sort_desc.empty())
        {
            required_child_prop.sort_prop.sort_scope = DataStream::SortScope::Stream;
            required_child_prop.sort_prop.sort_description = distinct_sort_desc;
        }
    }

    required_child_prop.distribution = {.type = PhysicalProperties::DistributionType::Singleton};

//    if (step.isPreliminary())
//    {
//        required_child_prop.distribution = {.type = PhysicalProperties::DistributionType::Any};
//    }
//    else
//    {
//        required_child_prop.distribution = {.type = PhysicalProperties::DistributionType::Singleton};
//    }
    return {{required_child_prop}};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(LimitStep & step)
{
    SortProp required_sort_prop;
    setSortProp(group_node->getStep(), required_sort_prop);

    AlternativeChildrenProp res;
    std::vector<PhysicalProperties> required_child_prop;

    if (step.getPhase() == LimitStep::Phase::Preliminary)
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}, .sort_prop = required_sort_prop});
    }
    else
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}, .sort_prop = required_sort_prop});
    }

    res.emplace_back(required_child_prop);
    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(JoinStep & step)
{
    AlternativeChildrenProp res;

    /// broadcast join
    std::vector<PhysicalProperties> broadcast_join_properties;
    broadcast_join_properties.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    broadcast_join_properties.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Replicated}});
    res.emplace_back(broadcast_join_properties);

    /// shuffle join
    JoinPtr join = step.getJoin();

    if (join->pipelineType() != JoinPipelineType::FillRightFirst)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support join pipeline type, please specify the join algorithm as hash or parallel_hash or grace_hash");

    const TableJoin & table_join = join->getTableJoin();
    if (table_join.getClauses().size() == 1 && table_join.strictness() != JoinStrictness::Asof) /// broadcast join. Asof support != condition
    {
        auto join_clause = table_join.getOnlyClause(); /// must be equals condition
        std::vector<PhysicalProperties> shaffle_join_prop;
        shaffle_join_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed, .keys = join_clause.key_names_left}});
        shaffle_join_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Hashed, .keys = join_clause.key_names_right}});
        res.emplace_back(shaffle_join_prop);
    }

    return res;
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(ExchangeDataStep & /*step*/)
{
    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    return {required_child_prop};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(CreatingSetStep & /*step*/)
{
    std::vector<PhysicalProperties> required_child_prop;
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Replicated}});
    return {required_child_prop};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(CreatingSetsStep & step)
{
    bool all_column_aligned = true;
    const auto & header = step.getInputStreams().front().header;
    for (const auto & sort_column : step.getInputStreams().front().sort_description)
    {
        if (!header.has(sort_column.column_name))
        {
            all_column_aligned = false;
            break;
        }
    }

    SortProp required_sort_prop;
    if (all_column_aligned && !step.getInputStreams().front().sort_description.empty())
    {
        required_sort_prop.sort_description = step.getInputStreams().front().sort_description;
        required_sort_prop.sort_scope = step.getInputStreams().front().sort_scope;
    }

    std::vector<PhysicalProperties> required_child_prop;

    /// Ensure that CreatingSetsStep and the left table scan are assigned to the same fragment.
    required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}, .sort_prop = required_sort_prop});
    for (size_t i = 1; i < group_node->childSize(); ++i)
    {
        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Any}});
    }
    return {required_child_prop};
}

AlternativeChildrenProp DeriveRequiredChildProp::visit(UnionStep & /*step*/)
{
    std::vector<PhysicalProperties> required_child_prop;
    for (size_t i = 0; i < group_node->childSize(); ++i)
    {
        SortProp required_child_sort_prop;
        setSortProp(group_node->getStep(), required_child_sort_prop, i);

        required_child_prop.push_back({.distribution = {.type = PhysicalProperties::DistributionType::Singleton}, .sort_prop = required_child_sort_prop});
    }
    return {required_child_prop};
}

}
