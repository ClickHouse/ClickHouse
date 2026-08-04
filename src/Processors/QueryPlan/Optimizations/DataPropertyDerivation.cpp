#include <Processors/QueryPlan/Optimizations/DataPropertyDerivation.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <algorithm>

namespace DB::QueryPlanOptimizations
{
namespace
{

std::optional<ColumnSet>
mapColumnSet(const ColumnSet & columns, const Block & input_header, const std::vector<std::optional<PlanColumnRef>> & identity_outputs)
{
    ColumnSet mapped;
    mapped.reserve(columns.size());
    for (const auto & column : columns)
    {
        if (column.position >= input_header.columns() || input_header.getByPosition(column.position).name != column.name
            || column.position >= identity_outputs.size() || !identity_outputs[column.position])
            return std::nullopt;
        mapped.push_back(*identity_outputs[column.position]);
    }
    if (!normalizeColumnSet(mapped))
        return std::nullopt;
    return mapped;
}

DataPropertySet mapThroughActionsDAG(
    const ActionsDAG & actions,
    const Block & input_header,
    const Block & output_header,
    const DataPropertySet & input_properties,
    DataPropertyPreservingTransformationKind preservation_transformation)
{
    const auto & dag_inputs = actions.getInputs();
    const UniqueColumnPositionIndex input_header_index(input_header);
    std::vector<std::optional<size_t>> input_positions(dag_inputs.size());
    for (size_t dag_input_position = 0; dag_input_position < dag_inputs.size(); ++dag_input_position)
        input_positions[dag_input_position]
            = input_header_index.find(dag_inputs[dag_input_position]->result_name, *dag_inputs[dag_input_position]->result_type);

    const auto & dag_outputs = actions.getOutputs();
    const UniqueColumnPositionIndex dag_output_index(dag_outputs);
    std::vector<std::optional<size_t>> dag_output_positions(output_header.columns());
    for (size_t output_position = 0; output_position < output_header.columns(); ++output_position)
    {
        const auto & output_column = output_header.getByPosition(output_position);
        dag_output_positions[output_position] = dag_output_index.find(output_column.name, *output_column.type);
    }

    const auto traced = traceActionsDAGLineage(actions);
    std::vector<std::optional<PlanColumnRef>> identity_outputs(input_header.columns());
    DataPropertySet result;
    for (size_t output_position = 0; output_position < output_header.columns(); ++output_position)
    {
        const auto & output_column = output_header.getByPosition(output_position);
        if (!canContainNull(*output_column.type))
            result.addNonNullColumn({output_position, output_column.name});

        if (!dag_output_positions[output_position])
            continue;
        const auto & output_lineage = traced[*dag_output_positions[output_position]];
        if (!output_lineage || output_lineage->input_position >= input_positions.size() || !input_positions[output_lineage->input_position])
            continue;

        const size_t input_position = *input_positions[output_lineage->input_position];
        ColumnLineageKind kind = ColumnLineageKind::Unknown;
        DataPropertyTransformationKind lineage_transformation = DataPropertyTransformationKind::Identity;
        switch (output_lineage->kind)
        {
            case ActionsDAGLineageKind::Identity:
                kind = ColumnLineageKind::Identity;
                lineage_transformation = DataPropertyTransformationKind::Identity;
                break;
            case ActionsDAGLineageKind::ValuePreserving:
                kind = ColumnLineageKind::ValuePreserving;
                lineage_transformation = DataPropertyTransformationKind::ValuePreservingExpression;
                break;
            case ActionsDAGLineageKind::DistinctValuesBound:
                kind = ColumnLineageKind::NDVBound;
                lineage_transformation = DataPropertyTransformationKind::NDVBoundExpression;
                break;
        }

        const PlanColumnRef output_ref{output_position, output_column.name};
        const auto & input_column = input_header.getByPosition(input_position);
        result.addLineage(
            {output_ref, {0, input_position, input_column.name}, kind, DataPropertyProvenance::transformation(lineage_transformation)});

        if ((kind == ColumnLineageKind::Identity || kind == ColumnLineageKind::ValuePreserving) && !identity_outputs[input_position])
            identity_outputs[input_position] = output_ref;
    }

    for (const auto & unique_key : input_properties.uniqueKeys())
    {
        if (auto mapped = mapColumnSet(unique_key.columns, input_header, identity_outputs))
            result.addUniqueKey(unique_key.remap(std::move(*mapped), preservation_transformation));
    }

    for (const auto & dependency : input_properties.functionalDependencies())
    {
        auto determinant = mapColumnSet(dependency.determinant, input_header, identity_outputs);
        auto dependents = mapColumnSet(dependency.dependents, input_header, identity_outputs);
        if (determinant && dependents)
            result.addFunctionalDependency(dependency.remap(std::move(*determinant), std::move(*dependents), preservation_transformation));
    }

    for (const auto & non_null : input_properties.nonNullColumns())
    {
        if (non_null.position < identity_outputs.size() && identity_outputs[non_null.position])
            result.addNonNullColumn(*identity_outputs[non_null.position]);
    }

    for (const auto & lineage : result.columnLineage())
    {
        if (lineage.kind != ColumnLineageKind::NDVBound || lineage.input.position >= identity_outputs.size()
            || !identity_outputs[lineage.input.position])
            continue;
        result.addFunctionalDependency(
            {{*identity_outputs[lineage.input.position]},
             {lineage.output},
             DataPropertyDependencyKind::Statistical,
             DataPropertyProvenance::transformation(DataPropertyTransformationKind::NDVBoundExpression)});
    }

    return result;
}

std::vector<std::optional<PlanColumnRef>>
mapPreservedColumns(const Block & source_header, const Block & other_header, const Block & output_header)
{
    const UniqueColumnPositionIndex output_header_index(output_header);
    std::vector<std::optional<PlanColumnRef>> mapped(source_header.columns());
    for (size_t source_position = 0; source_position < source_header.columns(); ++source_position)
    {
        const auto & source = source_header.getByPosition(source_position);
        if (other_header.has(source.name))
            continue;

        if (const auto output_position = output_header_index.find(source.name, *source.type))
            mapped[source_position] = PlanColumnRef{*output_position, source.name};
    }
    return mapped;
}

void appendPreservedSide(
    DataPropertySet & result,
    const DataPropertySet & source_properties,
    const Block & source_header,
    const Block & other_header,
    const Block & output_header,
    size_t child_index,
    bool preserve_keys_and_dependencies)
{
    const auto mapped_columns = mapPreservedColumns(source_header, other_header, output_header);

    if (preserve_keys_and_dependencies)
    {
        for (const auto & unique_key : source_properties.uniqueKeys())
        {
            if (auto mapped = mapColumnSet(unique_key.columns, source_header, mapped_columns))
                result.addUniqueKey(unique_key.remap(std::move(*mapped), DataPropertyPreservingTransformationKind::JoinPreservation));
        }
        for (const auto & dependency : source_properties.functionalDependencies())
        {
            auto determinant = mapColumnSet(dependency.determinant, source_header, mapped_columns);
            auto dependents = mapColumnSet(dependency.dependents, source_header, mapped_columns);
            if (determinant && dependents)
                result.addFunctionalDependency(dependency.remap(
                    std::move(*determinant), std::move(*dependents), DataPropertyPreservingTransformationKind::JoinPreservation));
        }
    }

    for (const auto & non_null : source_properties.nonNullColumns())
    {
        if (non_null.position >= mapped_columns.size() || !mapped_columns[non_null.position])
            continue;
        const auto & mapped = *mapped_columns[non_null.position];
        if (!canContainNull(*output_header.getByPosition(mapped.position).type))
            result.addNonNullColumn(mapped);
    }

    for (size_t source_position = 0; source_position < mapped_columns.size(); ++source_position)
    {
        if (!mapped_columns[source_position])
            continue;
        result.addLineage(
            {*mapped_columns[source_position],
             {child_index, source_position, source_header.getByPosition(source_position).name},
             ColumnLineageKind::Identity,
             DataPropertyProvenance::transformation(DataPropertyTransformationKind::JoinPreservation)});
    }
}

DataPropertySet deriveDataPropertiesForStorageRead(const Block & output_header, const StorageInMemoryMetadata * metadata)
{
    DataPropertySet properties;
    for (size_t position = 0; position < output_header.columns(); ++position)
    {
        const auto & column = output_header.getByPosition(position);
        if (!canContainNull(*column.type))
            properties.addNonNullColumn({position, column.name});
    }

    if (!metadata || !metadata->hasUniqueKey())
        return properties;

    const auto output_names = output_header.getNames();
    const auto unique_key_names = metadata->getUniqueKeyColumns();
    auto unique_key = resolveColumnSetByName(output_names, unique_key_names);
    if (unique_key)
        properties.addUniqueKey(UniqueKeyFact::fromStorageDeclaration(std::move(*unique_key)));
    return properties;
}

DataPropertySet
deriveDataPropertiesForAggregation(const Block & output_header, const Names & grouping_keys, AggregationDataPropertyOptions options)
{
    DataPropertySet result;
    for (size_t position = 0; position < output_header.columns(); ++position)
    {
        const auto & column = output_header.getByPosition(position);
        if (!canContainNull(*column.type))
            result.addNonNullColumn({position, column.name});
    }

    if (!options.final || options.has_grouping_sets || options.has_overflow_row || grouping_keys.empty())
        return result;

    const auto output_names = output_header.getNames();
    if (auto key = resolveColumnSetByName(output_names, grouping_keys))
        result.addUniqueKey(UniqueKeyFact::fromAggregationGrouping(std::move(*key)));
    return result;
}

DataPropertySet deriveDataPropertiesForJoin(
    JoinKind kind, JoinStrictness strictness, const Block & output_header, DataPropertyInputView left, DataPropertyInputView right)
{
    DataPropertySet result;
    const bool subset_join = strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti;
    if (subset_join && kind == JoinKind::Left)
    {
        appendPreservedSide(result, left.properties, left.header, right.header, output_header, 0, true);
        return result;
    }
    if (subset_join && kind == JoinKind::Right)
    {
        appendPreservedSide(result, right.properties, right.header, left.header, output_header, 1, true);
        return result;
    }

    const bool preserve_left_non_null
        = kind == JoinKind::Inner || kind == JoinKind::Left || kind == JoinKind::Cross || kind == JoinKind::Comma;
    const bool preserve_right_non_null
        = kind == JoinKind::Inner || kind == JoinKind::Right || kind == JoinKind::Cross || kind == JoinKind::Comma;
    if (preserve_left_non_null)
        appendPreservedSide(result, left.properties, left.header, right.header, output_header, 0, false);
    if (preserve_right_non_null)
        appendPreservedSide(result, right.properties, right.header, left.header, output_header, 1, false);
    return result;
}

namespace detail
{

DataPropertySet deriveDataPropertiesForStep(const IQueryPlanStep & step, std::span<DataPropertySet> child_properties)
{
    if (child_properties.size() > 2 || !step.hasOutputHeader())
        return {};

    if (child_properties.empty())
    {
        if (!dynamic_cast<const ISourceStep *>(&step))
            return {};

        const StorageInMemoryMetadata * metadata = nullptr;
        if (const auto * storage_source = dynamic_cast<const SourceStepWithFilter *>(&step))
        {
            const auto & snapshot = storage_source->getStorageSnapshot();
            if (snapshot && snapshot->metadata)
                metadata = snapshot->metadata.get();
        }
        return deriveDataPropertiesForStorageRead(*step.getOutputHeader(), metadata);
    }

    const auto & output_header = *step.getOutputHeader();
    if (const auto * aggregation = dynamic_cast<const AggregatingStep *>(&step))
    {
        if (child_properties.size() != 1 || step.getInputHeaders().size() != 1)
            return {};
        return deriveDataPropertiesForAggregation(
            output_header,
            aggregation->getParams().keys,
            {.final = aggregation->getFinal(),
             .has_grouping_sets = aggregation->isGroupingSets(),
             .has_overflow_row = aggregation->getParams().overflow_row});
    }
    if (const auto * join = dynamic_cast<const JoinStepLogical *>(&step))
    {
        if (child_properties.size() != 2 || step.getInputHeaders().size() != 2)
            return {};
        const auto & join_operator = join->getJoinOperator();
        return deriveDataPropertiesForJoin(
            join_operator.kind,
            join_operator.strictness,
            output_header,
            {*step.getInputHeaders()[0], child_properties[0]},
            {*step.getInputHeaders()[1], child_properties[1]});
    }

    if (child_properties.size() != 1 || step.getInputHeaders().size() != 1)
        return {};

    const auto & input_header = *step.getInputHeaders().front();
    if (const auto * expression = dynamic_cast<const ExpressionStep *>(&step))
    {
        /// `arrayJoin` multiplies rows, so a pass-through column keeps its lineage but not its
        /// uniqueness: a proven unique key would produce a false cardinality cap. Fail closed,
        /// mirroring `preserves_number_of_rows` in `ExpressionStep`.
        if (expression->getExpression().hasArrayJoin())
            return {};
        return mapThroughActionsDAG(
            expression->getExpression(),
            input_header,
            output_header,
            child_properties.front(),
            DataPropertyPreservingTransformationKind::Identity);
    }
    if (const auto * filter = dynamic_cast<const FilterStep *>(&step))
    {
        if (filter->getExpression().hasArrayJoin())
            return {};
        return mapThroughActionsDAG(
            filter->getExpression(),
            input_header,
            output_header,
            child_properties.front(),
            DataPropertyPreservingTransformationKind::FilterSubset);
    }
    if (const auto * sorting_step = dynamic_cast<const SortingStep *>(&step))
    {
        const bool has_fill
            = std::ranges::any_of(sorting_step->getSortDescription(), [](const auto & sort_column) { return sort_column.with_fill; });
        if (!has_fill && blocksHaveEqualStructure(input_header, output_header))
            return std::move(child_properties.front());
        return {};
    }
    if (dynamic_cast<const LimitStep *>(&step) && blocksHaveEqualStructure(input_header, output_header))
        return std::move(child_properties.front());
    return {};
}

}
}

DataPropertySet deriveDataProperties(const IQueryPlanStep & step, std::span<const DataPropertySet> child_properties)
{
    if (child_properties.size() > 2)
        return {};

    /// Copy so per-step derivation may move from its inputs; child counts are tiny,
    /// so a plain vector beats maintaining a small-size special case in two places.
    std::vector<DataPropertySet> owned_child_properties(child_properties.begin(), child_properties.end());
    return detail::deriveDataPropertiesForStep(step, owned_child_properties);
}
}
