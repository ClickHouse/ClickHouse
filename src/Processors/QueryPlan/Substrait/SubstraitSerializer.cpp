#include <Processors/QueryPlan/Substrait/SubstraitSerializer.h>

#if USE_SUBSTRAIT

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Block.h>
#include <Core/Joins.h>
#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/StorageID.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Substrait/SubstraitExpressions.h>
#include <Processors/QueryPlan/Substrait/SubstraitTypes.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <google/protobuf/util/json_util.h>
#include <substrait/version_vendored.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

namespace
{

std::expected<SubstraitSchema, String> convertHeaderToSchema(const Block & header, SubstraitPlanExtensions & extensions)
{
    SubstraitSchema schema;
    for (const auto & column : header)
    {
        auto type = convertTypeToSubstrait(column.type, extensions);
        if (!type)
            return blocked(fmt::format("column {}: {}", column.name, type.error()));
        schema.append(column.name, std::move(*type));
    }
    return schema;
}

substrait::Expression makeI64Literal(Int64 value)
{
    substrait::Expression expression;
    expression.mutable_literal()->set_i64(value);
    return expression;
}

/// FetchRel (Substrait equivalent of LIMIT/OFFSET) over `input`. An unset count means ALL. A zero offset is omitted, Substrait defaults to 0.
substrait::Rel makeFetchRel(substrait::Rel && input, std::optional<Int64> count, Int64 offset)
{
    substrait::Rel rel;
    auto * fetch = rel.mutable_fetch();
    fetch->mutable_common()->mutable_direct();
    *fetch->mutable_input() = std::move(input);
    if (count)
        *fetch->mutable_count_expr() = makeI64Literal(*count);
    if (offset > 0)
        *fetch->mutable_offset_expr() = makeI64Literal(offset);
    return rel;
}

/// Aggregate resolved against the table.
struct ResolvedAggregate
{
    std::string_view urn;
    String signature;
    substrait::Type output_type;
    substrait::AggregateFunction::AggregationInvocation invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL;
    FunctionOptions options;
};

/// Substrait function to emit and the result type its definition fixes.
/// Substrait defines each function together with its result type, nullable or not, and the type we write must repeat that definition.
struct AggregateEmission
{
    std::string_view name;
    std::string_view return_type; /// Short name.
    bool return_nullable;
};

/// One argument type the aggregate accepts, and what to emit for a call with it.
struct AggregateOverload
{
    std::string_view arg; /// Short type name of the overload's single argument (or "any" to accept every type).
    std::string_view urn;

    /// Over empty input ClickHouse aggregates return a default (sum of nothing is 0) where most Substrait functions return NULL.
    /// Only a query without GROUP BY can aggregate empty input into a row.
    /// keyed: Emitted with GROUP BY. Every group has rows, so the disagreement cannot surface.
    /// keyless: Emitted without GROUP BY, must return ClickHouse's empty-input value, sum emits sum0. nullopt means no such function.
    AggregateEmission keyed;
    std::optional<AggregateEmission> keyless;

    FunctionOptions options;
};

/// One translatable aggregate, its overloads and the ALL or DISTINCT flag of the emitted call. DISTINCT drops duplicate values first.
struct MappedAggregate
{
    substrait::AggregateFunction::AggregationInvocation invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL;
    std::vector<AggregateOverload> overloads;
};

/// The aggregates we can translate, keyed by ClickHouse name. New aggregates go here. Only call via aggregateTable.
std::unordered_map<std::string_view, MappedAggregate> buildAggregateTable()
{
    const FunctionOptions overflow_silent = {{"overflow", "SILENT"}};
    std::unordered_map<std::string_view, MappedAggregate> table;

    /// count of nothing is zero on both sides, so keyless count is the same function.
    const AggregateEmission count_emission{.name = "count", .return_type = "i64", .return_nullable = false};
    const AggregateOverload count_overload{
        .arg = "any",
        .urn = SubstraitExtensionURN::aggregate_generic,
        .keyed = count_emission,
        .keyless = count_emission,
        .options = overflow_silent};
    table["count"] = {.invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL, .overloads = {count_overload}};
    table["uniqExact"] = {.invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT, .overloads = {count_overload}};

    MappedAggregate sum{.invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL, .overloads = {}};
    for (std::string_view t : {"i8", "i16", "i32", "i64"})
        sum.overloads.push_back(
            {.arg = t,
             .urn = SubstraitExtensionURN::arithmetic,
             .keyed = {.name = "sum", .return_type = "i64", .return_nullable = true},
             .keyless = AggregateEmission{.name = "sum0", .return_type = "i64", .return_nullable = false},
             .options = overflow_silent});
    for (std::string_view t : {"fp32", "fp64"})
        sum.overloads.push_back(
            {.arg = t,
             .urn = SubstraitExtensionURN::arithmetic,
             .keyed = {.name = "sum", .return_type = "fp64", .return_nullable = true},
             .keyless = AggregateEmission{.name = "sum0", .return_type = "fp64", .return_nullable = false},
             .options = overflow_silent});
    /// The unsigned extension declares no sum0.
    for (std::string_view t : {"u!u8", "u!u16", "u!u32", "u!u64"})
        sum.overloads.push_back(
            {.arg = t,
             .urn = SubstraitExtensionURN::unsigned_integers,
             .keyed = {.name = "sum", .return_type = "u!u64", .return_nullable = true},
             .keyless = std::nullopt,
             .options = overflow_silent});
    table["sum"] = std::move(sum);

    for (std::string_view name : {"min", "max"})
    {
        MappedAggregate entry{.invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL, .overloads = {}};
        for (std::string_view t : {"i8", "i16", "i32", "i64", "fp32", "fp64"})
            entry.overloads.push_back(
                {.arg = t,
                 .urn = SubstraitExtensionURN::arithmetic,
                 .keyed = {.name = name, .return_type = t, .return_nullable = true},
                 .keyless = std::nullopt,
                 .options = {}});
        for (std::string_view t : {"u!u8", "u!u16", "u!u32", "u!u64"})
            entry.overloads.push_back(
                {.arg = t,
                 .urn = SubstraitExtensionURN::unsigned_integers,
                 .keyed = {.name = name, .return_type = t, .return_nullable = true},
                 .keyless = std::nullopt,
                 .options = {}});
        table[name] = std::move(entry);
    }

    table["avg"]
        = {.invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL,
           .overloads
           = {{.arg = "fp64",
               .urn = SubstraitExtensionURN::arithmetic,
               .keyed = {.name = "avg", .return_type = "fp64", .return_nullable = true},
               .keyless = std::nullopt,
               .options = overflow_silent}}};

    return table;
}

const std::unordered_map<std::string_view, MappedAggregate> & aggregateTable()
{
    static const auto table = buildAggregateTable();
    return table;
}

/// The output type from its short name.
substrait::Type makeAggregateOutputType(std::string_view output_short, bool nullable, SubstraitPlanExtensions & extensions)
{
    if (const auto type_name = tryParseUserDefinedShortName(output_short); !type_name.empty())
        return substraitUnsignedType(type_name, nullable, extensions);
    return makeSimpleSubstraitType(output_short, nullable);
}

/// Resolves one aggregate call against the table.
std::expected<ResolvedAggregate, String> mapAggregateFunction(
    const AggregateDescription & description,
    const std::vector<substrait::Type> & argument_types,
    bool has_grouping_keys,
    SubstraitPlanExtensions & extensions)
{
    const String & name = description.function->getName();

    if (!description.parameters.empty())
        return blocked(fmt::format("parametric aggregate function {} is not mapped to Substrait", name));

    const auto & table = aggregateTable();
    const auto it = table.find(name);
    if (it == table.end())
        return blocked(fmt::format("aggregate function {} is not mapped to Substrait", name));
    const MappedAggregate & mapping = it->second;

    /// count is the one aggregate callable with no argument. Its compound signature is the name with the colon, "count:".
    const bool count_without_args = name == "count" && argument_types.empty();
    if (!count_without_args && argument_types.size() != 1)
        return blocked(fmt::format("aggregate function {} with {} arguments is not mapped to Substrait", name, argument_types.size()));

    const String arg = count_without_args ? "" : substraitTypeShortName(argument_types[0], extensions);
    const auto matches
        = [&](const AggregateOverload & overload) { return count_without_args || overload.arg == "any" || overload.arg == arg; };
    const auto overload_it = std::ranges::find_if(mapping.overloads, matches);
    if (overload_it == mapping.overloads.end())
        return blocked(fmt::format("aggregate function {} has no Substrait overload for {}", name, arg));
    const AggregateOverload & overload = *overload_it;

    if (!has_grouping_keys && !overload.keyless)
        return blocked(
            fmt::format(
                "aggregate function {} returns a default value over empty input in ClickHouse "
                "but NULL in Substrait, so it cannot be offloaded without grouping keys",
                name));
    const AggregateEmission & emission = has_grouping_keys ? overload.keyed : *overload.keyless;

    return ResolvedAggregate{
        .urn = overload.urn,
        .signature = fmt::format("{}:{}", emission.name, count_without_args ? std::string_view{} : overload.arg),
        .output_type = makeAggregateOutputType(emission.return_type, emission.return_nullable, extensions),
        .invocation = mapping.invocation,
        .options = overload.options,
    };
}

}

const SubstraitPlanSerializer::Verdict & SubstraitPlanSerializer::visit(const QueryPlan::Node & root)
{
    struct Frame
    {
        const QueryPlan::Node * node;
        size_t next_child = 0;
    };

    std::vector<Frame> stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Already converted in an earlier visit.
        if (states.contains(frame.node))
        {
            stack.pop_back();
            continue;
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push_back({.node = frame.node->children[frame.next_child]});
            ++frame.next_child;
            continue;
        }

        const auto & node = *frame.node;
        auto & state = states[&node];
        stack.pop_back();

        std::vector<NodeState *> children;
        children.reserve(node.children.size());
        bool children_eligible = true;
        for (const auto * child : node.children)
        {
            auto & child_state = states.at(child);
            children_eligible &= child_state.verdict.subtree_eligible;
            children.push_back(&child_state);
        }

        /// No need to attempt step if any child is ineligible.
        if (!children_eligible)
            continue;

        auto converted = convertStep(node, std::move(children));
        if (converted)
        {
            state = std::move(*converted);
            state.verdict.subtree_eligible = true;
        }
        else
            state.verdict.blocking_reason = std::move(converted.error());
    }

    return states.at(&root).verdict;
}

const SubstraitPlanSerializer::Verdict & SubstraitPlanSerializer::verdictFor(const QueryPlan::Node & node) const
{
    const auto it = states.find(&node);
    if (it == states.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Node was not visited by the Substrait serializer");
    return it->second.verdict;
}

std::vector<std::pair<const QueryPlan::Node *, String>> SubstraitPlanSerializer::blockingSteps(const QueryPlan::Node & root) const
{
    std::vector<std::pair<const QueryPlan::Node *, String>> result;
    std::vector<const QueryPlan::Node *> stack{&root};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        const auto & verdict = verdictFor(*node);
        if (verdict.subtree_eligible)
            continue;

        if (!verdict.blocking_reason.empty())
        {
            result.emplace_back(node, verdict.blocking_reason);
            continue;
        }

        /// No reason means the step was never attempted because of a blocked child. Descend to find it.
        for (const auto * child : node->children)
            stack.push_back(child);
    }
    return result;
}

substrait::Plan SubstraitPlanSerializer::takePlan(const QueryPlan::Node & root)
{
    auto & state = states.at(&root);
    if (!state.verdict.subtree_eligible)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Requested a Substrait plan for an ineligible subtree");
    if (state.rel.rel_type_case() == substrait::Rel::REL_TYPE_NOT_SET)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Requested a Substrait plan for a node whose relation was already consumed");

    substrait::Plan plan;
    auto * version = plan.mutable_version();

    version->set_major_number(SUBSTRAIT_VERSION_MAJOR);
    version->set_minor_number(SUBSTRAIT_VERSION_MINOR);
    version->set_patch_number(SUBSTRAIT_VERSION_PATCH);
    version->set_producer("ClickHouse");

    extensions.declareIn(plan);

    auto * root_rel = plan.add_relations()->mutable_root();
    root_rel->mutable_input()->Swap(&state.rel);
    for (const auto & column : state.schema.columns)
        root_rel->add_names(column.name);

    return plan;
}

/// A converter builds its relation by moving parts out of its children's states. So move only after the last check that can fail.
/// A converter that fails after moving leaves a child eligible but empty, and serializing it later gives an empty plan.
std::expected<SubstraitPlanSerializer::NodeState, String>
SubstraitPlanSerializer::convertStep(const QueryPlan::Node & node, std::vector<NodeState *> children)
{
    const IQueryPlanStep * step = node.step.get();
    NodeState out;

    if (const auto * read = typeid_cast<const ReadFromMergeTree *>(step))
    {
        if (read->isQueryWithFinal())
            return blocked("FINAL is not supported");
        if (read->isQueryWithSampling())
            return blocked("SAMPLE is not supported");
        if (read->getPrewhereInfo() || read->getDeferredPrewhereInfo())
            return blocked("PREWHERE is not supported yet");
        if (read->getRowLevelFilter() || read->getDeferredRowLevelFilter())
            return blocked("row-level filters are not supported");

        const auto & header = *read->getOutputHeader();
        const auto & virtuals = read->getStorageSnapshot()->metadata->virtuals;
        for (const auto & column : header)
            if (virtuals.has(column.name))
                return blocked(fmt::format("virtual column {} is not representable in a Substrait read", column.name));

        auto schema = convertHeaderToSchema(header, extensions);
        if (!schema)
            return std::unexpected(std::move(schema.error()));
        out.schema = std::move(*schema);

        auto * read_rel = out.rel.mutable_read();
        read_rel->mutable_common()->mutable_direct();

        auto * base_schema = read_rel->mutable_base_schema();
        auto * struct_type = base_schema->mutable_struct_();
        struct_type->set_nullability(substraitNullabilityOf(false));
        for (const auto & column : out.schema.columns)
        {
            base_schema->add_names(column.name);
            *struct_type->add_types() = column.type;
        }

        const auto storage_id = read->getStorageID();
        auto * named_table = read_rel->mutable_named_table();
        named_table->add_names(storage_id.database_name);
        named_table->add_names(storage_id.table_name);
        return out;
    }

    /// FilterStep and ExpressionStep share everything but the FilterRel wrapper.
    const auto * filter = typeid_cast<const FilterStep *>(step);
    const auto * expression = typeid_cast<const ExpressionStep *>(step);
    if (filter || expression)
    {
        auto & child = *children.at(0);
        SubstraitExpressionConverter converter(child.schema, extensions);
        const auto & dag = filter ? filter->getExpression() : expression->getExpression();

        substrait::Expression condition;
        if (filter)
        {
            const auto * condition_node = dag.tryFindInOutputs(filter->getFilterColumnName());
            if (!condition_node)
                return blocked(fmt::format("filter column {} is not among the expression outputs", filter->getFilterColumnName()));

            auto converted = converter.convert(*condition_node);
            if (!converted)
                return std::unexpected(std::move(converted.error()));
            if (converted->type.kind_case() != substrait::Type::kBool)
                return blocked(fmt::format("filter condition {} is not a boolean", filter->getFilterColumnName()));
            condition = std::move(converted->expression);
        }

        /// The child nests into a FilterRel when there is a condition, and the projection wraps the result.
        substrait::Rel rel;
        rel.Swap(&child.rel);
        if (filter)
        {
            substrait::Rel filter_rel;
            auto * filter_body = filter_rel.mutable_filter();
            filter_body->mutable_common()->mutable_direct();
            *filter_body->mutable_condition() = std::move(condition);
            *filter_body->mutable_input() = std::move(rel);
            rel = std::move(filter_rel);
        }
        auto converted = converter.convertProjection(dag, *step->getOutputHeader(), std::move(rel));
        if (!converted)
            return std::unexpected(std::move(converted.error()));
        out.rel = std::move(converted->rel);
        out.schema = std::move(converted->schema);

        out.sort_reaches_output = child.sort_reaches_output;
        return out;
    }

    if (const auto * aggregating = typeid_cast<const AggregatingStep *>(step))
    {
        auto & child = *children.at(0);
        const auto & params = aggregating->getParams();

        if (!aggregating->isFinal())
            return blocked("partial aggregation states have no Substrait representation");
        if (aggregating->isGroupingSets())
            return blocked("GROUPING SETS are not supported yet");
        if (params.overflow_row)
            return blocked("aggregation with overflow row is not supported");
        if (params.top_k.has_value())
            return blocked("top-K aggregation is not supported");

        substrait::Rel aggregate_rel;
        auto * aggregate = aggregate_rel.mutable_aggregate();

        SubstraitSchema direct_schema;

        auto * grouping = aggregate->add_groupings();
        for (size_t i = 0; i < params.keys.size(); ++i)
        {
            const auto position = child.schema.positionByName(params.keys[i]);
            if (!position)
                return blocked(fmt::format("grouping key {} is not present in the child schema", params.keys[i]));
            *aggregate->add_grouping_expressions() = makeColumnReference(*position);
            grouping->add_expression_references(static_cast<UInt32>(i));
            direct_schema.append(params.keys[i], child.schema.columns[*position].type);
        }

        for (const auto & description : params.aggregates)
        {
            std::vector<size_t> argument_positions;
            std::vector<substrait::Type> argument_types;
            for (const auto & argument : description.argument_names)
            {
                const auto position = child.schema.positionByName(argument);
                if (!position)
                    return blocked(fmt::format("aggregate argument {} is not present in the child schema", argument));
                argument_positions.push_back(*position);
                argument_types.push_back(child.schema.columns[*position].type);
            }

            auto mapping = mapAggregateFunction(description, argument_types, !params.keys.empty(), extensions);
            if (!mapping)
                return std::unexpected(std::move(mapping.error()));

            auto * measure = aggregate->add_measures()->mutable_measure();
            measure->set_function_reference(extensions.internFunction(mapping->urn, mapping->signature));
            for (const auto position : argument_positions)
                *measure->add_arguments()->mutable_value() = makeColumnReference(position);
            addFunctionOptions(mapping->options, *measure);
            measure->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
            measure->set_invocation(mapping->invocation);
            *measure->mutable_output_type() = mapping->output_type;
            direct_schema.append(description.column_name, std::move(mapping->output_type));
        }

        const auto & header = *step->getOutputHeader();
        if (header.columns() != direct_schema.size())
            return blocked("aggregation output header does not match the converted schema");

        /// Align the direct output (keys, then measures) with the step's output header.
        /// TODO: remove duplicate logic with convertProjection.
        bool identity = true;
        std::vector<Int32> mapping_to_header;
        for (size_t i = 0; i < header.columns(); ++i)
        {
            const auto position = direct_schema.positionByName(header.getByPosition(i).name);
            if (!position)
                return blocked(
                    fmt::format("aggregation output column {} was not produced by the conversion", header.getByPosition(i).name));
            identity &= *position == i;
            mapping_to_header.push_back(static_cast<Int32>(*position));
            out.schema.append(header.getByPosition(i).name, direct_schema.columns[*position].type);
        }

        if (identity)
            aggregate->mutable_common()->mutable_direct();
        else
        {
            auto * emit = aggregate->mutable_common()->mutable_emit();
            for (const auto position : mapping_to_header)
                emit->add_output_mapping(position);
        }

        aggregate->mutable_input()->Swap(&child.rel);
        out.rel = std::move(aggregate_rel);
        return out;
    }

    if (const auto * sorting = typeid_cast<const SortingStep *>(step))
    {
        /// Full, FinishSorting and MergingSorted produce the same fully sorted output and differ only in algorithm.
        /// PartitionedFinishSorting sorts each partition separately and never merges.
        /// SortRel describes the result, not the algorithm, so first three serialize, while the latter does not.
        if (sorting->getType() == SortingStep::Type::PartitionedFinishSorting)
            return blocked("partitioned sorting produces separately sorted partitions, not one sorted stream");
        if (!std::in_range<Int64>(sorting->getLimit()))
            return blocked("LIMIT or OFFSET does not fit a Substrait i64");

        auto & child = *children.at(0);
        substrait::Rel sort_rel;
        auto * sort = sort_rel.mutable_sort();
        sort->mutable_common()->mutable_direct();

        for (const auto & column : sorting->getSortDescription())
        {
            if (column.collator)
                return blocked("sorting with a collation is not supported");
            if (column.with_fill)
                return blocked("WITH FILL is not supported");

            const auto position = child.schema.positionByName(column.column_name);
            if (!position)
                return blocked(fmt::format("sort column {} is not present in the child schema", column.column_name));

            auto * field = sort->add_sorts();
            *field->mutable_expr() = makeColumnReference(*position);

            const bool nulls_last = column.nulls_direction == column.direction;
            if (column.direction == 1)
                field->set_direction(
                    nulls_last ? substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST
                               : substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST);
            else
                field->set_direction(
                    nulls_last ? substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST
                               : substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST);
        }

        sort->mutable_input()->Swap(&child.rel);

        /// A sort with a limit produces at most `limit` rows, which SortRel cannot express. An explicit FetchRel on top represents this.
        if (sorting->getLimit() > 0)
            out.rel = makeFetchRel(std::move(sort_rel), static_cast<Int64>(sorting->getLimit()), 0);
        else
            out.rel = std::move(sort_rel);

        out.schema = std::move(child.schema);
        out.sort_reaches_output = true;
        return out;
    }

    if (const auto * limit = typeid_cast<const LimitStep *>(step))
    {
        if (limit->withTies())
            return blocked("LIMIT WITH TIES is not supported");
        if (!std::in_range<Int64>(limit->getLimit()) || !std::in_range<Int64>(limit->getOffset()))
            return blocked("LIMIT or OFFSET does not fit a Substrait i64");

        auto & child = *children.at(0);
        out.rel = makeFetchRel(std::move(child.rel), static_cast<Int64>(limit->getLimit()), static_cast<Int64>(limit->getOffset()));
        out.schema = std::move(child.schema);
        out.sort_reaches_output = child.sort_reaches_output;
        return out;
    }

    if (const auto * offset = typeid_cast<const OffsetStep *>(step))
    {
        if (!std::in_range<Int64>(offset->getOffset()))
            return blocked("LIMIT or OFFSET does not fit a Substrait i64");

        auto & child = *children.at(0);
        out.rel = makeFetchRel(std::move(child.rel), {}, static_cast<Int64>(offset->getOffset()));
        out.schema = std::move(child.schema);
        out.sort_reaches_output = child.sort_reaches_output;
        return out;
    }

    if (typeid_cast<const UnionStep *>(step))
    {
        const auto & first_schema = children.at(0)->schema;
        for (size_t i = 1; i < children.size(); ++i)
        {
            const auto & schema = children[i]->schema;
            if (schema.size() != first_schema.size())
                return blocked("UNION inputs have different widths");
            for (size_t j = 0; j < schema.size(); ++j)
                if (!substraitTypesIdentical(schema.columns[j].type, first_schema.columns[j].type))
                    return blocked(fmt::format("UNION input types differ in column {}", first_schema.columns[j].name));
        }

        auto * set = out.rel.mutable_set();
        set->mutable_common()->mutable_direct();
        set->set_op(substrait::SetRel_SetOp_SET_OP_UNION_ALL);
        for (auto * child : children)
            set->add_inputs()->Swap(&child->rel);

        out.schema = std::move(children.at(0)->schema);
        return out;
    }

    if (const auto * distinct = typeid_cast<const DistinctStep *>(step))
    {
        auto & child = *children.at(0);

        /// TODO: ClickHouse DISTINCT preserves the input order but Substrait AggregateRel does not.
        if (child.sort_reaches_output)
            return blocked("DISTINCT over sorted input preserves the order in ClickHouse, which AggregateRel cannot express");

        /// DISTINCT over a subset of columns keeps arbitrary values of the others, which has no Substrait equivalent.
        /// TODO: come up with semantically honest translation/
        NameSet distinct_columns(distinct->getColumnNames().begin(), distinct->getColumnNames().end());
        for (const auto & column : child.schema.columns)
            if (!distinct_columns.contains(column.name))
                return blocked(fmt::format("DISTINCT does not cover column {}", column.name));

        /// ClickHouse may split DISTINCT in two, a preliminary step deduplicating each stream and a final one above it finishing the job.
        /// Our AggregateRel fully deduplicates even as the preliminary step. Harmless as the final step then just finds nothing left to remove.
        auto * aggregate = out.rel.mutable_aggregate();
        aggregate->mutable_common()->mutable_direct();
        auto * grouping = aggregate->add_groupings();
        for (size_t i = 0; i < child.schema.size(); ++i)
        {
            *aggregate->add_grouping_expressions() = makeColumnReference(i);
            grouping->add_expression_references(static_cast<UInt32>(i));
        }
        aggregate->mutable_input()->Swap(&child.rel);

        out.schema = std::move(child.schema);
        return out;
    }

    if (const auto * join = typeid_cast<const JoinStepLogical *>(step))
    {
        auto & left = *children.at(0);
        auto & right = *children.at(1);
        const auto & join_operator = join->getJoinOperator();

        if (join_operator.locality == JoinLocality::Global)
            return blocked("GLOBAL JOIN is not supported");
        if (!join_operator.shared_runtime_filter_descriptors.empty())
            return blocked("joins with runtime filters are not supported");

        /// JoinRel direct output is all left fields then all right fields.
        /// A copy s.t. on every failure below the children stay eligible subtrees whose schemas takePlan still reads.
        SubstraitSchema combined = left.schema;
        combined.appendAll(right.schema);

        /// The schema the output projection resolves against. The SEMI and ANTI cases below narrow it to the kept side.
        const SubstraitSchema * post_join_schema = &combined;

        std::optional<substrait::JoinRel_JoinType> join_type;
        switch (join_operator.strictness)
        {
            case JoinStrictness::All:
            case JoinStrictness::Unspecified:
                if (join_operator.kind == JoinKind::Inner)
                    join_type = substrait::JoinRel_JoinType_JOIN_TYPE_INNER;
                else if (join_operator.kind == JoinKind::Cross || join_operator.kind == JoinKind::Comma)
                    join_type = std::nullopt; /// No JoinRel type (cross join) built below in the branch without a join type.
                else
                    /// The remaining kinds are the outer joins. For a non-matched row ClickHouse fills the other side with type defaults.
                    /// Unless join_use_nulls is set, Substrait always fills it with NULL.
                    /// TODO: come up with semantically honest translation.
                    return blocked(fmt::format("{} JOIN is not supported yet", toString(join_operator.kind)));
                break;
            case JoinStrictness::Semi:
            case JoinStrictness::Anti: {
                const bool semi = join_operator.strictness == JoinStrictness::Semi;
                if (join_operator.kind == JoinKind::Left)
                {
                    join_type = semi ? substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI : substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI;
                    post_join_schema = &left.schema;
                }
                else if (join_operator.kind == JoinKind::Right)
                {
                    join_type = semi ? substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI : substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT_ANTI;
                    post_join_schema = &right.schema;
                }
                else
                    return blocked(
                        fmt::format("{} {} JOIN is not supported", toString(join_operator.strictness), toString(join_operator.kind)));
                break;
            }
            default:
                /// ANY picks one matching row (Substrait SINGLE errors on duplicates instead) and ASOF picks the closest row. Neither maps exactly.
                return blocked(fmt::format("{} JOIN is not supported", toString(join_operator.strictness)));
        }

        SubstraitExpressionConverter converter(combined, extensions);

        /// Combines condition conjuncts into one boolean expression.
        const auto combine_conjuncts = [&](const std::vector<JoinActionRef> & conjuncts) -> std::expected<substrait::Expression, String>
        {
            std::vector<SubstraitExpressionConverter::ConvertedExpression> converted_conjuncts;
            for (const auto & conjunct : conjuncts)
            {
                auto converted = converter.convert(*conjunct.getNode());
                if (!converted)
                    return std::unexpected(std::move(converted.error()));
                if (converted->type.kind_case() != substrait::Type::kBool)
                    return blocked("join condition is not a boolean");
                converted_conjuncts.push_back(std::move(*converted));
            }
            return std::move(makeConjunction(std::move(converted_conjuncts), extensions).expression);
        };

        substrait::Rel join_rel;
        if (!join_type)
        {
            if (!join_operator.expression.empty() || !join_operator.residual_filter.empty())
                return blocked("CROSS JOIN with a condition is not supported");
            auto * cross = join_rel.mutable_cross();
            cross->mutable_common()->mutable_direct();
        }
        else
        {
            if (join_operator.expression.empty())
                return blocked("JOIN without a condition is not supported");
            auto * join_body = join_rel.mutable_join();
            join_body->mutable_common()->mutable_direct();
            join_body->set_type(*join_type);
            auto condition = combine_conjuncts(join_operator.expression);
            if (!condition)
                return std::unexpected(std::move(condition.error()));
            *join_body->mutable_expression() = std::move(*condition);
            if (!join_operator.residual_filter.empty())
            {
                auto residual = combine_conjuncts(join_operator.residual_filter);
                if (!residual)
                    return std::unexpected(std::move(residual.error()));
                *join_body->mutable_post_join_filter() = std::move(*residual);
            }
        }

        /// A SEMI/ANTI join outputs only the kept side, so the projection on top resolves names against that side's schema alone.
        /// A name from the dropped side then fails to resolve. ClickHouse SEMI joins may project the dropped side but Substrait ones cannot.
        /// For INNER/CROSS the output is both sides, the schema the condition converter already works over, so it is reused.
        std::optional<SubstraitExpressionConverter> side_converter;
        if (post_join_schema != &combined)
            side_converter.emplace(*post_join_schema, extensions);
        auto & output_converter = side_converter ? *side_converter : converter;

        /// Attach the children, then the projection wraps the join.
        if (join_rel.has_cross())
        {
            join_rel.mutable_cross()->mutable_left()->Swap(&left.rel);
            join_rel.mutable_cross()->mutable_right()->Swap(&right.rel);
        }
        else
        {
            join_rel.mutable_join()->mutable_left()->Swap(&left.rel);
            join_rel.mutable_join()->mutable_right()->Swap(&right.rel);
        }
        auto converted = output_converter.convertProjection(join->getActionsDAG(), *step->getOutputHeader(), std::move(join_rel));
        if (!converted)
            return std::unexpected(std::move(converted.error()));
        out.rel = std::move(converted->rel);
        out.schema = std::move(converted->schema);
        return out;
    }

    return blocked("the step has no Substrait representation");
}

void applySubstraitSerializationSettings(QueryPlanOptimizationSettings & settings)
{
    settings.keep_logical_steps = true;
    /// TODO: Serialize PREWHERE as ReadRel.filter and drop this.
    settings.optimize_prewhere = false;
}

substrait::Plan serializeQueryPlanToSubstrait(const QueryPlan & plan)
{
    const auto * root = plan.getRootNode();
    if (!root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize an uninitialized query plan");

    SubstraitPlanSerializer serializer;
    if (!serializer.visit(*root).subtree_eligible)
    {
        /// TODO: make this look nicer.
        std::vector<String> reasons;
        for (const auto & [node, reason] : serializer.blockingSteps(*root))
            reasons.push_back(fmt::format("step {}: {}", node->step->getName(), reason));
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The query plan cannot be serialized to Substrait: {}", fmt::join(reasons, "; "));
    }

    return serializer.takePlan(*root);
}

String serializeQueryPlanToSubstraitJSON(const QueryPlan & plan)
{
    const auto substrait_plan = serializeQueryPlanToSubstrait(plan);

    String json;
    google::protobuf::util::JsonPrintOptions print_options;
    print_options.add_whitespace = true;
    const auto status = google::protobuf::util::MessageToJsonString(substrait_plan, &json, print_options);
    if (!status.ok())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot render a Substrait plan as JSON: {}", status.ToString());
    return json;
}

}

#endif
