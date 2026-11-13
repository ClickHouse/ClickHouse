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

std::expected<SubstraitSchema, SubstraitBlockingReason> convertHeaderToSchema(const Block & header, SubstraitPlanExtensions & extensions)
{
    SubstraitSchema schema;
    for (const auto & column : header)
    {
        auto type = convertTypeToSubstrait(column.type, extensions);
        if (!type)
            return blocked(fmt::format("column {}: {}", column.name, type.error().text));
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

/// One argument list the aggregate accepts, and what to emit for a call with it.
struct AggregateOverload
{
    std::vector<std::string_view> arg_types;
    std::string_view urn;

    /// Over empty input ClickHouse returns a default (sum of nothing is 0) where most Substrait functions return NULL.
    /// A Nullable argument gives NULL in both, so it always emits keyed. Only a query without GROUP BY can aggregate empty input into a row.
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
    /// count takes one argument of any type or none. With none its compound signature is "count:".
    const AggregateEmission count_emission{.name = "count", .return_type = "i64", .return_nullable = false};
    const auto count_overload = [&](std::vector<std::string_view> arg_types)
    {
        return AggregateOverload{
            .arg_types = std::move(arg_types),
            .urn = SubstraitExtensionURN::aggregate_generic,
            .keyed = count_emission,
            .keyless = count_emission,
            .options = overflow_silent};
    };
    table["count"] = {
        .invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL, .overloads = {count_overload({"any"}), count_overload({})}};
    table["uniqExact"]
        = {.invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT, .overloads = {count_overload({"any"})}};

    MappedAggregate sum{.invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL, .overloads = {}};
    for (std::string_view t : {"i8", "i16", "i32", "i64"})
        sum.overloads.push_back(
            {.arg_types = {t},
             .urn = SubstraitExtensionURN::arithmetic,
             .keyed = {.name = "sum", .return_type = "i64", .return_nullable = true},
             .keyless = AggregateEmission{.name = "sum0", .return_type = "i64", .return_nullable = false},
             .options = overflow_silent});
    for (std::string_view t : {"fp32", "fp64"})
        sum.overloads.push_back(
            {.arg_types = {t},
             .urn = SubstraitExtensionURN::arithmetic,
             .keyed = {.name = "sum", .return_type = "fp64", .return_nullable = true},
             .keyless = AggregateEmission{.name = "sum0", .return_type = "fp64", .return_nullable = false},
             .options = overflow_silent});
    /// The unsigned extension declares no sum0.
    for (std::string_view t : {"u!u8", "u!u16", "u!u32", "u!u64"})
        sum.overloads.push_back(
            {.arg_types = {t},
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
                {.arg_types = {t},
                 .urn = SubstraitExtensionURN::arithmetic,
                 .keyed = {.name = name, .return_type = t, .return_nullable = true},
                 .keyless = std::nullopt,
                 .options = {}});
        for (std::string_view t : {"u!u8", "u!u16", "u!u32", "u!u64"})
            entry.overloads.push_back(
                {.arg_types = {t},
                 .urn = SubstraitExtensionURN::unsigned_integers,
                 .keyed = {.name = name, .return_type = t, .return_nullable = true},
                 .keyless = std::nullopt,
                 .options = {}});
        table[name] = std::move(entry);
    }

    table["avg"]
        = {.invocation = substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL,
           .overloads
           = {{.arg_types = {"fp64"},
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
std::expected<ResolvedAggregate, SubstraitBlockingReason> mapAggregateFunction(
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

    std::vector<String> arg_shorts;
    for (const auto & type : argument_types)
        arg_shorts.push_back(substraitTypeShortName(type, extensions));

    const auto accepts = [](std::string_view declared, std::string_view actual) { return declared == "any" || declared == actual; };
    const auto matches = [&](const AggregateOverload & overload) { return std::ranges::equal(overload.arg_types, arg_shorts, accepts); };
    const auto overload_it = std::ranges::find_if(mapping.overloads, matches);
    if (overload_it == mapping.overloads.end())
        return blocked(fmt::format("aggregate function {} has no Substrait overload for {}", name, fmt::join(arg_shorts, ", ")));
    const AggregateOverload & overload = *overload_it;

    /// A ClickHouse aggregate returns NULL over empty input when its result type is Nullable.
    const bool empty_input_agrees = has_grouping_keys || description.function->getResultType()->isNullable();
    if (!empty_input_agrees && !overload.keyless)
        return blocked(
            fmt::format(
                "aggregate function {} returns a default value over empty input in ClickHouse "
                "but NULL in Substrait, so it cannot be offloaded without grouping keys",
                name));
    const AggregateEmission & emission = empty_input_agrees ? overload.keyed : *overload.keyless;

    return ResolvedAggregate{
        .urn = overload.urn,
        .signature = fmt::format("{}:{}", emission.name, fmt::join(overload.arg_types, "_")),
        .output_type = makeAggregateOutputType(emission.return_type, emission.return_nullable, extensions),
        .invocation = mapping.invocation,
        .options = overload.options,
    };
}

/// True when rows leave the step in an order a sort below established.
/// A sorting step starts it, the steps that destroy row order end it, and every other step is assumed to keep it.
bool sortReachesOutput(const IQueryPlanStep & step, bool any_child_sorted)
{
    if (typeid_cast<const SortingStep *>(&step))
        return true;
    if (typeid_cast<const AggregatingStep *>(&step) || typeid_cast<const JoinStepLogical *>(&step) || typeid_cast<const UnionStep *>(&step))
        return false;
    return any_child_sorted;
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
        bool any_child_sorted = false;
        for (const auto * child : node.children)
        {
            auto & child_state = states.at(child);
            children_eligible &= child_state.verdict.subtree_eligible;
            any_child_sorted |= child_state.sort_reaches_output;
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
            state.sort_reaches_output = sortReachesOutput(*node.step, any_child_sorted);
        }
        else
            state.verdict.blocking_reason = std::move(converted.error());
    }

    return states.at(&root).verdict;
}

std::vector<std::pair<const QueryPlan::Node *, SubstraitBlockingReason>>
SubstraitPlanSerializer::blockingSteps(const QueryPlan::Node & root) const
{
    std::vector<std::pair<const QueryPlan::Node *, SubstraitBlockingReason>> result;
    std::vector<const QueryPlan::Node *> stack{&root};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        const auto & verdict = states.at(node).verdict;
        if (verdict.subtree_eligible)
            continue;

        if (verdict.blocking_reason)
        {
            result.emplace_back(node, *verdict.blocking_reason);
            continue;
        }

        /// No reason means the step was never attempted because of a blocked child. Descend to find it, left child first.
        stack.insert(stack.end(), node->children.rbegin(), node->children.rend());
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

    auto * root_rel = plan.add_relations()->mutable_root();
    root_rel->mutable_input()->Swap(&state.rel);
    for (const auto & column : state.schema.columns)
        root_rel->add_names(column.name);

    extensions.declareUsedIn(plan);

    return plan;
}

/// A converter builds its relation by moving parts out of its children's states. So move only after the last check that can fail.
/// A converter that fails after moving leaves a child eligible but empty, and serializing it later gives an empty plan.
std::expected<SubstraitPlanSerializer::NodeState, SubstraitBlockingReason>
SubstraitPlanSerializer::convertStep(const QueryPlan::Node & node, std::vector<NodeState *> children)
{
    const IQueryPlanStep * step = node.step.get();

    if (const auto * read = typeid_cast<const ReadFromMergeTree *>(step))
        return convertReadFromMergeTree(*read);

    /// FilterStep and ExpressionStep share everything but the FilterRel wrapper.
    if (const auto * filter = typeid_cast<const FilterStep *>(step))
        return convertFilterOrExpression(
            filter->getExpression(), *filter->getOutputHeader(), filter->getFilterColumnName(), *children.at(0));
    if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
        return convertFilterOrExpression(expression->getExpression(), *expression->getOutputHeader(), std::nullopt, *children.at(0));

    if (const auto * aggregating = typeid_cast<const AggregatingStep *>(step))
        return convertAggregation(*aggregating, *children.at(0));

    if (const auto * sorting = typeid_cast<const SortingStep *>(step))
        return convertSorting(*sorting, *children.at(0));

    if (const auto * limit = typeid_cast<const LimitStep *>(step))
        return convertLimit(*limit, *children.at(0));

    if (const auto * offset = typeid_cast<const OffsetStep *>(step))
        return convertOffset(*offset, *children.at(0));

    if (typeid_cast<const UnionStep *>(step))
        return convertUnion(children);

    if (const auto * distinct = typeid_cast<const DistinctStep *>(step))
        return convertDistinct(*distinct, *children.at(0));

    if (const auto * join = typeid_cast<const JoinStepLogical *>(step))
        return convertJoin(*join, *children.at(0), *children.at(1));

    return blocked("the step has no Substrait representation");
}

std::expected<SubstraitPlanSerializer::NodeState, SubstraitBlockingReason>
SubstraitPlanSerializer::convertReadFromMergeTree(const ReadFromMergeTree & read)
{
    NodeState out;

    if (read.isQueryWithFinal())
        return blocked("FINAL is not supported");
    if (read.isQueryWithSampling())
        return blocked("SAMPLE is not supported");
    if (read.getPrewhereInfo() || read.getDeferredPrewhereInfo())
        return blocked("PREWHERE is not supported yet");
    if (read.getRowLevelFilter() || read.getDeferredRowLevelFilter())
        return blocked("row-level filters are not supported");

    const auto & header = *read.getOutputHeader();
    const auto & virtuals = read.getStorageSnapshot()->metadata->virtuals;
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

    const auto storage_id = read.getStorageID();
    auto * named_table = read_rel->mutable_named_table();
    named_table->add_names(storage_id.database_name);
    named_table->add_names(storage_id.table_name);
    return out;
}

std::expected<SubstraitPlanSerializer::NodeState, SubstraitBlockingReason> SubstraitPlanSerializer::convertFilterOrExpression(
    const ActionsDAG & dag, const Block & output_header, const std::optional<String> & filter_column_name, NodeState & child)
{
    NodeState out;

    SubstraitExpressionConverter converter(child.schema, extensions);

    substrait::Expression condition;
    if (filter_column_name)
    {
        const auto * condition_node = dag.tryFindInOutputs(*filter_column_name);
        if (!condition_node)
            return blocked(fmt::format("filter column {} is not among the expression outputs", *filter_column_name));

        auto converted = converter.convert(*condition_node);
        if (!converted)
            return std::unexpected(std::move(converted.error()));
        if (converted->type.kind_case() != substrait::Type::kBool)
            return blocked(fmt::format("filter condition {} is not a boolean", *filter_column_name));
        condition = std::move(converted->expression);
    }

    /// The child nests into a FilterRel when there is a condition, and the projection wraps the result.
    substrait::Rel rel;
    rel.Swap(&child.rel);
    if (filter_column_name)
    {
        substrait::Rel filter_rel;
        auto * filter_body = filter_rel.mutable_filter();
        filter_body->mutable_common()->mutable_direct();
        *filter_body->mutable_condition() = std::move(condition);
        *filter_body->mutable_input() = std::move(rel);
        rel = std::move(filter_rel);
    }
    auto converted = converter.convertProjection(dag, output_header, std::move(rel));
    if (!converted)
        return std::unexpected(std::move(converted.error()));
    out.rel = std::move(converted->rel);
    out.schema = std::move(converted->schema);
    return out;
}

std::expected<SubstraitPlanSerializer::NodeState, SubstraitBlockingReason>
SubstraitPlanSerializer::convertAggregation(const AggregatingStep & aggregating, NodeState & child)
{
    NodeState out;

    const auto & params = aggregating.getParams();

    if (!aggregating.isFinal())
        return blocked("partial aggregation states have no Substrait representation");
    if (aggregating.isGroupingSets())
        return blocked("GROUPING SETS are not supported yet");
    if (params.overflow_row)
        return blocked("aggregation with overflow row is not supported");
    if (params.top_k.has_value())
        return blocked("top-K aggregation is not supported");
    if (params.only_merge)
        return blocked("merging pre-aggregated states is not supported");

    substrait::Rel aggregate_rel;
    auto * aggregate = aggregate_rel.mutable_aggregate();

    auto * grouping = aggregate->add_groupings();
    for (size_t i = 0; i < params.keys.size(); ++i)
    {
        const auto position = child.schema.positionByName(params.keys[i]);
        if (!position)
            return blocked(fmt::format("grouping key {} is not present in the child schema", params.keys[i]));
        *aggregate->add_grouping_expressions() = makeColumnReference(*position);
        grouping->add_expression_references(static_cast<UInt32>(i));
        out.schema.append(params.keys[i], child.schema.columns[*position].type);
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
        {
            mapping.error().expression = description.column_name;
            return std::unexpected(std::move(mapping.error()));
        }

        auto * measure = aggregate->add_measures()->mutable_measure();
        measure->set_function_reference(extensions.internFunction(mapping->urn, mapping->signature));
        for (const auto position : argument_positions)
            *measure->add_arguments()->mutable_value() = makeColumnReference(position);
        addFunctionOptions(mapping->options, *measure);
        measure->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
        measure->set_invocation(mapping->invocation);
        *measure->mutable_output_type() = mapping->output_type;
        out.schema.append(description.column_name, std::move(mapping->output_type));
    }

    /// Without only_merge the step's output header is keys, then aggregates. The relation produces the same order.
    aggregate->mutable_common()->mutable_direct();
    aggregate->mutable_input()->Swap(&child.rel);
    out.rel = std::move(aggregate_rel);
    return out;
}

std::expected<SubstraitPlanSerializer::NodeState, SubstraitBlockingReason>
SubstraitPlanSerializer::convertSorting(const SortingStep & sorting, NodeState & child)
{
    NodeState out;

    /// Full, FinishSorting and MergingSorted produce the same fully sorted output and differ only in algorithm.
    /// PartitionedFinishSorting sorts each partition separately and never merges.
    /// SortRel describes the result, not the algorithm, so first three serialize, while the latter does not.
    if (sorting.getType() == SortingStep::Type::PartitionedFinishSorting)
        return blocked("partitioned sorting produces separately sorted partitions, not one sorted stream");
    if (!std::in_range<Int64>(sorting.getLimit()))
        return blocked("LIMIT or OFFSET does not fit a Substrait i64");

    substrait::Rel sort_rel;
    auto * sort = sort_rel.mutable_sort();
    sort->mutable_common()->mutable_direct();

    for (const auto & column : sorting.getSortDescription())
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
    if (sorting.getLimit() > 0)
        out.rel = makeFetchRel(std::move(sort_rel), static_cast<Int64>(sorting.getLimit()), 0);
    else
        out.rel = std::move(sort_rel);

    out.schema = std::move(child.schema);
    return out;
}

std::expected<SubstraitPlanSerializer::NodeState, SubstraitBlockingReason>
SubstraitPlanSerializer::convertLimit(const LimitStep & limit, NodeState & child)
{
    NodeState out;

    if (limit.withTies())
        return blocked("LIMIT WITH TIES is not supported");
    if (!std::in_range<Int64>(limit.getLimit()) || !std::in_range<Int64>(limit.getOffset()))
        return blocked("LIMIT or OFFSET does not fit a Substrait i64");

    out.rel = makeFetchRel(std::move(child.rel), static_cast<Int64>(limit.getLimit()), static_cast<Int64>(limit.getOffset()));
    out.schema = std::move(child.schema);
    return out;
}

std::expected<SubstraitPlanSerializer::NodeState, SubstraitBlockingReason>
SubstraitPlanSerializer::convertOffset(const OffsetStep & offset, NodeState & child)
{
    NodeState out;

    if (!std::in_range<Int64>(offset.getOffset()))
        return blocked("LIMIT or OFFSET does not fit a Substrait i64");

    out.rel = makeFetchRel(std::move(child.rel), {}, static_cast<Int64>(offset.getOffset()));
    out.schema = std::move(child.schema);
    return out;
}

std::expected<SubstraitPlanSerializer::NodeState, SubstraitBlockingReason>
SubstraitPlanSerializer::convertUnion(const std::vector<NodeState *> & children)
{
    NodeState out;

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

std::expected<SubstraitPlanSerializer::NodeState, SubstraitBlockingReason>
SubstraitPlanSerializer::convertDistinct(const DistinctStep & distinct, NodeState & child)
{
    NodeState out;

    /// TODO: ClickHouse DISTINCT preserves the input order but Substrait AggregateRel does not.
    if (child.sort_reaches_output)
        return blocked("DISTINCT over sorted input preserves the order in ClickHouse, which AggregateRel cannot express");

    /// DISTINCT over a subset of columns keeps arbitrary values of the others, which has no Substrait equivalent.
    /// TODO: come up with semantically honest translation/
    NameSet distinct_columns(distinct.getColumnNames().begin(), distinct.getColumnNames().end());
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

std::expected<SubstraitPlanSerializer::NodeState, SubstraitBlockingReason>
SubstraitPlanSerializer::convertJoin(const JoinStepLogical & join, NodeState & left, NodeState & right)
{
    NodeState out;

    const auto & join_operator = join.getJoinOperator();

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
    const auto combine_conjuncts
        = [&](const std::vector<JoinActionRef> & conjuncts) -> std::expected<substrait::Expression, SubstraitBlockingReason>
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
        return makeConjunction(std::move(converted_conjuncts), extensions);
    };

    substrait::Rel join_rel;
    if (!join_type)
    {
        if (!join_operator.expression.empty() || !join_operator.residual_filter.empty())
            return blocked("CROSS JOIN with a condition is not supported");
        auto * cross = join_rel.mutable_cross();
        cross->mutable_common()->mutable_direct();
        cross->mutable_left()->Swap(&left.rel);
        cross->mutable_right()->Swap(&right.rel);
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
        join_body->mutable_left()->Swap(&left.rel);
        join_body->mutable_right()->Swap(&right.rel);
    }

    /// A SEMI/ANTI join outputs only the kept side, so the projection on top resolves names against that side's schema alone.
    /// A name from the dropped side then fails to resolve. ClickHouse SEMI joins may project the dropped side but Substrait ones cannot.
    /// For INNER/CROSS the output is both sides, the schema the condition converter already works over, so it is reused.
    std::optional<SubstraitExpressionConverter> side_converter;
    if (post_join_schema != &combined)
        side_converter.emplace(*post_join_schema, extensions);
    auto & output_converter = side_converter ? *side_converter : converter;

    auto converted = output_converter.convertProjection(join.getActionsDAG(), *join.getOutputHeader(), std::move(join_rel));
    if (!converted)
        return std::unexpected(std::move(converted.error()));
    out.rel = std::move(converted->rel);
    out.schema = std::move(converted->schema);
    return out;
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
        String blocking_steps;
        for (const auto & [node, reason] : serializer.blockingSteps(*root))
        {
            blocking_steps += fmt::format("\n  {}", node->step->getName());
            if (const auto description = node->step->getStepDescription(); !description.empty())
                blocking_steps += fmt::format(" ({})", description);
            blocking_steps += fmt::format(": {}", reason.text);
            if (!reason.expression.empty())
                blocking_steps += fmt::format(", in {}", reason.expression);
        }
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The query plan cannot be serialized to Substrait. Blocking steps:{}", blocking_steps);
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
