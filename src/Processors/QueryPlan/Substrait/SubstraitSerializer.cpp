#include <Processors/QueryPlan/Substrait/SubstraitSerializer.h>

#if USE_SUBSTRAIT

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Block.h>
#include <Core/Joins.h>
#include <Core/Names.h>
#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/JoinUtils.h>
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

namespace Setting
{
extern const SettingsBool apply_mutations_on_fly;
extern const SettingsBool apply_patch_parts;
}

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
    /// `keyed` is emitted where that cannot show with GROUP BY or a Nullable argument. `keyless` returns the default, as `sum0` does.
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

substrait::Type makeAggregateOutputType(std::string_view output_short, bool nullable, SubstraitPlanExtensions & extensions)
{
    if (const auto type_name = tryParseUserDefinedShortName(output_short); !type_name.empty())
        return substraitUnsignedType(type_name, nullable, extensions);
    return makeSimpleSubstraitType(output_short, nullable);
}

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

/// With an overflow mode other than `throw` CH returns a part of the result once the limit is reached. No Substrait relation does that.
bool truncatesResult(const SizeLimits & limits) { return limits.hasLimits() && limits.overflow_mode != OverflowMode::THROW; }

/// ClickHouse join keys match NaN with NaN and tell -0 from 0. Substrait `equal` does neither.
bool hasFloatKey(const ActionsDAG::Node & node)
{
    const bool is_equality = node.type == ActionsDAG::ActionType::FUNCTION && node.function_base->getName() == "equals";
    if (is_equality && std::ranges::any_of(node.children, JoinCommon::keyMatchesValuesEqualsRejects, &ActionsDAG::Node::result_type))
        return true;
    return std::ranges::any_of(node.children, [](const auto * child) { return hasFloatKey(*child); });
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

/// The header is ClickHouse's list of the step's output columns, the schema is the list we wrote for Substrait. They must agree.
void checkSchemaMirrorsHeader(const SubstraitSchema & schema, const IQueryPlanStep & step)
{
    const auto & header = *step.getOutputHeader();
    if (!std::ranges::equal(schema.columns, header, {}, &SubstraitColumn::name, &ColumnWithTypeAndName::name))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Step {} converted to Substrait with columns {}, but its header has {}",
            step.getName(),
            fmt::join(schema.columns | std::views::transform(&SubstraitColumn::name), ", "),
            header.dumpNames());

    for (size_t i = 0; i < schema.size(); ++i)
        if (isNullableOrLowCardinalityNullable(header.getByPosition(i).type) && !isSubstraitTypeNullable(schema.columns[i].type))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Step {} converted to Substrait with a required column {}, but it is Nullable in ClickHouse",
                step.getName(),
                schema.columns[i].name);
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
            const auto * child = frame.node->children[frame.next_child++];
            stack.push_back({.node = child});
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

        if (!children_eligible)
            continue;

        auto converted = convertStep(node, std::move(children));
        if (converted)
        {
            checkSchemaMirrorsHeader(converted->schema, *node.step);
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

/// A converter builds its relation by moving parts out of its children's states.
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

    if (&read.getStorageSnapshot()->storage != &read.getMergeTreeData())
        return blocked("reading from a projection is not supported");
    if (read.isQueryWithFinal())
        return blocked("FINAL is not supported");
    if (read.getQueryInfo().isStream())
        return blocked("STREAM is not supported");
    if (read.isQueryWithSampling())
        return blocked("SAMPLE is not supported");
    if (!read.getReaderSettings().apply_deleted_mask)
        return blocked("apply_deleted_mask = 0 is not supported");
    const auto & settings = read.getContext()->getSettingsRef();
    if (settings[Setting::apply_mutations_on_fly])
        return blocked("apply_mutations_on_fly is not supported");
    if (!settings[Setting::apply_patch_parts])
        return blocked("apply_patch_parts = 0 is not supported");
    if (read.getPrewhereInfo() || read.getDeferredPrewhereInfo())
        return blocked("PREWHERE is not supported yet");
    if (read.getRowLevelFilter() || read.getDeferredRowLevelFilter())
        return blocked("row-level filters are not supported");

    if (const auto & storage_limits = read.getQueryInfo().storage_limits)
    {
        for (const auto & limits : *storage_limits)
        {
            const auto & local_limits = limits.local_limits;
            if (truncatesResult(local_limits.size_limits))
                return blocked("max_rows_to_read or max_bytes_to_read with read_overflow_mode = 'break' is not supported");
            if (truncatesResult(limits.leaf_limits))
                return blocked("max_rows_to_read_leaf or max_bytes_to_read_leaf with read_overflow_mode_leaf = 'break' is not supported");
            if (local_limits.speed_limits.max_execution_time != 0 && local_limits.timeout_overflow_mode != OverflowMode::THROW)
                return blocked("max_execution_time with timeout_overflow_mode = 'break' is not supported");
        }
    }

    const auto & header = *read.getOutputHeader();
    const auto & table_columns = read.getStorageSnapshot()->metadata->getColumns();
    for (const auto & column : header)
        if (!table_columns.hasPhysical(column.name))
            return blocked(fmt::format("column {} is not a physical column of the table", column.name));

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

        auto converted = converter.convertCondition(*condition_node);
        if (!converted)
            return std::unexpected(std::move(converted.error()));
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
    if (params.keys.empty() && params.empty_result_for_aggregation_by_empty_set)
        return blocked("aggregation without keys that returns no rows over empty input is not supported");
    if (truncatesResult({params.max_rows_to_group_by, 0, params.group_by_overflow_mode}))
        return blocked("max_rows_to_group_by with group_by_overflow_mode = 'any' or 'break' is not supported");

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
    if (truncatesResult(sorting.getSettings().size_limits))
        return blocked("max_rows_to_sort or max_bytes_to_sort with sort_overflow_mode = 'break' is not supported");
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

        const bool nulls_last = column.nulls_direction == column.direction;

        /// Substrait leaves NaN placement to the consumer. A leading `is_nan` key puts it between the values and NULL, as in ClickHouse.
        const auto & key_type = child.schema.columns[*position].type;
        if (isSubstraitFloatShort(substraitTypeShortName(key_type, extensions)))
        {
            auto * nan_field = sort->add_sorts();
            *nan_field->mutable_expr() = makeIsNan(*position, key_type, extensions);
            nan_field->set_direction(
                nulls_last ? substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST
                           : substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST);
        }

        auto * field = sort->add_sorts();
        *field->mutable_expr() = makeColumnReference(*position);
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

    if (truncatesResult(distinct.getSettings().set_size_limits))
        return blocked("max_rows_in_distinct or max_bytes_in_distinct with distinct_overflow_mode = 'break' is not supported");

    /// TODO: ClickHouse DISTINCT preserves the input order but Substrait AggregateRel does not.
    if (child.sort_reaches_output)
        return blocked("DISTINCT over sorted input preserves the order in ClickHouse, which AggregateRel cannot express");

    /// DISTINCT over a subset of columns keeps arbitrary values of the others, which has no Substrait equivalent.
    /// TODO: come up with semantically honest translation.
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
    const auto & join_settings = join.getSettings();
    if (truncatesResult({join_settings.max_rows_in_join, join_settings.max_bytes_in_join, join_settings.join_overflow_mode}))
        return blocked("max_rows_in_join or max_bytes_in_join with join_overflow_mode = 'break' is not supported");
    if (truncatesResult(join.getSortingSettings().size_limits))
        return blocked("max_rows_to_sort or max_bytes_to_sort with sort_overflow_mode = 'break' is not supported");

    /// JoinRel direct output is all left fields then all right fields.
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
        = [&](const VectorWithMemoryTracking<JoinActionRef> & conjuncts) -> std::expected<substrait::Expression, SubstraitBlockingReason>
    {
        std::vector<SubstraitExpressionConverter::ConvertedExpression> converted_conjuncts;
        for (const auto & conjunct : conjuncts)
        {
            auto converted = converter.convertCondition(*conjunct.getNode());
            if (!converted)
                return std::unexpected(std::move(converted.error()));
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
        if (std::ranges::any_of(join_operator.expression, [](const auto & conjunct) { return hasFloatKey(*conjunct.getNode()); }))
            return blocked("JOIN on a floating-point key is not supported");
        auto * join_body = join_rel.mutable_join();
        join_body->mutable_common()->mutable_direct();
        join_body->set_type(*join_type);
        auto condition = combine_conjuncts(join_operator.expression);
        if (!condition)
            return std::unexpected(std::move(condition.error()));
        *join_body->mutable_expression() = std::move(*condition);
        if (!join_operator.residual_filter.empty())
        {
            if (post_join_schema != &combined)
                return blocked(fmt::format("{} JOIN with a post-join filter is not supported", toString(join_operator.strictness)));
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

namespace
{

/// The optimizations whose plans the serializer can express.

/// Optimizations left out that we can add later. TODO:
/// * `optimize_prewhere` moves a filter into the read step. The serializer has to write PREWHERE as `ReadRel.filter`.
/// * `enable_group_by_top_k_optimization` drops the groups that the limit above would drop. It needs a check for that limit and PREWHERE.
/// * `convert_join_to_in` replaces a join by `in` over a `CreatingSet` step. The serializer has to map `in` together with its set.

/// Optimizations left out that we can't add as there is no Substrait form.
/// * `enable_join_runtime_filters`.
/// * `min_columns_for_join_lazy_indexing`.

constexpr bool QueryPlanOptimizationSettings::* substrait_compatible_optimizations[] = {
    &QueryPlanOptimizationSettings::remove_redundant_sorting,
    &QueryPlanOptimizationSettings::lift_up_array_join,
    &QueryPlanOptimizationSettings::push_down_limit,
    &QueryPlanOptimizationSettings::aggregation_bucket_top_k,
    &QueryPlanOptimizationSettings::split_filter,
    &QueryPlanOptimizationSettings::merge_expressions,
    &QueryPlanOptimizationSettings::merge_filters,
    &QueryPlanOptimizationSettings::filter_push_down,
    &QueryPlanOptimizationSettings::propagate_predicate_across_join,
    &QueryPlanOptimizationSettings::fuse_filter_into_array_join,
    &QueryPlanOptimizationSettings::lower_array_join_function,
    &QueryPlanOptimizationSettings::push_down_volume_reducing_functions,
    &QueryPlanOptimizationSettings::convert_outer_join_to_inner_join,
    &QueryPlanOptimizationSettings::convert_outer_join_to_inner_join_transitively,
    &QueryPlanOptimizationSettings::short_circuit_constant_false_join,
    &QueryPlanOptimizationSettings::execute_functions_after_sorting,
    &QueryPlanOptimizationSettings::reuse_storage_ordering_for_window_functions,
    &QueryPlanOptimizationSettings::lift_up_union,
    &QueryPlanOptimizationSettings::aggregate_partitions_independently,
    &QueryPlanOptimizationSettings::limit_by_partitions_independently,
    &QueryPlanOptimizationSettings::distinct_partitions_independently,
    &QueryPlanOptimizationSettings::window_partitions_independently,
    &QueryPlanOptimizationSettings::creating_set_partitions_independently,
    &QueryPlanOptimizationSettings::remove_redundant_distinct,
    &QueryPlanOptimizationSettings::try_use_vector_search,
    &QueryPlanOptimizationSettings::merge_filter_into_join_condition,
    &QueryPlanOptimizationSettings::merge_expression_into_join,
    &QueryPlanOptimizationSettings::convert_any_join_to_semi_or_anti_join,
    &QueryPlanOptimizationSettings::top_k_through_join,
    &QueryPlanOptimizationSettings::remove_unused_columns,
    &QueryPlanOptimizationSettings::aggregation_having_prefilter,
    &QueryPlanOptimizationSettings::read_in_order,
    &QueryPlanOptimizationSettings::distinct_in_order,
    &QueryPlanOptimizationSettings::limit_by_in_order,
    &QueryPlanOptimizationSettings::optimize_sorting_by_input_stream_properties,
    &QueryPlanOptimizationSettings::aggregation_in_order,
    &QueryPlanOptimizationSettings::optimize_aggregation_in_order_limit,
    &QueryPlanOptimizationSettings::push_limit_by_into_sort,
};

}

void disableSubstraitIncompatiblePlanning(const ContextMutablePtr & context)
{
    context->setSetting("optimize_functions_to_subcolumns", false); /// Should be possible in future.
    context->setSetting("optimize_trivial_count_query", false); /// I don't see how to serialize this right now.
    context->setSetting("correlated_subqueries_use_in_memory_buffer", false); /// No Substrait form for this.
    context->setSetting("optimize_trivial_group_by_limit_query", false); /// Should be possible in future.
}

QueryPlanOptimizationSettings makeSubstraitCompatibleOptimizationSettings(const ContextPtr & context)
{
    auto context_without_optimizations = Context::createCopy(context);
    context_without_optimizations->setSetting("query_plan_enable_optimizations", false);
    context_without_optimizations->setSetting("optimize_use_projections", false);
    QueryPlanOptimizationSettings settings(context_without_optimizations);

    settings.optimize_lazy_materialization = false; /// No Substrait form for `LazilyReadFromMergeTree`.
    settings.try_use_top_k_optimization = false; /// Substrait can only express fixed conditions.

    const QueryPlanOptimizationSettings from_query_settings(context);
    settings.optimize_plan = from_query_settings.optimize_plan;
    for (const auto optimization : substrait_compatible_optimizations)
        settings.*optimization = from_query_settings.*optimization;

    settings.keep_logical_steps = true;
    return settings;
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
