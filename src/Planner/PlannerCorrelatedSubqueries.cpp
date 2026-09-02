#include <Planner/PlannerCorrelatedSubqueries.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>

#include <Common/EquivalenceClasses.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <Columns/ColumnConst.h>

#include <Core/Joins.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinOperator.h>

#include <Parsers/SelectUnionMode.h>

#include <Planner/Planner.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerJoinsLogical.h>
#include <Planner/Utils.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/UnionStep.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IStorage.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{

extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;

}

namespace Setting
{

extern const SettingsBool correlated_subqueries_substitute_equivalent_expressions;
extern const SettingsBool correlated_subqueries_use_in_memory_buffer;
extern const SettingsBool join_use_nulls;
extern const SettingsBool use_variant_as_common_type;
extern const SettingsDecorrelationJoinKind correlated_subqueries_default_join_kind;
extern const SettingsMaxThreads max_threads;
extern const SettingsNonZeroUInt64 max_block_size;

}

void CorrelatedSubtrees::assertEmpty(std::string_view reason) const
{
    if (notEmpty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Correlated subqueries {} are not supported", reason);
}

namespace
{

/// The joins built during decorrelation are internal implementation details, not user joins, so
/// the user's join size limits must not apply to them. In particular, under join_overflow_mode =
/// 'break' a size limit lets the build side stop early and drop rows, which both yields a wrong
/// subquery result and lets the probe side start before the build side has fully consumed its
/// input (the source of the ChunkBuffer / runtime-filter "before all inputs are finished" logical
/// errors). Run such joins unbounded with THROW.
void makeInternalDecorrelationJoinUnbounded(JoinStepLogical & join_step)
{
    auto & join_settings = join_step.getJoinSettings();
    join_settings.max_rows_in_join = 0;
    join_settings.max_bytes_in_join = 0;
    join_settings.join_overflow_mode = OverflowMode::THROW;
}

using CorrelatedPlanStepMap = std::unordered_map<QueryPlan::Node *, bool>;

CorrelatedPlanStepMap buildCorrelatedPlanStepMap(QueryPlan & correlated_query_plan)
{
    CorrelatedPlanStepMap result;

    struct State
    {
        QueryPlan::Node * node;
        bool processed_children = false;
    };

    std::vector<State> nodes_to_process{ { .node = correlated_query_plan.getRootNode() } };
    while (!nodes_to_process.empty())
    {
        size_t current_index = nodes_to_process.size() - 1;
        if (nodes_to_process[current_index].processed_children)
        {
            auto * current = nodes_to_process[current_index].node;

            auto & value = result[current];
            value = current->step->hasCorrelatedExpressions();

            for (auto * child : current->children)
                value |= result[child];

            nodes_to_process.pop_back();
        }
        else
        {
            for (auto * child : nodes_to_process[current_index].node->children)
                nodes_to_process.push_back({ .node = child });
            nodes_to_process[current_index].processed_children = true;
        }
    }

    return result;
}

/// A column name with its type for equivalence tracking. The planner assigns unique names
/// to columns, so hash and equality use only the name. The type is a payload used by the
/// substitution to build a conversion between the types of equivalent columns.
struct ColumnWithType
{
    String name;
    DataTypePtr type;

    bool operator==(const ColumnWithType & other) const { return name == other.name; }
};

struct ColumnWithTypeHash
{
    size_t operator()(const ColumnWithType & column) const { return std::hash<String>{}(column.name); }
};

using ColumnEquivalenceClasses = EquivalenceClasses<ColumnWithType, ColumnWithTypeHash>;

/// Per-UNION-arm decorrelation scope (see the UnionStep handler): equivalence classes from one
/// arm must not leak into another, while usage restrictions inherited from shared ancestor
/// steps apply to every arm.
struct DecorrelationScope
{
    ColumnEquivalenceClasses equivalence_classes;
    /// Correlated identifiers used in this scope anywhere except the recorded equality
    /// conjuncts. Substituting them would evaluate those expressions on the inner values
    /// (including rows that never match any outer value), so e.g. a division that is safe
    /// over the outer domain could throw.
    std::unordered_set<String> identifiers_used_outside_equalities;
};

struct DecorrelationContext
{
    const CorrelatedSubquery & correlated_subquery;
    const PlannerContextPtr & planner_context;
    QueryPlan query_plan; // LHS plan
    QueryPlan correlated_query_plan;
    CorrelatedPlanStepMap correlated_plan_steps;
    /// Scope stack for subqueries. Equivalence classes should not be propagated
    /// to the subqueries of the JOIN or UNION steps.
    std::vector<DecorrelationScope> scope_stack;
    /// Whether the optimizer will turn the referenced input subplan into an in-memory ChunkBuffer.
    /// Decided once here (see decorrelateQueryPlan); buildLogicalJoin uses it to pick the join kind.
    bool uses_in_memory_buffer = false;
};

/// How to convert an equivalent column to exactly the correlated column type.
struct SubstitutionConversion
{
    /// The base types differ and do not embed losslessly: an exactness-checking cast is needed.
    bool needs_accurate_cast = false;
    /// Rows whose value cannot match any correlated-typed value must be dropped up front.
    bool needs_null_prefilter = false;
    /// 0 = same type, 1 = lossless conversion, 2 = conversion with a pre-filter. Lower is better.
    size_t rank = 0;
};

/// Classifies the conversion of an equivalent member column to the correlated column type.
/// `Nullable` and `LowCardinality` are value-transparent wrappers, so only the base types
/// base(T) = removeNullable(removeLowCardinality(T)) decide. Returns std::nullopt when the member
/// is unusable for substitution.
std::optional<SubstitutionConversion> classifySubstitutionConversion(const DataTypePtr & member_type, const DataTypePtr & correlated_type)
{
    SubstitutionConversion conversion;

    auto member_base = removeNullable(removeLowCardinality(member_type));
    auto correlated_base = removeNullable(removeLowCardinality(correlated_type));

    /// A float correlated column is unusable for ANY substitution, exact-type members included.
    /// Float representations are not unique (`equals` merges `-0.0` and `+0.0`, hash joins and
    /// aggregation compare bitwise), and an exact-type float member may be linked to the correlated
    /// column through cross-type equalities that the non-substituted plan evaluates with `equals`
    /// semantics (e.g. `i.a = o.x AND i.a = i.b` with an `Int32` bridge: the fallback keeps the
    /// outer `-0.0`, the reconstructed `+0.0` does not hash-join with it). The equivalence classes
    /// are flat, so a bridged member cannot be told apart from a direct one.
    if (isFloat(correlated_base))
        return std::nullopt;

    /// `Bool` is a custom-named `UInt8` and `IDataType::equals` does not distinguish them, so a
    /// plain `UInt8` member would take the exact/equal-base paths and alias a value like `2`
    /// into the `Bool` correlated column, breaking the value domain expressions and optimizers
    /// assume; the cross-base cast is unfaithful too (`accurateCastOrNull` into `Bool` maps every
    /// non-zero value to `true`). The custom name is load-bearing: reconstruction into `Bool`
    /// requires a `Bool` member.
    if (isBool(correlated_base) && !isBool(member_base))
        return std::nullopt;

    /// `IDataType::equals` compares the storage type and ignores semantically significant
    /// attributes (`DateTime` timezones, custom names such as `Bool`), so the full type names are
    /// compared instead: an aliased column keeps the member's attributes, and e.g. `toHour` over a
    /// correlated `DateTime('UTC')` reconstructed from a `DateTime('Asia/Tokyo')` member would
    /// change meaning. Attribute-mismatched pairs fall through to the cross-base branch, whose
    /// `isNativeNumber` check rejects them (CROSS JOIN fallback — conservative; a lossless
    /// timezone-reinterpreting `CAST` could be a future improvement).
    if (member_type->getName() == correlated_type->getName())
        return conversion;

    if (member_base->getName() != correlated_base->getName())
    {
        if (!isNativeNumber(member_base) || !isNativeNumber(correlated_base))
            return std::nullopt;
        auto supertype = tryGetLeastSupertype(DataTypes{member_base, correlated_base});
        if (!supertype)
            return std::nullopt;
        /// A widening into the correlated base is lossless; anything else must be checked per value.
        conversion.needs_accurate_cast = !supertype->equals(*correlated_base);
    }

    conversion.needs_null_prefilter = conversion.needs_accurate_cast
        || (isNullableOrLowCardinalityNullable(member_type) && !isNullableOrLowCardinalityNullable(correlated_type));
    conversion.rank = conversion.needs_null_prefilter ? 2 : 1;
    return conversion;
}

/// Whether an equality between columns of these two types may be recorded as an equivalence
/// (see the recording site for the full reasoning: wrapper-transparent equal bases, or native
/// numbers with a least supertype — pairs whose comparison semantics are consistent with CAST).
bool isRecordableEquivalencePair(const DataTypePtr & lhs_type, const DataTypePtr & rhs_type)
{
    auto lhs_base = removeNullable(removeLowCardinality(lhs_type));
    auto rhs_base = removeNullable(removeLowCardinality(rhs_type));
    bool equal_bases = lhs_base->equals(*rhs_base);
    bool safe_number_pair = isNativeNumber(lhs_base) && isNativeNumber(rhs_base)
        && tryGetLeastSupertype(DataTypes{lhs_base, rhs_base}) != nullptr;
    return equal_bases || safe_number_pair;
}

/// A renaming of a correlated column to an equivalent column of the decorrelated subplan.
struct ExpressionRenaming
{
    const ActionsDAG::Node * source;
    String correlated_name;
    DataTypePtr correlated_type;
    SubstitutionConversion conversion;
};

/// Builds accurateCastOrNull(node, 'TypeName'): NULL for values that do not convert exactly,
/// the result type is Nullable(target_type).
const ActionsDAG::Node & addAccurateCastOrNull(
    ActionsDAG & dag,
    const ActionsDAG::Node & node,
    const DataTypePtr & target_type,
    const ContextPtr & query_context)
{
    auto type_name = target_type->getName();
    auto type_name_column = DataTypeString().createColumnConst(0, type_name);
    /// The name is prefixed to avoid a collision with a column of the subplan header.
    const auto & type_name_node = dag.addColumn(std::move(type_name_column), std::make_shared<DataTypeString>(), "__correlated_cast_type_" + type_name);
    return dag.addFunction(FunctionFactory::instance().get("accurateCastOrNull", query_context), {&node, &type_name_node}, {});
}

/// Collects correlated identifiers (PLACEHOLDER nodes) whose uses are not limited to the recorded
/// equality conjuncts. The allowed consumers are exactly the recorded equality nodes; any other
/// edge into a placeholder, and any placeholder that is itself an output (e.g. a projection column,
/// the filter column, or a bare conjunct consumed by the `and` combiner), counts as a use.
void collectIdentifiersUsedOutsideEqualities(
    const ActionsDAG & dag,
    const std::unordered_set<const ActionsDAG::Node *> & recorded_equalities,
    std::unordered_set<String> & result)
{
    for (const auto & node : dag.getNodes())
    {
        if (recorded_equalities.contains(&node))
            continue;
        for (const auto * child : node.children)
        {
            if (child->type == ActionsDAG::ActionType::PLACEHOLDER)
                result.insert(child->result_name);
        }
    }

    for (const auto * output : dag.getOutputs())
    {
        if (output->type == ActionsDAG::ActionType::PLACEHOLDER)
            result.insert(output->result_name);
    }
}

/// Traces an arm-local column into one union arm: the caller maps the union output column to the
/// arm positionally; this function only follows the arm's correlated single-child chain down to
/// the uncorrelated boundary (the substitution point), translating the name through pure renames.
/// Returns the boundary column or std::nullopt when the column is computed, dropped, or the chain
/// contains a step that may change row identity.
std::optional<ColumnWithType> traceUnionColumnIntoArm(
    QueryPlan::Node * arm_root,
    const String & arm_column_name,
    CorrelatedPlanStepMap & correlated_plan_steps)
{
    String name = arm_column_name;
    QueryPlan::Node * node = arm_root;

    while (correlated_plan_steps[node])
    {
        if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
        {
            const ActionsDAG::Node * output_node = nullptr;
            for (const auto * output : expression_step->getExpression().getOutputs())
            {
                if (output->result_name == name)
                {
                    output_node = output;
                    break;
                }
            }
            if (!output_node)
                return std::nullopt;
            while (output_node->type == ActionsDAG::ActionType::ALIAS)
                output_node = output_node->children.front();
            /// Anything but a plain input is a computed column.
            if (output_node->type != ActionsDAG::ActionType::INPUT)
                return std::nullopt;
            name = output_node->result_name;
        }
        else if (typeid_cast<FilterStep *>(node->step.get()))
        {
            /// Names pass through unchanged (the filter only adds/removes its filter column).
        }
        else
        {
            /// Aggregation, nested union, or anything that may rename or regroup.
            return std::nullopt;
        }

        chassert(!node->children.empty());
        if (node->children.empty())
            return std::nullopt;
        node = node->children.front();
    }

    const auto & boundary_header = node->step->getOutputHeader();
    if (!boundary_header->has(name))
        return std::nullopt;
    return ColumnWithType{name, boundary_header->getByName(name).type};
}

/// Correlated subquery is represented by implicit dependent join operator.
/// This function builds a query plan to evaluate correlated subquery by
/// pushing dependent join down and replacing it with CROSS JOIN.
QueryPlan decorrelateQueryPlan(
    DecorrelationContext & context,
    QueryPlan::Node * node
)
{
    if (!context.correlated_plan_steps[node])
    {
        /// The rest of the query plan doesn't use any correlated columns.
        const auto & settings = context.planner_context->getQueryContext()->getSettingsRef();

        if (settings[Setting::correlated_subqueries_substitute_equivalent_expressions])
        {
            const auto & query_context = context.planner_context->getQueryContext();
            const auto & decorrelated_plan_header = node->step->getOutputHeader();
            ActionsDAG dag(decorrelated_plan_header->getNamesAndTypesList());
            auto & outputs = dag.getOutputs();

            std::unordered_map<std::string_view, const ActionsDAG::Node *> decorrelated_nodes_names;
            for (const auto * output : outputs)
                decorrelated_nodes_names[output->result_name] = output;

            /// Find possible renamings for all correlated columns
            std::vector<ExpressionRenaming> expression_renamings;
            for (const auto & correlated_column_identifier : context.correlated_subquery.correlated_column_identifiers)
            {
                /// A use outside the recorded equalities would be evaluated on the substituted inner
                /// values, including rows that never match any outer value (e.g. a division that is
                /// safe over the outer domain could throw), so such identifiers are never substituted.
                if (context.scope_stack.back().identifiers_used_outside_equalities.contains(correlated_column_identifier))
                    continue;

                /// Hash and equality use only the name, so the type may be left empty for the lookup.
                auto equivalence_class = context.scope_stack.back().equivalence_classes.getClass(ColumnWithType{correlated_column_identifier, nullptr});
                if (!equivalence_class)
                    continue;

                /// The type of the correlated placeholder. The substituted column must have exactly
                /// this type: decorrelated DAGs above declare it for their inputs, and a column of a
                /// different runtime type would be a logical error during execution.
                DataTypePtr correlated_type;
                for (const auto & member : *equivalence_class)
                {
                    if (member.name == correlated_column_identifier)
                    {
                        correlated_type = member.type;
                        break;
                    }
                }
                chassert(correlated_type != nullptr);
                if (!correlated_type)
                    continue;

                std::optional<ExpressionRenaming> best_renaming;
                for (const auto & member : *equivalence_class)
                {
                    auto it = decorrelated_nodes_names.find(member.name);
                    if (it == decorrelated_nodes_names.end())
                        continue;
                    /// Harden against a name collision with an unrelated column of the subplan.
                    if (!it->second->result_type->equals(*member.type))
                        continue;

                    auto conversion = classifySubstitutionConversion(member.type, correlated_type);
                    if (!conversion)
                        continue;

                    if (!best_renaming || conversion->rank < best_renaming->conversion.rank)
                        best_renaming = ExpressionRenaming{it->second, correlated_column_identifier, correlated_type, *conversion};

                    if (best_renaming->conversion.rank == 0)
                        break;
                }

                if (best_renaming)
                    expression_renamings.push_back(std::move(*best_renaming));
            }

            /// If all columns from outer query have equivalent expressions in the current subplan,
            /// we can safely replace them and avoid introduction of CROSS JOIN.
            if (context.correlated_subquery.correlated_column_identifiers.size() == expression_renamings.size())
            {
                auto & function_factory = FunctionFactory::instance();

                bool needs_prefilter = false;
                for (const auto & renaming : expression_renamings)
                {
                    needs_prefilter |= renaming.conversion.needs_null_prefilter;

                    const ActionsDAG::Node * substituted = renaming.source;
                    if (renaming.conversion.needs_accurate_cast)
                        substituted = &addAccurateCastOrNull(
                            dag, *substituted, removeNullable(removeLowCardinality(renaming.correlated_type)), query_context);
                    /// `assumeNotNull` (and `accurateCastOrNull`) is used instead of a throwing `CAST`
                    /// deliberately: the plan optimizer may merge the pre-filter and this expression
                    /// into a single step, which computes expressions on the not-yet-filtered rows, so
                    /// the conversion must not throw on rows the filter drops. The final `CAST` below
                    /// never sees a NULL it cannot represent for the same reason.
                    if (isNullableOrLowCardinalityNullable(substituted->result_type) && !isNullableOrLowCardinalityNullable(renaming.correlated_type))
                        substituted = &dag.addFunction(function_factory.get("assumeNotNull", query_context), {substituted}, {});
                    /// The remaining difference is a lossless wrapper change or numeric widening.
                    /// The named cast also serves as the renaming alias.
                    /// Compare full type names like classifySubstitutionConversion does: `IDataType::equals` cannot
                    /// tell `Bool` from `UInt8`, and aliasing would smuggle the member's custom name into the
                    /// correlated column (e.g. `toString` would render `true` instead of `1`). The `CAST` is an
                    /// identity on the values for such storage-equal pairs.
                    if (substituted->result_type->getName() != renaming.correlated_type->getName())
                        substituted = &dag.addCast(*substituted, renaming.correlated_type, renaming.correlated_name, query_context);
                    else
                        substituted = &dag.addAlias(*substituted, renaming.correlated_name);

                    chassert(substituted->result_type->getName() == renaming.correlated_type->getName());
                    outputs.push_back(substituted);
                }

                auto result_plan = context.correlated_query_plan.extractSubplan(node);

                if (needs_prefilter)
                {
                    /// Rows whose member value is NULL or does not convert exactly to the correlated
                    /// type can never satisfy the recorded equality (a top-level AND-conjunct of an
                    /// ancestor FilterStep), so they are dropped up front. This guarantees the
                    /// default values produced by assumeNotNull for such rows never exist in any
                    /// downstream step: under filter/expression step merging and short-circuit
                    /// settings, the retained recorded equality itself is evaluated on the
                    /// not-yet-filtered rows.
                    /// The retained recorded equality would still drop these rows by itself, but
                    /// only after downstream expressions computed on the garbage values.
                    ActionsDAG filter_dag(decorrelated_plan_header->getNamesAndTypesList());
                    ActionsDAG::NodeRawConstPtrs conditions;
                    std::unordered_set<String> deduplicated_conditions;
                    for (const auto & renaming : expression_renamings)
                    {
                        if (!renaming.conversion.needs_null_prefilter)
                            continue;

                        DataTypePtr cast_target;
                        String condition_key = renaming.source->result_name;
                        if (renaming.conversion.needs_accurate_cast)
                        {
                            /// The same member may be converted to different types for different
                            /// correlated columns, so the target type is a part of the key.
                            cast_target = removeNullable(removeLowCardinality(renaming.correlated_type));
                            condition_key += '\0';
                            condition_key += cast_target->getName();
                        }
                        if (!deduplicated_conditions.insert(condition_key).second)
                            continue;

                        const ActionsDAG::Node * checked = &filter_dag.findInOutputs(renaming.source->result_name);
                        if (cast_target)
                            checked = &addAccurateCastOrNull(filter_dag, *checked, cast_target, query_context);
                        conditions.push_back(&filter_dag.addFunction(function_factory.get("isNotNull", query_context), {checked}, {}));
                    }

                    const auto * filter_condition = conditions.front();
                    if (conditions.size() > 1)
                        filter_condition = &filter_dag.addFunction(function_factory.get("and", query_context), std::move(conditions), {});

                    /// An explicit unique name: an auto-generated function name could collide with an
                    /// existing column of the subplan header.
                    String filter_column_name = "__correlated_not_null_" + context.correlated_subquery.action_node_name;
                    filter_dag.getOutputs().push_back(&filter_dag.addAlias(*filter_condition, filter_column_name));

                    auto filter_step = std::make_unique<FilterStep>(
                        result_plan.getCurrentHeader(), std::move(filter_dag), filter_column_name, /*remove_filter_column_=*/true);
                    filter_step->setStepDescription("Filter values of expressions equivalent to correlated columns that cannot match");
                    result_plan.addStep(std::move(filter_step));
                }

                auto renaming_step = std::make_unique<ExpressionStep>(result_plan.getCurrentHeader(), std::move(dag));
                renaming_step->setStepDescription("Renaming correlated columns to equivalent expressions in subquery");
                result_plan.addStep(std::move(renaming_step));
                return result_plan;
            }
        }
        /// Either context can be the one that builds the QueryPlanOptimizationSettings and creates the
        /// buffer, so neither alone is authoritative: keep the protection when either would buffer.
        auto would_buffer = [](const Settings & settings_to_check)
        {
            return settings_to_check[Setting::correlated_subqueries_use_in_memory_buffer]
                && settings_to_check[Setting::correlated_subqueries_default_join_kind] == DecorrelationJoinKind::RIGHT;
        };
        const auto top_level_context = context.planner_context->getQueryContext();
        context.uses_in_memory_buffer = would_buffer(settings)
            || (top_level_context->hasQueryContext() && would_buffer(top_level_context->getQueryContext()->getSettingsRef()));

        QueryPlan lhs_plan = context.correlated_query_plan.extractSubplan(node);
        QueryPlan rhs_plan;

        /// The inner subplan can have zero output columns when it only contributes cardinality (e.g. an
        /// EXISTS body reduced to a bare filter). Such a relation loses its row count on the streamed side
        /// of a join (Block::rows() == 0 with no columns), so the join would drop all rows. Add a
        /// materialized placeholder column to carry the row count across the join; it is stripped again
        /// right after the join (see below) so it stays internal to this branch.
        std::optional<String> row_marker_name;
        if (lhs_plan.getCurrentHeader()->columns() == 0)
        {
            ActionsDAG marker_dag(lhs_plan.getCurrentHeader()->getNamesAndTypesList());
            auto marker_type = std::make_shared<DataTypeUInt8>();
            auto marker_column = marker_type->createColumnConst(0, 0u);
            row_marker_name = "__correlated_subquery_row_marker_" + context.correlated_subquery.action_node_name;
            marker_dag.getOutputs() = { &marker_dag.materializeNode(marker_dag.addColumn(std::move(marker_column), marker_type, *row_marker_name)) };

            auto marker_step = std::make_unique<ExpressionStep>(lhs_plan.getCurrentHeader(), std::move(marker_dag));
            marker_step->setStepDescription("Row marker for zero-column correlated subquery body");
            lhs_plan.addStep(std::move(marker_step));
        }

        auto default_join_kind = settings[Setting::correlated_subqueries_default_join_kind];
        context.query_plan.addStep(std::make_unique<CommonSubplanStep>(context.query_plan.getCurrentHeader()));

        auto buffer_header = std::make_shared<Block>();
        const auto & input_header = context.query_plan.getCurrentHeader();
        for (const auto & column : context.correlated_subquery.correlated_column_identifiers)
        {
            /// A nested correlated subquery may reference a column from a scope beyond its immediate
            /// outer query (it skips an intermediate scope). Such a column is not present in the outer
            /// query plan yet at this point, because decorrelation runs inside-out while the correlated
            /// inputs of the intermediate scope are injected later. Reject this shape with a clear error
            /// instead of failing deep inside with NOT_FOUND_COLUMN_IN_BLOCK.
            if (!input_header->has(column))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Correlated subquery is not supported yet, because it references column '{}' from a "
                    "scope beyond the immediate outer query. Current outer query header: {}",
                    column,
                    input_header->dumpNames());
            buffer_header->insert(input_header->getByName(column));
        }

        rhs_plan.addStep(std::make_unique<CommonSubplanReferenceStep>(
            buffer_header,
            context.query_plan.getRootNode(),
            context.correlated_subquery.correlated_column_identifiers));
        rhs_plan.getRootNode()->step->setStepDescription("Input for " + context.correlated_subquery.action_node_name, 100);

        /// Needed to simulate the Duplicate Eliminating Join. Runs with internal unbounded limits so
        /// that a user's max_rows_in_distinct / distinct_overflow_mode can never truncate the domain.
        {
            SizeLimits distinct_limits(/*max_rows_=*/0, /*max_bytes_=*/0, OverflowMode::THROW);
            rhs_plan.addStep(std::make_unique<DistinctStep>(
                rhs_plan.getCurrentHeader(),
                distinct_limits,
                /*limit_hint_=*/0,
                context.correlated_subquery.correlated_column_identifiers,
                /*pre_distinct_=*/false));
        }

        if (default_join_kind == DecorrelationJoinKind::LEFT)
            std::swap(lhs_plan, rhs_plan);

        auto lhs_plan_header = lhs_plan.getCurrentHeader();
        auto rhs_plan_header = rhs_plan.getCurrentHeader();

        JoinExpressionActions join_expression_actions(
            lhs_plan_header->getColumnsWithTypeAndName(),
            rhs_plan_header->getColumnsWithTypeAndName());

        NameSet output_columns;
        output_columns.insert_range(lhs_plan_header->getNames());
        output_columns.insert_range(rhs_plan_header->getNames());

        auto decorrelated_join = std::make_unique<JoinStepLogical>(
            /*left_header_=*/lhs_plan_header,
            /*right_header_=*/rhs_plan_header,
            JoinOperator(JoinKind::Cross),
            std::move(join_expression_actions),
            output_columns,
            std::unordered_map<String, const ActionsDAG::Node *>{},
            settings[Setting::join_use_nulls],
            JoinSettings(settings, context.planner_context->getQueryContext()->getJoinAnalyzeMode()),
            SortingStep::Settings(settings));
        decorrelated_join->setStepDescription("JOIN to evaluate correlated expression");
        makeInternalDecorrelationJoinUnbounded(*decorrelated_join);

        /// Add CROSS JOIN to combine data streams from left and right plans.
        QueryPlan result_plan;

        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(lhs_plan)));
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(rhs_plan)));

        result_plan.unitePlans(std::move(decorrelated_join), {std::move(plans)});

        /// Drop the row marker now that it has carried the row count across the join. It must not
        /// leave this branch: the ExpressionStep/FilterStep decorrelation handlers restore unused
        /// inputs, which would otherwise propagate the marker up to a UnionStep and make correlated
        /// UNION arms have mismatched widths. After the join the domain columns carry the row count,
        /// so removing the marker leaves at least one column and is safe.
        if (row_marker_name)
        {
            ActionsDAG drop_marker_dag(result_plan.getCurrentHeader()->getNamesAndTypesList());
            ActionsDAG::NodeRawConstPtrs kept_outputs;
            for (const auto * input : drop_marker_dag.getInputs())
                if (input->result_name != *row_marker_name)
                    kept_outputs.push_back(input);
            drop_marker_dag.getOutputs() = std::move(kept_outputs);

            auto drop_marker_step = std::make_unique<ExpressionStep>(result_plan.getCurrentHeader(), std::move(drop_marker_dag));
            drop_marker_step->setStepDescription("Drop row marker for zero-column correlated subquery body");
            result_plan.addStep(std::move(drop_marker_step));
        }

        return result_plan;
    }

    if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
    {
        /// Any correlated placeholder use in a projection expression is a use.
        collectIdentifiersUsedOutsideEqualities(
            expression_step->getExpression(), {}, context.scope_stack.back().identifiers_used_outside_equalities);

        /// Record pure renames as equivalences: an alias output is the same value as its underlying
        /// input (identical type), and equalities above a renaming step reference the renamed column
        /// (e.g. a derived table's identifier over a union output). The union-arm seeding below relies
        /// on these edges to translate such equalities to the union output names.
        for (const auto * output : expression_step->getExpression().getOutputs())
        {
            const auto * source = output;
            while (source->type == ActionsDAG::ActionType::ALIAS)
                source = source->children.front();
            if (source->type != ActionsDAG::ActionType::INPUT || source->result_name == output->result_name)
                continue;
            context.scope_stack.back().equivalence_classes.add(
                ColumnWithType{output->result_name, output->result_type},
                ColumnWithType{source->result_name, source->result_type});
        }

        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());

        auto input_header = decorrelated_query_plan.getCurrentHeader();

        expression_step->decorrelateActions();
        expression_step->getExpression().appendInputsForUnusedColumns(*input_header);
        for (const auto & column : input_header->getColumnsWithTypeAndName())
            expression_step->getExpression().tryRestoreColumn(column.name);

        expression_step->updateInputHeader(input_header);

        decorrelated_query_plan.addStep(std::move(node->step));
        return decorrelated_query_plan;
    }
    if (auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
    {
        auto & dag = filter_step->getExpression();
        auto * predicate = const_cast<ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
        auto conjuncts_list = getConjunctsList(predicate);
        std::unordered_set<const ActionsDAG::Node *> recorded_equality_nodes;
        for (const auto * conjunct : conjuncts_list)
        {
            bool is_equality = conjunct->type == ActionsDAG::ActionType::FUNCTION && conjunct->function_base->getName() == "equals";
            if (is_equality)
            {
                const auto & arguments = conjunct->children;
                if (arguments.size() != 2)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Correlated subquery equality predicate must have exactly two arguments, but has {}",
                        arguments.size());

                const auto & lhs_type = arguments[0]->result_type;
                const auto & rhs_type = arguments[1]->result_type;

                /// Substitution can compensate for a nullability difference (the equality rejects NULLs),
                /// and for native-number pairs with a least supertype: for them `equals` is mathematically
                /// exact (so equivalence is transitive across a class) and accurateCastOrNull is
                /// exactness-checking. `LowCardinality` and `Nullable` are value-transparent wrappers,
                /// so only the base types decide. Anything else (Decimal vs Float, String vs
                /// FixedString, ...) has comparison semantics that are not consistent with CAST, so it
                /// is not recorded. Substitution is additionally restricted in `classifySubstitutionConversion`:
                /// float correlated bases are never substituted (signed zero), and `Bool` correlated
                /// bases only from `Bool` members (the custom name is load-bearing). Substitution also
                /// requires the identifier to have no uses outside the recorded equalities
                /// (see `identifiers_used_outside_equalities`).
                if (!isRecordableEquivalencePair(lhs_type, rhs_type))
                    continue;

                context.scope_stack.back().equivalence_classes.add(
                    ColumnWithType{arguments[0]->result_name, lhs_type},
                    ColumnWithType{arguments[1]->result_name, rhs_type});
                recorded_equality_nodes.insert(conjunct);
            }
        }
        /// An `equals` conjunct that failed the recording type guard is not an allowed consumer:
        /// its identifiers stay unsubstituted (conservative — substitution could change that
        /// equality's evaluation).
        collectIdentifiersUsedOutsideEqualities(
            dag, recorded_equality_nodes, context.scope_stack.back().identifiers_used_outside_equalities);

        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());
        auto input_header = decorrelated_query_plan.getCurrentHeader();

        filter_step->decorrelateActions();
        filter_step->getExpression().appendInputsForUnusedColumns(*input_header);
        for (const auto & column : input_header->getColumnsWithTypeAndName())
            filter_step->getExpression().tryRestoreColumn(column.name);

        node->step->updateInputHeader(input_header);

        decorrelated_query_plan.addStep(std::move(node->step));
        return decorrelated_query_plan;
    }
    if (auto * union_step = typeid_cast<UnionStep *>(node->step.get()))
    {
        /// Subplans must be decorrelated separately, because every subquery in the UNION step
        /// can have its own equivalence classes. The equivalence classes in one subquery
        /// should not be visible by another subquery. Example:
        ///
        /// SELECT *
        /// FROM t
        /// WHERE EXISTS (
        ///     SELECT *
        ///     FROM t1
        ///     WHERE t.x = t1.x
        ///     UNION ALL
        ///     SELECT *
        ///     FROM t2
        ///     WHERE t.x = t2.y
        /// )
        const auto & settings = context.planner_context->getQueryContext()->getSettingsRef();
        auto process_isolated_subplan = [](
            DecorrelationContext & current_context,
            QueryPlan::Node * subplan_root,
            const SharedHeader & union_output_header
        ) -> QueryPlan
        {
            /// Fresh equivalence classes: an equality inside one arm must not enable substitution in a
            /// sibling. The usage set is inherited: a use recorded by a step shared above the union
            /// receives the arm's substituted column through the union output, so it must disable
            /// substitution in every arm. The copy keeps uses discovered inside one arm local to it.
            DecorrelationScope child_scope;
            child_scope.identifiers_used_outside_equalities = current_context.scope_stack.back().identifiers_used_outside_equalities;

            /// A parent-scope equality on a union output column (e.g. `u.x = o.x` above the union)
            /// constrains each arm's rows exactly like an arm-local equality would: for the arm's rows the
            /// union output IS the arm column, and the retained parent conjunct keeps filtering after
            /// decorrelation. Seed the arm scope with the parent members translated to arm-local names so
            /// the arm can substitute; the type guard is re-checked against the traced boundary type
            /// (union type coercion may differ per arm).
            for (const auto & correlated_column_identifier : current_context.correlated_subquery.correlated_column_identifiers)
            {
                auto parent_class = current_context.scope_stack.back().equivalence_classes.getClass(
                    ColumnWithType{correlated_column_identifier, nullptr});
                if (!parent_class)
                    continue;

                DataTypePtr identifier_type;
                for (const auto & member : *parent_class)
                {
                    if (member.name == correlated_column_identifier)
                    {
                        identifier_type = member.type;
                        break;
                    }
                }
                chassert(identifier_type != nullptr);
                if (!identifier_type)
                    continue;

                for (const auto & member : *parent_class)
                {
                    if (member.name == correlated_column_identifier)
                        continue;
                    /// The union aligns arms positionally; arm headers may use different column identifiers.
                    if (!union_output_header->has(member.name))
                        continue;
                    size_t position = union_output_header->getPositionByName(member.name);
                    const auto & arm_header = subplan_root->step->getOutputHeader();
                    if (position >= arm_header->columns())
                        continue;
                    const auto & arm_column = arm_header->getByPosition(position);
                    auto traced = traceUnionColumnIntoArm(subplan_root, arm_column.name, current_context.correlated_plan_steps);
                    if (!traced)
                        continue;
                    if (!isRecordableEquivalencePair(traced->type, identifier_type))
                        continue;
                    child_scope.equivalence_classes.add(*traced, ColumnWithType{correlated_column_identifier, identifier_type});
                }
            }

            current_context.scope_stack.push_back(std::move(child_scope));
            auto decorrelated_isolated_plan = decorrelateQueryPlan(current_context, subplan_root);
            current_context.scope_stack.pop_back();
            return decorrelated_isolated_plan;
        };

        /// A UnionStep can have any number of inputs; every arm must be decorrelated.
        SharedHeaders query_plans_headers;
        std::vector<QueryPlanPtr> child_plans;
        query_plans_headers.reserve(node->children.size());
        child_plans.reserve(node->children.size());
        for (auto * child : node->children)
        {
            auto decorrelated_child_plan = process_isolated_subplan(context, child, node->step->getOutputHeader());
            query_plans_headers.push_back(decorrelated_child_plan.getCurrentHeader());
            child_plans.emplace_back(std::make_unique<QueryPlan>(std::move(decorrelated_child_plan)));
        }

        Block union_common_header = buildCommonHeaderForUnion(
            query_plans_headers,
            SelectUnionMode::UNION_ALL,
            settings[Setting::use_variant_as_common_type]); // Union mode doesn't matter here
        addConvertingToCommonHeaderActionsIfNeeded(child_plans, union_common_header, query_plans_headers, context.planner_context->getQueryContext());

        union_step->updateInputHeaders(std::move(query_plans_headers));

        QueryPlan result_plan;
        result_plan.unitePlans(std::move(node->step), std::move(child_plans));

        return result_plan;
    }
    if (auto * aggeregating_step = typeid_cast<AggregatingStep *>(node->step.get()))
    {
        /// At entry the parameters are the user's — the decorrelation appends its own keys only
        /// afterwards. A correlated identifier among the user's keys or aggregate arguments is a
        /// use outside the recorded equalities.
        {
            const auto & user_params = aggeregating_step->getAggregatorParameters();
            for (const auto & correlated_column_identifier : context.correlated_subquery.correlated_column_identifiers)
            {
                bool used = std::ranges::contains(user_params.keys, correlated_column_identifier);
                for (const auto & aggregate : user_params.aggregates)
                    used = used || std::ranges::contains(aggregate.argument_names, correlated_column_identifier);
                if (used)
                    context.scope_stack.back().identifiers_used_outside_equalities.insert(correlated_column_identifier);
            }
        }

        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());
        auto input_header = decorrelated_query_plan.getCurrentHeader();

        if (aggeregating_step->isGroupingSets())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Decorrelation of GROUP BY GROUPING SETS is not supported yet");

        const auto & original_aggregator_params = aggeregating_step->getAggregatorParameters();

        Names new_keys = original_aggregator_params.keys;
        for (const auto & correlated_column_identifier : context.correlated_subquery.correlated_column_identifiers)
        {
            new_keys.push_back(correlated_column_identifier);
        }

        auto new_aggregator_params = original_aggregator_params.cloneWithKeys(new_keys, original_aggregator_params.only_merge);

        auto result_step = std::make_unique<AggregatingStep>(
            std::move(input_header),
            std::move(new_aggregator_params),
            aggeregating_step->getGroupingSetsParamsList(),
            aggeregating_step->getFinal(),
            aggeregating_step->getMaxBlockSize(),
            aggeregating_step->getMaxBlockSizeForAggregationInOrder(),
            aggeregating_step->getMergeThreads(),
            aggeregating_step->getTemporaryDataMergeThreads(),
            false /*storage_has_evenly_distributed_read_*/,
            aggeregating_step->isGroupByUseNulls(),
            SortDescription{} /*sort_description_for_merging_*/,
            SortDescription{} /*group_by_sort_description_*/,
            aggeregating_step->shouldProduceResultsInBucketOrder(),
            aggeregating_step->usingMemoryBoundMerging(),
            aggeregating_step->explicitSortingRequired()
        );
        result_step->setStepDescription(*aggeregating_step);

        decorrelated_query_plan.addStep(std::move(result_step));

        return decorrelated_query_plan;
    }
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot decorrelate query, because '{}' step is not supported",
        node->step->getName());
}

void buildRenamingForScalarSubquery(
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery
)
{
    ActionsDAG dag(query_plan.getCurrentHeader()->getNamesAndTypesList());
    const auto * result_node = &dag.findInOutputs(correlated_subquery.action_node_name);

    ActionsDAG::NodeRawConstPtrs new_outputs{ result_node };
    new_outputs.reserve(correlated_subquery.correlated_column_identifiers.size() + 1);

    for (const auto & column_name : correlated_subquery.correlated_column_identifiers)
    {
        new_outputs.push_back(&dag.addAlias(dag.findInOutputs(column_name), fmt::format("{}.{}", correlated_subquery.action_node_name, column_name)));
    }

    dag.getOutputs() = std::move(new_outputs);

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(dag));
    expression_step->setStepDescription("Create renaming actions for scalar subquery");
    query_plan.addStep(std::move(expression_step));
}

void buildExistsResultExpression(
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery,
    bool project_only_correlated_columns
)
{
    ActionsDAG dag(query_plan.getCurrentHeader()->getNamesAndTypesList());
    auto result_type = std::make_shared<DataTypeUInt8>();
    auto column = result_type->createColumnConst(0, 1);
    const auto * exists_result = &dag.materializeNode(dag.addColumn(std::move(column), result_type, correlated_subquery.action_node_name));

    if (project_only_correlated_columns)
    {
        ActionsDAG::NodeRawConstPtrs new_outputs;
        new_outputs.reserve(correlated_subquery.correlated_column_identifiers.size() + 1);

        for (const auto & column_name : correlated_subquery.correlated_column_identifiers)
        {
            new_outputs.push_back(&dag.addAlias(dag.findInOutputs(column_name), fmt::format("{}.{}", correlated_subquery.action_node_name, column_name)));
        }
        new_outputs.push_back(exists_result);

        dag.getOutputs() = std::move(new_outputs);
    }
    else
    {
        dag.addOrReplaceInOutputs(*exists_result);
    }

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(dag));
    expression_step->setStepDescription("Create result for always true EXISTS expression");
    query_plan.addStep(std::move(expression_step));
}

QueryPlan buildLogicalJoin(
    const PlannerContextPtr & planner_context,
    QueryPlan input_stream_plan,
    QueryPlan decorrelated_plan,
    const CorrelatedSubquery & correlated_subquery,
    bool uses_in_memory_buffer
)
{
    auto lhs_plan_header = decorrelated_plan.getCurrentHeader();
    auto rhs_plan_header = input_stream_plan.getCurrentHeader();

    using ColumnNameGetter = std::function<String(const String &)>;
    ColumnNameGetter get_lhs_column_name = [&](const String & column_name) -> String {
        return fmt::format("{}.{}", correlated_subquery.action_node_name, column_name);
    };
    ColumnNameGetter get_rhs_column_name = [&](const String & column_name) -> String {
        return column_name;
    };

    auto lhs_plan = std::move(decorrelated_plan);
    auto rhs_plan = std::move(input_stream_plan);

    NameSet output_columns;
    output_columns.insert_range(rhs_plan_header->getNames());
    output_columns.insert(correlated_subquery.action_node_name);

    const auto & settings = planner_context->getQueryContext()->getSettingsRef();

    /// A buffered referenced input (SaveSubqueryResultToBuffer / ReadFromCommonBuffer) requires the
    /// reader to run after the writer finished, which only JoinKind::Right guarantees, so force it when
    /// a buffer is created (the join kind does not change the result). Whether a buffer is created is
    /// decided in decorrelateQueryPlan and passed in here, so the layout always matches the actual
    /// buffer decision (issue #108521).
    if (settings[Setting::correlated_subqueries_default_join_kind] == DecorrelationJoinKind::LEFT && !uses_in_memory_buffer)
    {
        std::swap(lhs_plan, rhs_plan);
        std::swap(lhs_plan_header, rhs_plan_header);
        std::swap(get_lhs_column_name, get_rhs_column_name);
    }

    JoinExpressionActions join_expression_actions(
        lhs_plan_header->getColumnsWithTypeAndName(),
        rhs_plan_header->getColumnsWithTypeAndName());

    std::vector<JoinActionRef> predicates;
    for (const auto & column_name : correlated_subquery.correlated_column_identifiers)
    {
        std::vector<JoinActionRef> eq_arguments;
        eq_arguments.push_back(join_expression_actions.findNode(get_lhs_column_name(column_name), /* is_input= */ true));
        eq_arguments.push_back(join_expression_actions.findNode(get_rhs_column_name(column_name), /* is_input= */ true));
        auto eq_node = JoinActionRef::transform(eq_arguments, JoinActionRef::AddFunction(JoinConditionOperator::Equals));
        predicates.push_back(std::move(eq_node));
    }

    auto join_kind_to_use = (uses_in_memory_buffer || settings[Setting::correlated_subqueries_default_join_kind] == DecorrelationJoinKind::RIGHT) ? JoinKind::Right : JoinKind::Left;

    /// Add ANY OUTER JOIN
    auto result_join = std::make_unique<JoinStepLogical>(
        lhs_plan_header,
        rhs_plan_header,
        JoinOperator(join_kind_to_use, JoinStrictness::Any, JoinLocality::Unspecified, std::move(predicates)),
        std::move(join_expression_actions),
        output_columns,
        std::unordered_map<String, const ActionsDAG::Node *>{},
        /*join_use_nulls=*/false,
        JoinSettings(settings, planner_context->getQueryContext()->getJoinAnalyzeMode()),
        SortingStep::Settings(settings));
    result_join->setStepDescription("JOIN to generate result stream");
    makeInternalDecorrelationJoinUnbounded(*result_join);

    /// Reordering protection for the buffered case whose layout was forced to JoinKind::Right above.
    if (uses_in_memory_buffer)
    {
        auto & join_algorithms = result_join->getJoinSettings().join_algorithms;
        /// Remove algorithms that are not compatible with in-memory buffering
        /// of correlated subquery input.
        /// We must be sure that the input stream is fully evaluated
        /// before the correlated subquery is executed.
        std::erase_if(join_algorithms, [](auto join_algorithm) { return join_algorithm != JoinAlgorithm::HASH && join_algorithm != JoinAlgorithm::PARALLEL_HASH; });
        /// This JOIN is an internal decorrelation detail, so the user-facing `join_algorithm` list must not
        /// decide whether it can run at all: with `auto` or a merge-only list nothing would survive the filter
        /// and `chooseJoinAlgorithm` would throw `NOT_IMPLEMENTED`. Force the compatible algorithms instead.
        if (join_algorithms.empty())
            join_algorithms = {JoinAlgorithm::HASH, JoinAlgorithm::PARALLEL_HASH};
        /// Forbid reordering of this JOIN step. Child subplans still can be reordered and optimized.
        result_join->setOptimized();
    }

    QueryPlan result_plan;

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(lhs_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(rhs_plan)));

    result_plan.unitePlans(std::move(result_join), {std::move(plans)});
    return result_plan;
}

Planner buildPlannerForCorrelatedSubquery(
    const PlannerContextPtr & planner_context,
    const CorrelatedSubquery & correlated_subquery,
    const SelectQueryOptions & select_query_options
)
{
    auto subquery_options = select_query_options.subquery();
    auto global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
    /// Register table expression data for correlated columns sources in the global context.
    /// Table expression data would be reused because it can't be initialized
    /// during plan construction for correlated subquery.
    global_planner_context->collectTableExpressionDataForCorrelatedColumns(correlated_subquery.query_tree, planner_context);

    Planner subquery_planner(
        correlated_subquery.query_tree,
        subquery_options,
        std::move(global_planner_context));
    subquery_planner.buildQueryPlanIfNeeded();

    return subquery_planner;
}

void addStepForResultRenaming(
    const CorrelatedSubquery & correlated_subquery,
    QueryPlan & correlated_subquery_plan,
    const PlannerContextPtr & planner_context
)
{
    const auto & header = correlated_subquery_plan.getCurrentHeader();
    const auto & subquery_result_columns = header->getColumnsWithTypeAndName();

    if (subquery_result_columns.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected to get only 1 result column of correlated subquery, but got {}",
            subquery_result_columns.size());

    const auto & result_column = subquery_result_columns[0];
    auto expected_result_type = correlated_subquery.query_tree->getResultType();
    /// Scalar correlated subquery must return nullable result. See method `QueryNode::getResultType()` for details.
    if (!expected_result_type->equals(*makeNullableOrLowCardinalityNullableSafe(result_column.type)))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected {} as correlated subquery result, but got {}",
            expected_result_type->getName(),
            result_column.type->getName());

    ActionsDAG dag(subquery_result_columns);

    const ActionsDAG::Node * result_node = nullptr;
    if (!expected_result_type->equals(*result_column.type))
    {
        result_node = &dag.addCast(
            *dag.getOutputs()[0],
            expected_result_type,
            correlated_subquery.action_node_name,
            planner_context->getQueryContext());
    }
    else
    {
        result_node = &dag.addAlias(*dag.getOutputs()[0], correlated_subquery.action_node_name);
    }

    dag.getOutputs() = { result_node };

    auto expression_step = std::make_unique<ExpressionStep>(header, std::move(dag));
    expression_step->setStepDescription("Create correlated subquery result alias");
    correlated_subquery_plan.addStep(std::move(expression_step));
}

}

/* Build query plan for correlated subquery using decorrelation algorithm
 * on top of relational algebra operators proposed by TU Munich researchers
 * Thomas Neumann and Alfons Kemper.
 *
 * Original research paper "Unnesting Arbitrary Queries": https://cs.emis.de/LNI/Proceedings/Proceedings241/383.pdf
 * See also a follow-up paper, "Improving Unnesting of Complex Queries": https://dl.gi.de/items/b9df4765-d1b0-4267-a77c-4ce4ab0ee62d
 *
 * NOTE: ClickHouse does not explicitly build SQL query into relational algebra expression.
 * Instead, it produces a query plan where almost every step has an analog from relational algebra.
 * This function implements a decorrelation algorithm using the ClickHouse query plan.
 *
 * TODO: Support decorrelation of all kinds of query plan steps.
 */
void buildQueryPlanForCorrelatedSubquery(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery,
    const SelectQueryOptions & select_query_options)
{
    auto * query_node = correlated_subquery.query_tree->as<QueryNode>();  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    auto * union_node = correlated_subquery.query_tree->as<UnionNode>();  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    chassert(query_node != nullptr && query_node->isCorrelated() || union_node != nullptr && union_node->isCorrelated());

    switch (correlated_subquery.kind)
    {
        case DB::CorrelatedSubqueryKind::SCALAR:
        {
            Planner subquery_planner = buildPlannerForCorrelatedSubquery(planner_context, correlated_subquery, select_query_options);
            /// Logical plan for correlated subquery
            auto & correlated_query_plan = subquery_planner.getQueryPlan();

            addStepForResultRenaming(correlated_subquery, correlated_query_plan, planner_context);

            /// Mark all query plan steps if they or their subplans contain usage of correlated subqueries.
            /// It's needed to identify the moment when dependent join can be replaced by CROSS JOIN.
            auto correlated_step_map = buildCorrelatedPlanStepMap(correlated_query_plan);

            auto correlated_plan = std::move(subquery_planner).extractQueryPlan();
            /// Propagate interpreter contexts (e.g. for table functions like `url()`) to the parent plan,
            /// so they stay alive after decorrelation destroys the correlated plan.
            for (const auto & ctx : correlated_plan.getInterpretersContexts())
                query_plan.addInterpreterContext(ctx);

            DecorrelationContext context{
                .correlated_subquery = correlated_subquery,
                .planner_context = planner_context,
                .query_plan = std::move(query_plan),
                .correlated_query_plan = std::move(correlated_plan),
                .correlated_plan_steps = std::move(correlated_step_map),
                .scope_stack = { DecorrelationScope{} }
            };

            auto decorrelated_plan = decorrelateQueryPlan(context, context.correlated_query_plan.getRootNode());
            buildRenamingForScalarSubquery(decorrelated_plan, correlated_subquery);

            /// Use LEFT OUTER JOIN to produce the result plan.
            query_plan = buildLogicalJoin(
                planner_context,
                std::move(context.query_plan),
                std::move(decorrelated_plan),
                correlated_subquery,
                context.uses_in_memory_buffer);
            break;
        }
        case CorrelatedSubqueryKind::EXISTS:
        {
            Planner subquery_planner = buildPlannerForCorrelatedSubquery(planner_context, correlated_subquery, select_query_options);
            /// Logical plan for correlated subquery
            auto & correlated_query_plan = subquery_planner.getQueryPlan();

            /// For EXISTS expression we can remove plan steps that doesn't change the number of result rows.
            /// It may also result in non-correlated subquery plan
            /// Example:
            /// SELECT * FROM numbers(1) WHERE EXISTS (SELECT a = number FROM table)
            if (optimizePlanForExists(correlated_query_plan))
            {
                /// Subquery always produces at least 1 row.
                buildExistsResultExpression(query_plan, correlated_subquery, /*project_only_correlated_columns=*/false);
                return;
            }

            /// Mark all query plan steps if they or their subplans contain usage of correlated subqueries.
            /// It's needed to identify the moment when dependent join can be replaced by CROSS JOIN.
            auto correlated_step_map = buildCorrelatedPlanStepMap(correlated_query_plan);

            auto correlated_plan = std::move(subquery_planner).extractQueryPlan();
            /// Propagate interpreter contexts (e.g. for table functions like `url()`) to the parent plan,
            /// so they stay alive after decorrelation destroys the correlated plan.
            for (const auto & ctx : correlated_plan.getInterpretersContexts())
                query_plan.addInterpreterContext(ctx);

            DecorrelationContext context{
                .correlated_subquery = correlated_subquery,
                .planner_context = planner_context,
                .query_plan = std::move(query_plan),
                .correlated_query_plan = std::move(correlated_plan),
                .correlated_plan_steps = std::move(correlated_step_map),
                .scope_stack = { DecorrelationScope{} }
            };

            auto decorrelated_plan = decorrelateQueryPlan(context, context.correlated_query_plan.getRootNode());
            /// Add a 'exists(<table expression id>)' expression that is always true.
            buildExistsResultExpression(decorrelated_plan, correlated_subquery, /*project_only_correlated_columns=*/true);

            /// Use LEFT OUTER JOIN to produce the result plan.
            /// If there's no corresponding rows from the right side, 'exists(<table expression id>)' would be replaced by default value (false).
            query_plan = buildLogicalJoin(
                planner_context,
                std::move(context.query_plan),
                std::move(decorrelated_plan),
                correlated_subquery,
                context.uses_in_memory_buffer);
            break;
        }
    }
}

}
