#include <Columns/ColumnConst.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/QueueWithMemoryTracking.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Common/assert_cast.h>

#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinExpressionActions.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/TypeTree.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/indexHint.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Transforms/ExpressionTransform.h>

#include <Storages/StorageLog.h>
#include <Storages/StorageMerge.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsDAGTraversal.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <fmt/format.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::Setting
{
    extern const SettingsBool parallel_replicas_filter_pushdown;
}

namespace DB::QueryPlanOptimizations
{

/// Assert that `node->children` has at least `child_num` elements
static void checkChildrenSize(QueryPlan::Node * node, size_t child_num)
{
    auto & child = node->step;
    if (child_num > child->getInputHeaders().size() || child_num > node->children.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of children: expected at least {}, got {} children and {} streams",
                        child_num, child->getInputHeaders().size(), node->children.size());
}

static bool identifiersIsAmongAllGroupingSets(const GroupingSetsParamsList & grouping_sets_params, const NameSet & identifiers_in_predicate)
{
    for (const auto & grouping_set : grouping_sets_params)
    {
        for (const auto & identifier : identifiers_in_predicate)
        {
            if (std::find(grouping_set.used_keys.begin(), grouping_set.used_keys.end(), identifier) == grouping_set.used_keys.end())
                return false;
        }
    }
    return true;
}

/// An implied hint reads its arguments from its own DAG, not from children. A user hint never passes a key step.
static void addImpliedIndexHintInputs(const ActionsDAG::Node & node, NameSet & res)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION)
        return;
    const auto * index_hint = FunctionIndexHint::tryGet(node.function_base);
    if (!index_hint || !index_hint->isImplied())
        return;
    for (const auto * input : index_hint->getActions().getInputs())
        res.emplace(input->result_name);
    for (const auto & inner : index_hint->getActions().getNodes())
        addImpliedIndexHintInputs(inner, res);
}

static NameSet findIdentifiersOfNode(const ActionsDAG::Node * node, NameSet & implied_index_hint_inputs)
{
    NameSet res;
    visitReachableNodes({node}, [&](const ActionsDAG::Node * cur)
    {
        /// We treat all INPUT as identifier
        if (cur->type == ActionsDAG::ActionType::INPUT)
            res.emplace(cur->result_name);
        else
            addImpliedIndexHintInputs(*cur, implied_index_hint_inputs);
        return VisitResult::Continue;
    });
    return res;
}

namespace
{
/// Returns true if the filter column is present in the output and it is a constant.
/// Returns false if the the filter column removed from the output or it is not a constant.
bool isFilterColumnConst(const FilterStep & filter)
{
    if (filter.removesFilterColumn())
        return false;

    return filter.getOutputHeader()->getByName(filter.getFilterColumnName()).column->isConst();
}

void materializeFilterColumnIfNeededAfterPushDown(FilterStep & filter, const bool was_filter_const_before, const bool is_filter_const_after)
{
    if (was_filter_const_before || !is_filter_const_after)
        return;

    auto & expression = filter.getExpression();
    const auto & filter_node = expression.findInOutputs(filter.getFilterColumnName());
    const auto & non_const_filter_node = expression.materializeNode(filter_node, false);
    expression.addOrReplaceInOutputs(non_const_filter_node);
}

/// When the filter column was const before push-down (e.g., `and(const_false, ..., aggregate)` short-circuits to 0)
/// but the remaining expression after extracting the constant parts is non-const, we must constify it back.
/// Otherwise, parent steps that cached the original (const) output header will see a non-const column, causing
/// a "Block structure mismatch" exception or "non constant in source stream but must be constant in result" error.
bool constifyFilterColumnAfterPushDown(ActionsDAG & expression, const String & filter_column_name, const ColumnPtr & original_const_column)
{
    auto * filter_node = const_cast<ActionsDAG::Node *>(expression.tryFindInOutputs(filter_column_name));
    if (!filter_node || filter_node->type == ActionsDAG::ActionType::INPUT)
        return false;

    ActionsDAG::Node const_node;
    const_node.type = ActionsDAG::ActionType::COLUMN;
    const_node.result_name = filter_node->result_name;
    const_node.result_type = filter_node->result_type;
    const_node.column = filter_node->result_type->createColumnConst(0, (*original_const_column)[0]);

    *filter_node = std::move(const_node);
    return true;
}
}

static std::optional<ActionsDAG::ActionsForFilterPushDown> splitFilter(
    QueryPlan::Node * parent_node,
    bool step_changes_the_number_of_rows,
    const Names & available_inputs,
    ActionsDAG::PushDownIndexHints index_hints,
    size_t child_idx = 0)
{
    QueryPlan::Node * child_node = parent_node->children.front();
    checkChildrenSize(child_node, child_idx + 1);

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = assert_cast<FilterStep *>(parent.get());
    auto & expression = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    const bool removes_filter = filter->removesFilterColumn();

    const auto & all_inputs = child->getInputHeaders()[child_idx]->getColumnsWithTypeAndName();
    const bool allow_deterministic_functions = !step_changes_the_number_of_rows;
    const bool is_filter_column_const_before = isFilterColumnConst(*filter);

    /// Capture the original constant column value before push-down modifies the expression DAG.
    ColumnPtr original_filter_const_column;
    if (is_filter_column_const_before)
        original_filter_const_column = filter->getOutputHeader()->getByName(filter_column_name).column;

    auto result = expression.splitActionsForFilterPushDown(
        filter_column_name, removes_filter, available_inputs, all_inputs, allow_deterministic_functions, index_hints);
    if (result)
    {
        if (is_filter_column_const_before && !result->is_filter_const_after_push_down)
        {
            /// The filter column was const before push-down (e.g., AND short-circuits to constant false)
            /// but became non-const after extracting the constant parts. Restore constness.
            result->is_filter_const_after_push_down
                = constifyFilterColumnAfterPushDown(expression, filter_column_name, original_filter_const_column);
        }
        else
        {
            bool is_filter_const_after = result->is_filter_const_after_push_down;

            /// After push-down, the remaining expression may produce a Const filter column
            /// even though `is_filter_const_after_push_down` is false (that flag is only set
            /// when ALL conjunctions are pushed down). This happens when the remaining expression
            /// contains a NULL constant argument — `defaultImplementationForNulls` short-circuits
            /// to a ColumnConst, e.g. `plus(count(), NULL)` becomes Const(NULL).
            /// If uncorrected, the Const output header propagates to parent steps (e.g. UnionStep)
            /// causing a "Block structure mismatch" exception.
            if (!is_filter_column_const_before && !is_filter_const_after && !removes_filter)
            {
                auto test_header = expression.updateHeader(*filter->getInputHeaders().front());
                const auto * filter_col = test_header.findByName(filter_column_name);
                if (filter_col && filter_col->column && isColumnConst(*filter_col->column))
                    is_filter_const_after = true;
            }

            materializeFilterColumnIfNeededAfterPushDown(*filter, is_filter_column_const_before, is_filter_const_after);
        }
    }
    return result;
}

static size_t addNewFilterStepOrThrow(
    QueryPlan::Node * parent_node,
    QueryPlan::Nodes & nodes,
    ActionsDAG::ActionsForFilterPushDown split_filter,
    size_t child_idx = 0, bool update_parent_filter = true)
{
    QueryPlan::Node * child_node = parent_node->children.front();
    checkChildrenSize(child_node, child_idx + 1);

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = assert_cast<FilterStep *>(parent.get());
    auto & expression = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();

    const auto * filter_node = expression.tryFindInOutputs(filter_column_name);
    if (update_parent_filter && !filter_node && !filter->removesFilterColumn())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Filter column {} was removed from ActionsDAG but it is needed in result. DAG:\n{}",
                        filter_column_name, expression.dumpDAG());

    /// Add new Filter step before Child.
    /// Expression/Filter -> Child -> Something
    auto & node = nodes.emplace_back();
    node.children.emplace_back(&node);

    std::swap(node.children[0], child_node->children[child_idx]);
    /// Expression/Filter -> Child -> Filter -> Something

    /// New filter column is the first one.
    String split_filter_column_name = split_filter.dag.getOutputs()[split_filter.filter_pos]->result_name;
    auto new_filter = std::make_unique<FilterStep>(
        node.children.at(0)->step->getOutputHeader(), std::move(split_filter.dag), std::move(split_filter_column_name), split_filter.remove_filter);
    new_filter->setOptimizeConstantColumnsAfterFilter(filter->getOptimizeConstantColumnsAfterFilter());
    node.step = std::move(new_filter);
    node.step->setStepDescription(*filter);

    child->updateInputHeader(node.step->getOutputHeader(), child_idx);

    if (update_parent_filter)
    {
        if (!filter_node || split_filter.is_filter_const_after_push_down)
        {
            /// This means that all predicates of filter were pushed down.
            /// Replace current actions to expression, as we don't need to filter anything.
            auto new_step = std::make_unique<ExpressionStep>(child->getOutputHeader(), std::move(expression));
            new_step->setStepDescription(*filter);
            parent = std::move(new_step);
        }
        else
        {
            filter->updateInputHeader(child->getOutputHeader());
        }
    }

    return 3;
}

static size_t tryAddNewFilterStep(
    QueryPlan::Node * parent_node,
    bool step_changes_the_number_of_rows,
    QueryPlan::Nodes & nodes,
    const Names & allowed_inputs,
    ActionsDAG::PushDownIndexHints index_hints = ActionsDAG::PushDownIndexHints::All,
    size_t child_idx = 0)
{
    if (auto split_filter = splitFilter(parent_node, step_changes_the_number_of_rows, allowed_inputs, index_hints, child_idx))
        return addNewFilterStepOrThrow(parent_node, nodes, std::move(*split_filter), child_idx);
    return 0;
}


/// Push down filter through specified type of step
template <typename Step>
static size_t simplePushDownOverStep(QueryPlan::Node * parent_node, bool step_changes_the_number_of_rows, QueryPlan::Nodes & nodes, QueryPlanStepPtr & child)
{
    if (typeid_cast<Step *>(child.get()))
    {
        Names allowed_inputs = child->getOutputHeader()->getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, step_changes_the_number_of_rows, nodes, allowed_inputs))
            return updated_steps;
    }
    return 0;
}

/// Disjoint Set Union (Union-Find) data structure for JoinActionRef.
/// Used to find equivalent expressions in JOIN conditions.
/// For example, if (a = b) and (b = c), then a, b, c belong to the same equivalence class.
class EquivalentJoinKeySet
{
public:
    JoinActionRef findOrAdd(JoinActionRef ref)
    {
        auto it = parent.find(ref);
        if (it == parent.end())
        {
            parent.emplace(ref, ref);
            return ref;
        }

        if (it->second == ref)
            return ref;

        JoinActionRef root = findOrAdd(it->second);
        parent.insert_or_assign(ref, root);
        return root;
    }

    JoinActionRef unite(JoinActionRef a, JoinActionRef b)
    {
        JoinActionRef root_a = findOrAdd(a);
        JoinActionRef root_b = findOrAdd(b);

        if (root_a == root_b)
            return root_a;

        size_t rank_a = rank[root_a];
        size_t rank_b = rank[root_b];

        if (rank_a < rank_b)
            std::swap(root_a, root_b);

        parent.insert_or_assign(root_b, root_a);

        if (rank_a == rank_b)
            ++rank[root_a];

        return root_a;
    }

    bool connected(JoinActionRef a, JoinActionRef b)
    {
        return findOrAdd(a) == findOrAdd(b);
    }

    UnorderedMapWithMemoryTracking<JoinActionRef, VectorWithMemoryTracking<JoinActionRef>> getClasses()
    {
        UnorderedMapWithMemoryTracking<JoinActionRef, VectorWithMemoryTracking<JoinActionRef>> classes;
        for (auto & [ref, _] : parent)
            classes[findOrAdd(ref)].push_back(ref);
        return classes;
    }

    VectorWithMemoryTracking<JoinActionRef> getClass(JoinActionRef ref)
    {
        VectorWithMemoryTracking<JoinActionRef> res;
        JoinActionRef root = findOrAdd(ref);
        for (auto & [other_ref, _] : parent)
        {
            if (findOrAdd(other_ref) == root)
                res.push_back(other_ref);
        }
        return res;
    }

private:
    UnorderedMapWithMemoryTracking<JoinActionRef, JoinActionRef> parent;
    UnorderedMapWithMemoryTracking<JoinActionRef, size_t> rank;
};

using JoinActionRefPair = std::pair<JoinActionRef, JoinActionRef>;

struct JoinActionRefPairHash
{
    size_t operator()(const JoinActionRefPair & p) const noexcept
    {
        return std::hash<JoinActionRef>{}(p.first) ^ std::hash<JoinActionRef>{}(p.second);
    }
};

/// `IDataType::equals` ignores the time zone of `DateTime` and custom names such as `Bool`, but functions
/// over the key do not, so one key can stand in for another only if the full type names match.
static bool areTypesIdentical(const IDataType & lhs, const IDataType & rhs)
{
    return lhs.getName() == rhs.getName();
}

/// Invokes `callback(lhs, rhs)` per Equals / NullSafeEquals predicate, `lhs` normalised to the left side.
template <typename Callback>
static void forEachEquiJoinKey(const JoinOperator & join_operator, Callback && callback)
{
    for (const auto & predicate : join_operator.expression)
    {
        auto [predicate_op, lhs, rhs] = predicate.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals && predicate_op != JoinConditionOperator::NullSafeEquals)
            continue;
        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        else if (!lhs.fromLeft() || !rhs.fromRight())
            continue;
        callback(lhs, rhs);
    }
}

static VectorWithMemoryTracking<JoinActionRefPair> getJoiningKeysForJoinStep(const JoinOperator & join_operator)
{
    VectorWithMemoryTracking<JoinActionRefPair> joining_keys;
    forEachEquiJoinKey(join_operator, [&](const JoinActionRef & lhs, const JoinActionRef & rhs)
    {
        if (!areTypesIdentical(*lhs.getColumn().type, *rhs.getColumn().type))
            return;
        joining_keys.emplace_back(lhs, rhs);
    });
    return joining_keys;
}

static VectorWithMemoryTracking<JoinActionRefPair> buildEquialentSetsForJoinStepLogical(
    EquivalentJoinKeySet & equivalent_sets,
    const JoinStepLogical * join_step,
    const VectorWithMemoryTracking<QueryPlan::Node *> & child_nodes,
    int lookup_depth = 0)
{
    auto join_inputs = join_step->getInputActions();
    auto join_input_it = join_inputs.begin();

    for (const auto * child_node : child_nodes)
    {
        /// Sanity check to avoid expensive computations on too deep plans
        if (lookup_depth >= 32)
            break;

        const auto * child_join = typeid_cast<const JoinStepLogical *>(child_node->step.get());
        if (!child_join)
            continue;
        auto join_strictness = child_join->getJoinOperator().strictness;
        auto join_kind = child_join->getJoinOperator().kind;
        if (join_kind != JoinKind::Inner || (join_strictness != JoinStrictness::All && join_strictness != JoinStrictness::Any))
            continue;

        for (const auto & output : child_join->getOutputActions())
        {
            while (join_input_it != join_inputs.end() && output.getColumnName() != join_input_it->getColumnName())
                ++join_input_it;
            if (join_input_it == join_inputs.end())
                break;
            equivalent_sets.unite(*join_input_it, output);
        }
        buildEquialentSetsForJoinStepLogical(equivalent_sets, child_join, child_node->children, lookup_depth + 1);
    }

    auto joining_keys = getJoiningKeysForJoinStep(join_step->getJoinOperator());
    for (const auto & [lhs, rhs] : joining_keys)
        equivalent_sets.unite(lhs, rhs);
    return joining_keys;
}

/// The pushed-down filter computes a key and the JOIN computes it again, so the key must return the same
/// value twice within one query, must not change the number of rows, and one more evaluation must not be observable.
static bool canComputeJoinKeyTwice(const JoinActionRef & key)
{
    static constexpr auto changes_between_evaluations = [](const IFunctionBase & function)
    { return function.isStateful() || !function.isDeterministicInScopeOfQuery() || function.hasObservableSideEffects(); };

    const auto key_dag = JoinExpressionActions::getSubDAG(key);
    for (const auto & node : key_dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION)
        {
            if (changes_between_evaluations(*node.function_base))
                return false;
        }
        else if (node.type != ActionsDAG::ActionType::INPUT
            && node.type != ActionsDAG::ActionType::COLUMN
            && node.type != ActionsDAG::ActionType::ALIAS)
            return false;

        if (ActionsDAG::hasUnsafeHiddenLambdaBody(node, changes_between_evaluations))
            return false;
    }
    return true;
}

/// A pair of JOIN keys that are equal in every joined row. Keys of different types are equal after the
/// conversion to `common_type`, so the pair is then the two converted keys.
struct JoinKeyPair
{
    JoinActionRef left;
    JoinActionRef right;
    /// Not set when the two keys have the same type.
    DataTypePtr common_type;
};

static VectorWithMemoryTracking<JoinKeyPair> getJoinKeyPairs(
    const JoinStepLogical & join_step, const VectorWithMemoryTracking<JoinActionRefPair> & keys_of_identical_types)
{
    VectorWithMemoryTracking<JoinKeyPair> pairs;
    for (const auto & [lhs, rhs] : keys_of_identical_types)
        if (canComputeJoinKeyTwice(lhs) && canComputeJoinKeyTwice(rhs))
            pairs.push_back({lhs, rhs, nullptr});

    forEachEquiJoinKey(join_step.getJoinOperator(), [&](const JoinActionRef & lhs, const JoinActionRef & rhs)
    {
        /// Types that are equal but not identical, such as `DateTime` in two time zones, give no pair: functions tell them apart.
        if (lhs.getType()->equals(*rhs.getType()))
            return;

        /// The converted keys must hold the values the JOIN matched. For `FixedString` x and `String` y
        /// they do not: `CAST(x, 'String')` is `'a'` while the matched y may be `'a\0\0'`.
        if (!JoinCommon::isExactJoinKeyEquality(lhs.getType(), rhs.getType()))
            return;

        auto common_type = tryGetLeastSupertype(DataTypes{lhs.getType(), rhs.getType()});
        if (!common_type)
            return;

        /// Float keys can be equal for the JOIN and differ in bits (`-0.0` and `+0.0`), and a `Dynamic` or
        /// `JSON` type describes neither the contents nor the representation. A predicate can read both.
        if (anyInTypeTree(*common_type, [](const IDataType & type) { return isFloat(type) || isDynamic(type) || isObject(type); }))
            return;

        if (canComputeJoinKeyTwice(lhs) && canComputeJoinKeyTwice(rhs))
            pairs.push_back({lhs, rhs, common_type});
    });

    return pairs;
}

/// One side of a JOIN, as the push down of a filter sees it.
struct JoinSideForFilter
{
    const Block & header;
    /// Columns of this side that a condition pushed to this side can read. None when the JOIN pads this side.
    Names available_columns;
    /// The keys of this side stand for the keys of the other side.
    bool keys_admitted = false;
    /// The JOIN only matches against this side, so it gets no copy of a runtime filter.
    bool only_matched = false;
    bool accepts_filter = true;
};

struct FiltersForJoinSides
{
    std::optional<ActionsDAG> left;
    std::optional<ActionsDAG> right;
    bool is_filter_const_after_push_down = false;
};

/// Splits the filter above a JOIN: a conjunct goes to a side when it reads only the columns of that side,
/// and to both sides when it reads only JOIN keys, with the keys of the other side replaced.
/// The filter reads the JOIN output and the keys read the JOIN input, so all the work is done on a copy of the
/// filter with the JOIN actions under it, which reads the columns of the two streams.
static FiltersForJoinSides splitFilterForJoinSides(
    ActionsDAG & filter_dag,
    const String & filter_column_name,
    bool removes_filter,
    const JoinStepLogical * logical_join,
    const VectorWithMemoryTracking<JoinKeyPair> & key_pairs,
    const JoinSideForFilter & left,
    const JoinSideForFilter & right,
    bool filter_is_always_false)
{
    FiltersForJoinSides result;

    const auto * predicate = filter_dag.tryFindInOutputs(filter_column_name);
    if (!predicate)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Output nodes for ActionsDAG do not contain filter column name {}. DAG:\n{}",
            filter_column_name, filter_dag.dumpDAG());

    /// A constant filter has nothing to push.
    if (predicate->type == ActionsDAG::ActionType::COLUMN)
        return result;

    /// The JOIN actions and the keys. A key of a pair of different types is cast to the common type,
    /// without a context, like the casts that the conversion to the physical join adds to match the keys.
    struct Key
    {
        const ActionsDAG::Node * key_of_opposite_side;
        bool is_left;
        bool is_cross_type;
    };
    ActionsDAG over_streams;
    UnorderedMapWithMemoryTracking<const ActionsDAG::Node *, Key> key_nodes;
    ActionsDAG::NodeRawConstPtrs key_nodes_list;
    if (logical_join)
    {
        const auto join_outputs = logical_join->getOutputActions();
        ActionsDAG::NodeRawConstPtrs join_nodes;
        join_nodes.reserve(join_outputs.size() + 2 * key_pairs.size());
        for (const auto & output : join_outputs)
            join_nodes.push_back(output.getNode());
        for (const auto & pair : key_pairs)
        {
            join_nodes.push_back(pair.left.getNode());
            join_nodes.push_back(pair.right.getNode());
        }

        ActionsDAG::NodeMapping join_copies;
        over_streams = ActionsDAG::cloneSubDAG(join_nodes, join_copies, /*remove_aliases=*/ false);
        /// The filter reads the JOIN output by name. A key is not read by name.
        over_streams.getOutputs().resize(join_outputs.size());

        auto in_common_type = [&](const JoinActionRef & key, const DataTypePtr & common_type) -> const ActionsDAG::Node *
        {
            const auto * node = join_copies.at(key.getNode());
            if (!common_type || areTypesIdentical(*node->result_type, *common_type))
                return node;
            return &over_streams.addCast(*node, common_type, fmt::format("__filterpushdown_cast{}", node->result_name), nullptr);
        };

        for (const auto & pair : key_pairs)
        {
            const auto * left_key = in_common_type(pair.left, pair.common_type);
            const auto * right_key = in_common_type(pair.right, pair.common_type);
            const bool is_cross_type = pair.common_type != nullptr;
            if (key_nodes.emplace(left_key, Key{right_key, true, is_cross_type}).second)
                key_nodes_list.push_back(left_key);
            if (key_nodes.emplace(right_key, Key{left_key, false, is_cross_type}).second)
                key_nodes_list.push_back(right_key);
        }
    }

    /// The copy of the filter on top. Every node of the filter gets a node that reads the columns of the streams.
    ActionsDAG::NodeMapping filter_copies;
    auto filter_copy = filter_dag.clone(filter_copies);
    ActionsDAG::NodeMapping bound_inputs;
    if (logical_join)
        over_streams.mergeInplace(std::move(filter_copy), bound_inputs, /*remove_dangling_inputs=*/ false);
    else
        over_streams = std::move(filter_copy);

    auto over_streams_node = [&](const ActionsDAG::Node * filter_node)
    {
        const auto * copy = filter_copies.at(filter_node);
        auto it = bound_inputs.find(copy);
        return it == bound_inputs.end() ? copy : it->second;
    };

    /// What a conjunct can read: the available columns of each side that the filter reads, and the admitted keys.
    /// A key is found by what it computes, so it can be a column, an expression that the filter computes again,
    /// or sit under a conversion of the JOIN such as `toNullable`.
    NodeSet left_columns;
    NodeSet right_columns;
    {
        const NameSet left_names(left.available_columns.begin(), left.available_columns.end());
        const NameSet right_names(right.available_columns.begin(), right.available_columns.end());
        ActionsDAG::NodeRawConstPtrs filter_inputs;
        for (const auto * input : filter_dag.getInputs())
            filter_inputs.push_back(over_streams_node(input));

        visitReachableNodes(filter_inputs, [&](const ActionsDAG::Node * node)
        {
            if (node->type == ActionsDAG::ActionType::INPUT)
            {
                if (left_names.contains(node->result_name))
                    left_columns.insert(node);
                if (right_names.contains(node->result_name))
                    right_columns.insert(node);
            }
            return VisitResult::Continue;
        });
    }

    NodeSet keys;
    NodeSet cross_type_keys;
    /// For a node equal to a key of the opposite stream, the key of this stream.
    ActionsDAG::NodeMapping keys_of_left_stream;
    ActionsDAG::NodeMapping keys_of_right_stream;
    if (!key_nodes_list.empty())
    {
        const auto matches = matchTrees(key_nodes_list, over_streams, /*check_monotonicity=*/ false);
        for (const auto & node : over_streams.getNodes())
        {
            auto it = matches.find(&node);
            if (it == matches.end() || !it->second.node || it->second.monotonicity)
                continue;
            auto key_it = key_nodes.find(it->second.node);
            if (key_it == key_nodes.end())
                continue;

            const auto & key = key_it->second;
            (key.is_left ? keys_of_right_stream : keys_of_left_stream).emplace(&node, key.key_of_opposite_side);
            if (key.is_left ? left.keys_admitted : right.keys_admitted)
            {
                keys.insert(&node);
                if (key.is_cross_type)
                    cross_type_keys.insert(&node);
            }
        }
    }

    /// The conjuncts in the order they are written, as `and` evaluates them.
    auto filter_conjuncts = ActionsDAG::extractConjunctionAtoms(predicate);
    std::ranges::reverse(filter_conjuncts);
    ActionsDAG::NodeRawConstPtrs conjuncts;
    conjuncts.reserve(filter_conjuncts.size());
    for (const auto * conjunct : filter_conjuncts)
        conjuncts.push_back(over_streams_node(conjunct));

    auto conjuncts_over = [&](const NodeSet & nodes)
    {
        auto list = over_streams.getConjunctionsOver(over_streams_node(predicate), nodes);
        return NodeSet(list.begin(), list.end());
    };
    auto for_left = conjuncts_over(left_columns);
    auto for_right = conjuncts_over(right_columns);
    auto for_both = conjuncts_over(keys);

    /// The other side has a cast in place of a cross-type key. The cast can be constant where the key is not, and the
    /// key is computed a second time, so a conjunct must read only the value of such a key, the same in both evaluations.
    if (!cross_type_keys.empty())
    {
        static constexpr auto is_unstable_within_query = [](const IFunctionBase & function)
        { return function.isStateful() || !function.isDeterministicInScopeOfQuery(); };

        std::erase_if(for_both, [&](const ActionsDAG::Node * conjunct)
        {
            ActionsDAG::NodeRawConstPtrs representation_readers;
            ActionsDAG::getInputsReadByRepresentation(*conjunct, &representation_readers);
            auto is_cross_type_key = [&](const ActionsDAG::Node * node) { return cross_type_keys.contains(node); };
            if (anyReachableNode(representation_readers, is_cross_type_key))
                return true;

            return anyReachableNode({conjunct}, is_cross_type_key)
                && anyReachableNode({conjunct}, [](const ActionsDAG::Node * node)
                    { return ActionsDAG::hasUnsafeHiddenLambdaBody(*node, is_unstable_within_query); });
        });
    }

    /// A conjunct without columns, such as a constant, passes for every side. A side whose columns the filter does
    /// not read, like the padded side of an OUTER join, must not get it: the rows that the JOIN pads would escape it.
    /// If no row passes the filter at all, the JOIN output is empty anyway, and such a side is not read in vain.
    if (!filter_is_always_false)
    {
        if (left_columns.empty())
            for_left.clear();
        if (right_columns.empty())
            for_right.clear();
        if (left_columns.empty() || right_columns.empty())
            std::erase_if(for_both, [&](const ActionsDAG::Node * conjunct)
            {
                return !anyReachableNode({conjunct}, [&](const ActionsDAG::Node * node) { return keys.contains(node); });
            });
    }

    /// Two copies of a runtime filter can pass different rows, so a side that the JOIN only matches against gets none.
    auto is_runtime_filter = [](const ActionsDAG::Node * conjunct)
    {
        return anyReachableNode({conjunct}, [](const ActionsDAG::Node * node) { return node->is_runtime_filter_id; });
    };

    auto choose_conjuncts = [&](const NodeSet & of_side, const JoinSideForFilter & side)
    {
        ActionsDAG::NodeRawConstPtrs chosen;
        if (!side.accepts_filter)
            return chosen;

        for (const auto * conjunct : conjuncts)
            if (of_side.contains(conjunct) || (for_both.contains(conjunct) && !(side.only_matched && is_runtime_filter(conjunct))))
                chosen.push_back(conjunct);
        return chosen;
    };

    /// The condition of a side: the conjunction with the keys of that side, then the columns of the stream,
    /// which pass through to the JOIN.
    auto build_filter = [&](const ActionsDAG::NodeRawConstPtrs & chosen, const Block & header, const ActionsDAG::NodeMapping & keys_of_stream)
        -> std::optional<ActionsDAG>
    {
        /// A constant predicate has nothing to push.
        if (chosen.empty() || (chosen.size() == 1 && chosen.front()->type == ActionsDAG::ActionType::COLUMN))
            return {};

        auto dag = ActionsDAG::buildFilterActionsDAGWithReplacedNodes(chosen, keys_of_stream);

        UnorderedMapWithMemoryTracking<std::string_view, ActionsDAG::NodeRawConstPtrs> inputs;
        for (const auto * input : dag.getInputs())
        {
            /// The conjunct leaves the filter above the JOIN, so it must not silently read the other side here.
            if (!header.has(input->result_name))
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Filter pushed to one side of JOIN reads column {} that this side does not have. DAG:\n{}",
                    input->result_name, dag.dumpDAG());
            inputs[input->result_name].push_back(input);
        }

        /// The filter column becomes a constant after the filter, so it must not be a column that the JOIN reads.
        auto & outputs = dag.getOutputs();
        if (header.has(outputs[0]->result_name))
            outputs[0] = &dag.addAlias(*outputs[0], "__filter" + outputs[0]->result_name);

        UnorderedMapWithMemoryTracking<std::string_view, size_t> used_inputs;
        for (const auto & column : header.getColumnsWithTypeAndName())
        {
            const auto & inputs_of_name = inputs[column.name];
            const size_t position = used_inputs[column.name]++;
            outputs.push_back(position < inputs_of_name.size() ? inputs_of_name[position] : &dag.addInput(column));
        }

        return dag;
    };

    const auto left_conjuncts = choose_conjuncts(for_left, left);
    const auto right_conjuncts = choose_conjuncts(for_right, right);
    result.left = build_filter(left_conjuncts, left.header, keys_of_left_stream);
    result.right = build_filter(right_conjuncts, right.header, keys_of_right_stream);
    if (!result.left && !result.right)
        return result;

    /// Only the conjuncts that a side really got leave the filter.
    NodeSet pushed;
    if (result.left)
        pushed.insert(left_conjuncts.begin(), left_conjuncts.end());
    if (result.right)
        pushed.insert(right_conjuncts.begin(), right_conjuncts.end());

    ActionsDAG::NodeRawConstPtrs kept;
    for (size_t i = 0; i < conjuncts.size(); ++i)
        if (!pushed.contains(conjuncts[i]))
            kept.push_back(filter_conjuncts[i]);

    result.is_filter_const_after_push_down = filter_dag.keepConjunctionsInFilter(filter_column_name, removes_filter, std::move(kept));
    return result;
}

std::optional<ActionsDAG> tryToExtractPartialPredicate(
    const ActionsDAG & original_dag,
    const std::string & filter_name,
    const Names & available_columns,
    bool only_conditions_safe_on_more_rows = false);

void addFilterOnTop(QueryPlan::Node & join_node, size_t child_idx, QueryPlan::Nodes & nodes, ActionsDAG filter_dag);

static size_t tryPushDownOverJoinStep(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, QueryPlan::Node * child_node)
{
    auto & parent = parent_node->step;
    QueryPlanStepPtr & child = child_node->step;
    auto * filter = assert_cast<FilterStep *>(parent.get());

    auto * logical_join = typeid_cast<JoinStepLogical *>(child.get());
    auto * join = typeid_cast<JoinStep *>(child.get());
    auto * filled_join = typeid_cast<FilledJoinStep *>(child.get());

    if (!join && !filled_join && !logical_join)
        return 0;

    /** For equivalent JOIN with condition `ON lhs.x_1 = rhs.y_1 AND lhs.x_2 = rhs.y_2 ...`, we can build equivalent sets of columns and this
      * will allow to push conditions that only use columns from equivalent sets to both sides of JOIN, without considering JOIN type.
      *
      * For example: `FROM lhs INNER JOIN rhs ON lhs.id = rhs.id AND lhs.value = rhs.value`
      * In this example columns `id` and `value` from both tables are equivalent.
      *
      * During filter push down for different JOIN types filter push down logic is different:
      *
      * 1. For INNER JOIN we can push all valid conditions to both sides of JOIN. We also can push all valid conditions that use columns from
      * equivalent sets to both sides of JOIN.
      * 2. For LEFT/RIGHT JOIN we can push conditions that use columns from LEFT/RIGHT stream to LEFT/RIGHT JOIN side. We can also push conditions
      * that use columns from LEFT/RIGHT equivalent sets to RIGHT/LEFT JOIN side.
      *
      * Additional filter push down optimizations:
      * 1. TODO: Support building equivalent sets for more than 2 JOINS. It is possible, but will require more complex analysis step.
      * 2. TODO: Support building equivalent sets for JOINs with more than 1 clause.
      * 3. TODO: It is possible to pull up filter conditions from LEFT/RIGHT stream and push conditions that use columns from LEFT/RIGHT equivalent sets
      * to RIGHT/LEFT JOIN side.
      */

    const auto & join_header = child->getOutputHeader();
    const TableJoin * table_join_ptr = nullptr;
    if (join)
        table_join_ptr = &join->getJoin()->getTableJoin();
    else if (filled_join)
        table_join_ptr = &filled_join->getJoin()->getTableJoin();

    const auto & left_stream_input_header = child->getInputHeaders().front();
    const auto & right_stream_input_header = child->getInputHeaders().back();

    if (table_join_ptr && table_join_ptr->kind() == JoinKind::Full)
        return 0;
    if (logical_join && logical_join->getJoinOperator().kind == JoinKind::Full)
        return 0;

    /// PASTE JOIN aligns rows from both sides by position, and pushing filters
    /// to either side may change relative alignment
    if ((table_join_ptr && table_join_ptr->kind() == JoinKind::Paste)
        || (logical_join && logical_join->getJoinOperator().kind == JoinKind::Paste))
        return 0;

    VectorWithMemoryTracking<JoinKeyPair> key_pairs;
    if (logical_join)
    {
        EquivalentJoinKeySet equivalent_sets;
        auto equivalent_expressions = buildEquialentSetsForJoinStepLogical(equivalent_sets, logical_join, child_node->children);
        UnorderedSetWithMemoryTracking<JoinActionRefPair, JoinActionRefPairHash> equivalent_expressions_set(equivalent_expressions.begin(), equivalent_expressions.end());
        VectorWithMemoryTracking<JoinActionRefPair> extra_equivalent_expressions;
        for (const auto & [lhs, rhs] : equivalent_expressions)
        {
            for (const auto & eq_expr : equivalent_sets.getClass(lhs))
            {
                if (!eq_expr.isFromSameActions(lhs) || !eq_expr.fromRight())
                    continue;
                if (equivalent_expressions_set.contains({lhs, eq_expr}))
                    continue;
                equivalent_expressions_set.emplace(lhs, eq_expr);
                extra_equivalent_expressions.emplace_back(lhs, eq_expr);
            }
            for (const auto & eq_expr : equivalent_sets.getClass(rhs))
            {
                if (!eq_expr.isFromSameActions(rhs) || !eq_expr.fromLeft())
                    continue;
                if (equivalent_expressions_set.contains({eq_expr, rhs}))
                    continue;
                equivalent_expressions_set.emplace(eq_expr, rhs);
                extra_equivalent_expressions.emplace_back(eq_expr, rhs);
            }
        }
        equivalent_expressions.append_range(std::move(extra_equivalent_expressions));

        key_pairs = getJoinKeyPairs(*logical_join, equivalent_expressions);
    }

    auto get_available_columns_for_filter = [&](bool push_to_left_stream, bool filter_push_down_input_columns_available, bool require_stable_types = false)
    {
        Names available_input_columns_for_filter;

        if (!filter_push_down_input_columns_available)
            return available_input_columns_for_filter;

        const auto & input_header = push_to_left_stream ? left_stream_input_header : right_stream_input_header;
        const auto & input_columns_names = input_header->getNames();

        for (const auto & name : input_columns_names)
        {
            if (!join_header->has(name))
                continue;

            /// A filter pushed over `JoinStepLogical` is built over the JOIN actions, which hold the conversions of the JOIN.
            /// The other joins and the partial predicate have no such actions, so they take only the columns of a stable type.
            if ((!logical_join || require_stable_types)
                && !input_header->getByName(name).type->equals(*join_header->getByName(name).type))
                continue;

            available_input_columns_for_filter.push_back(name);
        }

        return available_input_columns_for_filter;
    };

    bool left_stream_filter_push_down_input_columns_available = true;
    bool right_stream_filter_push_down_input_columns_available = true;

    if (table_join_ptr && table_join_ptr->kind() == JoinKind::Left)
        right_stream_filter_push_down_input_columns_available = false;
    else if (table_join_ptr && table_join_ptr->kind() == JoinKind::Right)
        left_stream_filter_push_down_input_columns_available = false;

    if (logical_join && logical_join->getJoinOperator().kind == JoinKind::Left)
        right_stream_filter_push_down_input_columns_available = false;
    else if (logical_join && logical_join->getJoinOperator().kind == JoinKind::Right)
        left_stream_filter_push_down_input_columns_available = false;

    /** We disable push down to right table in cases:
      * 1. Right side is already filled. Example: JOIN with Dictionary.
      * 2. ASOF Right join is not supported.
      */
    bool allow_push_down_to_right = join && join->allowPushDownToRight() && table_join_ptr && table_join_ptr->strictness() != JoinStrictness::Asof;
    if (logical_join)
    {
        bool has_logical_lookup = typeid_cast<JoinStepLogicalLookup *>(child_node->children.back()->step.get());
        allow_push_down_to_right = !has_logical_lookup && logical_join->getJoinOperator().strictness != JoinStrictness::Asof;
    }

    if (!allow_push_down_to_right)
        right_stream_filter_push_down_input_columns_available = false;

    const bool is_filter_column_const_before = isFilterColumnConst(*filter);

    /// Capture the original constant column value before push-down modifies the expression DAG.
    ColumnPtr original_filter_const_column;
    if (is_filter_column_const_before)
        original_filter_const_column = filter->getOutputHeader()->getByName(filter->getFilterColumnName()).column;

    /// With no input given only the conjuncts that read no column are evaluated, so FALSE means no row passes.
    const bool filter_is_always_false
        = (left_stream_filter_push_down_input_columns_available || right_stream_filter_push_down_input_columns_available)
        && !isSensitiveToEvaluationCount(filter->getExpression())
        && filterResultForNotMatchedRows(filter->getExpression(), filter->getFilterColumnName(), Block{}) == FilterResult::FALSE;

    const JoinKind join_kind = logical_join ? logical_join->getJoinOperator().kind : table_join_ptr->kind();
    const JoinStrictness join_strictness = logical_join ? logical_join->getJoinOperator().strictness : table_join_ptr->strictness();
    const bool left_stream_only_matched = join_kind == JoinKind::Right && join_strictness != JoinStrictness::Semi;
    const bool right_stream_only_matched = join_kind == JoinKind::Left && join_strictness != JoinStrictness::Semi;

    /// The keys of a side stand for the keys of the other side where the JOIN pads no row of that side.
    /// A SEMI join keeps only matched rows, so there it also holds for the side that an outer join would pad.
    bool left_keys_admitted = false;
    bool right_keys_admitted = false;
    if (logical_join)
    {
        const auto type_changing_sides = logical_join->typeChangingSides();
        left_keys_admitted = left_stream_filter_push_down_input_columns_available
            || (join_kind == JoinKind::Right && join_strictness == JoinStrictness::Semi && !type_changing_sides.contains(JoinTableSide::Left));
        right_keys_admitted = right_stream_filter_push_down_input_columns_available
            || (join_kind == JoinKind::Left && join_strictness == JoinStrictness::Semi && !type_changing_sides.contains(JoinTableSide::Right));
    }

    const JoinSideForFilter left_side{
        .header = *left_stream_input_header,
        .available_columns = get_available_columns_for_filter(true /*push_to_left_stream*/, left_stream_filter_push_down_input_columns_available),
        .keys_admitted = left_keys_admitted,
        .only_matched = left_stream_only_matched};
    const JoinSideForFilter right_side{
        .header = *right_stream_input_header,
        .available_columns = get_available_columns_for_filter(false /*push_to_left_stream*/, right_stream_filter_push_down_input_columns_available),
        .keys_admitted = right_keys_admitted,
        .only_matched = right_stream_only_matched,
        .accepts_filter = allow_push_down_to_right};

    auto filters = splitFilterForJoinSides(
        filter->getExpression(), filter->getFilterColumnName(), filter->removesFilterColumn(),
        logical_join, key_pairs, left_side, right_side, filter_is_always_false);

    if (is_filter_column_const_before && !filters.is_filter_const_after_push_down)
    {
        filters.is_filter_const_after_push_down
            = constifyFilterColumnAfterPushDown(filter->getExpression(), filter->getFilterColumnName(), original_filter_const_column);
    }
    else
    {
        materializeFilterColumnIfNeededAfterPushDown(*filter, is_filter_column_const_before, filters.is_filter_const_after_push_down);
    }

    size_t updated_steps = 0;

    auto add_filter_to_side = [&](std::optional<ActionsDAG> & side_filter, const Block & header, size_t child_idx, JoinKind side)
    {
        if (!side_filter)
            return;

        /// Copy the name: the DAG is moved into the new FilterStep below, whose constructor may fold and
        /// prune the very node this name belongs to, leaving a reference dangling.
        const String result_name = side_filter->getOutputs()[0]->result_name;
        const bool removes_side_filter = !header.has(result_name);
        updated_steps += addNewFilterStepOrThrow(
            parent_node, nodes, {std::move(*side_filter), 0, removes_side_filter, false}, child_idx, false /*update_parent_filter*/);
        LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"), "Pushed down filter {} to the {} side of join", result_name, side);
    };
    add_filter_to_side(filters.left, *left_stream_input_header, 0, JoinKind::Left);
    add_filter_to_side(filters.right, *right_stream_input_header, 1, JoinKind::Right);

    if (updated_steps > 0)
    {
        const auto & filter_column_name = filter->getFilterColumnName();
        auto & filter_expression = filter->getExpression();

        const auto * filter_node = filter_expression.tryFindInOutputs(filter_column_name);
        if (!filter_node && !filter->removesFilterColumn())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Filter column {} was removed from ActionsDAG but it is needed in result. DAG:\n{}",
                        filter_column_name, filter_expression.dumpDAG());


        /// Filter column was replaced to constant.
        if (!filter_node || filters.is_filter_const_after_push_down)
        {
            /// This means that all predicates of filter were pushed down.
            /// Replace current actions to expression, as we don't need to filter anything.
            parent = std::make_unique<ExpressionStep>(child->getOutputHeader(), std::move(filter_expression));
            filter = nullptr;
        }
        else
        {
            filter->updateInputHeader(child->getOutputHeader());
        }
    }

    const bool disj_pushdown_enabled = (join && join->useJoinDisjunctionsPushDown()) || (logical_join && logical_join->getSettings().use_join_disjunctions_push_down);
    size_t pushed_partial_filters = 0;
    if (filter && disj_pushdown_enabled)
    {
        if ((join && join->isDisjunctionsOptimizationApplied()) ||
            (logical_join && logical_join->isDisjunctionsOptimizationApplied()) ||
            (filled_join && filled_join->isDisjunctionsOptimizationApplied()))
        {
            return updated_steps;
        }

        /// Unlike the main push-down above, addFilterOnTop builds the partial FilterStep directly
        /// against the join input header without the JOIN actions under it. So a function
        /// node whose type was computed for the join output (e.g. equals over a USING key widened to
        /// Nullable) would be applied to the non-widened input column and trip the result-type check
        /// in updateHeader. Restrict the partial predicate to columns with stable types across the join.
        Names left_stream_stable_columns_to_push_down = get_available_columns_for_filter(
            true /*push_to_left_stream*/, left_stream_filter_push_down_input_columns_available, /*require_stable_types=*/true);
        Names right_stream_stable_columns_to_push_down = get_available_columns_for_filter(
            false /*push_to_left_stream*/, right_stream_filter_push_down_input_columns_available, /*require_stable_types=*/true);

        /// An empty column set does not mean "nothing is pushable": a column-free conjunct such as a folded
        /// `WHERE 0` still passes `tryToExtractPartialPredicate`, because it depends on no column at all.
        /// A side whose push-down is disabled must not receive it either: for a prepared lookup on the
        /// right (`JoinStepLogicalLookup`) the inserted `Filter` would hide the storage from the direct-join
        /// detection, and a forced `join_algorithm = 'direct'` would fail with `NOT_IMPLEMENTED` instead
        /// of returning an empty result. The estimator still sees the empty read from the other side.
        if (left_stream_filter_push_down_input_columns_available)
        {
            auto left_partial_filter_dag = tryToExtractPartialPredicate(filter->getExpression(), filter->getFilterColumnName(), left_stream_stable_columns_to_push_down);
            if (left_partial_filter_dag.has_value())
            {
                const auto partial_predicate_column_name = left_partial_filter_dag->getOutputs().front()->result_name;
                addFilterOnTop(*child_node, 0, nodes, std::move(*left_partial_filter_dag));
                ++updated_steps;
                ++pushed_partial_filters;
                LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"),
                    "Pushed down partial filter {} to the {} side of join",
                    partial_predicate_column_name,
                    JoinKind::Left);
            }
        }

        if (right_stream_filter_push_down_input_columns_available)
        {
            auto right_partial_filter_dag = tryToExtractPartialPredicate(filter->getExpression(), filter->getFilterColumnName(), right_stream_stable_columns_to_push_down);
            if (right_partial_filter_dag.has_value())
            {
                const auto partial_predicate_column_name = right_partial_filter_dag->getOutputs().front()->result_name;
                addFilterOnTop(*child_node, 1, nodes, std::move(*right_partial_filter_dag));
                ++updated_steps;
                ++pushed_partial_filters;
                LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"),
                    "Pushed down partial filter {} to the {} side of join",
                    partial_predicate_column_name,
                    JoinKind::Right);
            }
        }

        if (join)
            join->setDisjunctionsOptimizationApplied(true);
        if (logical_join)
            logical_join->setDisjunctionsOptimizationApplied(true);
        if (filled_join)
            filled_join->setDisjunctionsOptimizationApplied(true);

        /// A partial filter is inserted two levels below this node, as a new child of the join, with the
        /// expression that renames the read's columns under it. The return value is how many layers the
        /// traversal comes back down, so reporting one leaves the new filter unvisited: it is never
        /// merged with that expression, stays one step away from the read, and `optimizePrewhere` - which
        /// only takes a filter adjacent to the read - cannot move it. Three reaches it, the same depth
        /// the union push above reports for the filters it inserts.
        if (pushed_partial_filters)
            updated_steps = std::max<size_t>(updated_steps, 3);
    }

    return updated_steps;
}

/// Whether a conjunct might fix a column to a single value, and so let a read go in order:
/// `tenant = 42` can, `tenant > 42` or a join runtime filter cannot. Any column counts, in the sorting
/// key or not: the read is inside the fragment and its key is not looked up here. A superset of what
/// `appendFixedColumnsFromFilterExpression`, the analysis read-in-order uses, can find: an `equals` or an
/// `isNotDistinctFrom` anywhere inside, or an input reached the way it looks for one - through aliases and
/// `and`. Such an input is a column of the fragment, which may compute it as an equality.
static bool mayFixColumn(const ActionsDAG::Node * conjunct)
{
    VectorWithMemoryTracking<const ActionsDAG::Node *> stack{conjunct};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (node->type == ActionsDAG::ActionType::INPUT)
            return true;
        if (node->type == ActionsDAG::ActionType::ALIAS
            || (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base->getName() == "and"))
            stack.insert(stack.end(), node->children.begin(), node->children.end());
    }

    return anyReachableNode({conjunct}, [](const ActionsDAG::Node * node)
    {
        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & name = node->function_base->getName();
            if (name == "equals" || name == "isNotDistinctFrom")
                return true;
        }
        return false;
    });
}

/// a filter on the column can merge with the expressions below and go below a step that removes rows (e.g. a JOIN),
/// so the steps below must make the column from a source step or an ARRAY JOIN with only functions that cannot throw
static bool isColumnSafeOnMoreRows(const QueryPlan::Node * node, const String & column_name)
{
    NameSet names{column_name};
    while (!names.empty() && !node->children.empty())
    {
        if (node->children.size() != 1)
            return false;

        const ActionsDAG * dag = nullptr;
        if (const auto * expression = typeid_cast<const ExpressionStep *>(node->step.get()))
            dag = &expression->getExpression();
        else if (const auto * filter = typeid_cast<const FilterStep *>(node->step.get()))
            dag = &filter->getExpression();
        else if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(node->step.get()))
        {
            for (const auto & column : array_join->getColumns())
                names.erase(column);
        }
        else
            return false;

        if (dag)
        {
            NameSet inputs;
            for (const auto & name : names)
            {
                const auto * output = dag->tryFindInOutputs(name);
                if (!output)
                {
                    /// the step passes through a column that its actions do not read
                    if (std::ranges::any_of(dag->getInputs(), [&](const auto * input) { return input->result_name == name; }))
                        return false;
                    inputs.insert(name);
                    continue;
                }
                if (!canBeMovedToTheFrontOfAndChain(output))
                    return false;
                for (const auto * reachable : findReachableNodes({output}))
                    if (reachable->type == ActionsDAG::ActionType::INPUT)
                        inputs.insert(reachable->result_name);
            }
            names = std::move(inputs);
        }

        node = node->children.front();
    }

    /// other sources (e.g. `Merge` or remote tables) can give a filter to a plan that this walk does not see, where it can go below a JOIN
    const auto * source = node->step.get();
    return names.empty() || typeid_cast<const ReadFromMergeTree *>(source) || typeid_cast<const ReadFromMemoryStorageStep *>(source)
        || typeid_cast<const ReadFromStorageLogStep *>(source) || typeid_cast<const ReadFromSystemNumbersStep *>(source)
        || dynamic_cast<const ReadFromPreparedSource *>(source);
}

size_t tryPushDownFilter(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = typeid_cast<FilterStep *>(parent.get());

    if (!filter)
        return 0;

    if (filter->getExpression().hasStatefulFunctions())
        return 0;

    const auto * merging_aggregated = typeid_cast<MergingAggregatedStep *>(child.get());
    const auto * aggregating = typeid_cast<AggregatingStep *>(child.get());

    if (aggregating || merging_aggregated)
    {
        auto index_hints = ActionsDAG::PushDownIndexHints::Implied;

        /// If aggregating is GROUPING SETS, and not all the identifiers exist in all
        /// of the grouping sets, we could not push the filter down.
        bool is_grouping_sets = aggregating ? aggregating->isGroupingSets() : merging_aggregated->isGroupingSets();
        if (is_grouping_sets)
        {
            /// Cannot push down filter if type has been changed. MergingAggregated does not change types.
            if (aggregating && aggregating->isGroupByUseNulls())
                return 0;

            const auto & actions = filter->getExpression();
            const auto & filter_node = actions.findInOutputs(filter->getFilterColumnName());

            NameSet index_hint_inputs;
            auto identifiers_in_predicate = findIdentifiersOfNode(&filter_node, index_hint_inputs);

            const auto & grouping_sets = aggregating ? aggregating->getGroupingSetsParamsList() : merging_aggregated->getGroupingSetsParamsList();
            /// A grouping set without keys returns a row for an empty input, like an aggregation without keys.
            if (std::ranges::any_of(grouping_sets, [](const GroupingSetsParams & grouping_set) { return grouping_set.used_keys.empty(); }))
                return 0;
            if (!identifiersIsAmongAllGroupingSets(grouping_sets, identifiers_in_predicate))
                return 0;

            /// A hint reading a column that some set lacks would prune rows that set aggregates.
            if (!identifiersIsAmongAllGroupingSets(grouping_sets, index_hint_inputs))
                index_hints = ActionsDAG::PushDownIndexHints::None;
        }

        const auto & params = aggregating ? aggregating->getParams() : merging_aggregated->getParams();
        const auto & keys = params.keys;
        /** The filter is applied either to aggregation keys or aggregation result
          * (columns under aggregation is not available in outer scope, so we can't have a filter for them).
          * The filter for the aggregation result is not pushed down, so the only valid case is filtering aggregation keys.
          * In case keys are empty, do not push down the filter.
          * Also with empty keys we can have an issue with `empty_result_for_aggregation_by_empty_set`,
          * since we can gen a result row when everything is filtered.
          */
        if (keys.empty())
            return 0;

        if (auto updated_steps = tryAddNewFilterStep(parent_node, true, nodes, keys, index_hints))
            return updated_steps;
    }

    if (const auto * limit_by = typeid_cast<LimitByStep *>(child.get()))
    {
        if (!settings.filter_push_down_below_limit_by)
            return 0;

        /// A predicate on the LIMIT BY key columns removes whole groups, so the surviving
        /// per-group rows (and therefore the result) are identical whether it runs above or
        /// below the LIMIT BY. But it is only safe to push when every non-empty input group
        /// keeps at least one output row, i.e. `OFFSET 0` and `LIMIT >= 1`. Otherwise a group
        /// can be fully discarded by the step (OFFSET past its size, or `LIMIT 0 BY`), and a
        /// pushed key predicate would then be evaluated on rows the original query never
        /// reached -- changing exception semantics for throwing key expressions
        /// (e.g. `intDiv(1, key)` on a group that OFFSET would have dropped). This mirrors
        /// AggregatingStep, where GROUP BY likewise never empties a non-empty group.
        /// `step_changes_the_number_of_rows = true`: LIMIT BY drops rows, so
        /// non-deterministic key predicates must NOT be pushed.
        const auto & keys = limit_by->getColumns();
        if (keys.empty() || limit_by->getGroupOffset() != 0 || limit_by->getGroupLength() == 0)
            return 0;

        if (auto updated_steps = tryAddNewFilterStep(parent_node, true, nodes, keys, ActionsDAG::PushDownIndexHints::Implied))
            return updated_steps;
    }

    if (typeid_cast<CreatingSetsStep *>(child.get()))
    {
        /// CreatingSets does not change header.
        /// We can push down filter and update header.
        ///                       - Something
        /// Filter - CreatingSets - CreatingSet
        ///                       - CreatingSet
        auto input_streams = child->getInputHeaders();
        input_streams.front() = filter->getOutputHeader();
        child = std::make_unique<CreatingSetsStep>(input_streams);
        std::swap(parent, child);
        std::swap(parent_node->children, child_node->children);
        std::swap(parent_node->children.front(), child_node->children.front());
        ///              - Filter - Something
        /// CreatingSets - CreatingSet
        ///              - CreatingSet
        return 2;
    }

    if (auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(child.get()))
    {
        /// CreatingSets does not change header.
        /// We can push down filter and update header.
        /// Filter - DelayedCreatingSets - Something

        child = std::make_unique<DelayedCreatingSetsStep>(
            filter->getOutputHeader(),
            delayed->detachSets(),
            delayed->getNetworkTransferLimits(),
            delayed->getPreparedSetsCache());

        std::swap(parent, child);
        /// DelayedCreatingSets - Filter - Something
        return 2;
    }

    if (auto * totals_having = typeid_cast<TotalsHavingStep *>(child.get()))
    {
        /// If totals step has HAVING expression, skip it for now.
        /// TODO:
        /// We can merge HAVING expression with current filter.
        /// Also, we can push down part of HAVING which depend only on aggregation keys.
        if (totals_having->getActions())
            return 0;

        Names keys;
        const auto & header = totals_having->getInputHeaders().front();
        for (const auto & column : *header)
            if (typeid_cast<const DataTypeAggregateFunction *>(column.type.get()) == nullptr)
                keys.push_back(column.name);

        /// NOTE: this optimization changes TOTALS value. Example:
        ///   `select * from (select y, sum(x) from (
        ///        select number as x, number % 4 as y from numbers(10)
        ///    ) group by y with totals) where y != 2`
        /// Optimization will replace totals row `y, sum(x)` from `(0, 45)` to `(0, 37)`.
        /// It is expected to be OK, because the AST-level push-down that preceded this optimization did the same.
        if (auto updated_steps = tryAddNewFilterStep(parent_node, false, nodes, keys, ActionsDAG::PushDownIndexHints::None))
            return updated_steps;
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(child.get()))
    {
        const auto & keys = array_join->getColumns();
        UnorderedSetWithMemoryTracking<std::string_view> keys_set(keys.begin(), keys.end());

        const auto & array_join_header = array_join->getInputHeaders().front();

        Names allowed_inputs;
        for (const auto & column : *array_join_header)
            if (!keys_set.contains(column.name))
                allowed_inputs.push_back(column.name);

        if (auto updated_steps = tryAddNewFilterStep(parent_node, true, nodes, allowed_inputs))
            return updated_steps;

        /// e.g. an OR over the elements of several arrays: add the part that the input can compute below, keep the filter above
        if (!array_join->hasRelaxedFilterBelow())
        {
            Names safe_inputs;
            for (const auto * input : filter->getExpression().getInputs())
                if (!keys_set.contains(input->result_name) && isColumnSafeOnMoreRows(child_node->children.front(), input->result_name))
                    safe_inputs.push_back(input->result_name);

            auto relaxed_filter = tryToExtractPartialPredicate(
                filter->getExpression(), filter->getFilterColumnName(), safe_inputs, /*only_conditions_safe_on_more_rows=*/ true);
            if (relaxed_filter)
            {
                array_join->setHasRelaxedFilterBelow();
                addFilterOnTop(*child_node, 0, nodes, std::move(*relaxed_filter));
                return 3;
            }
        }
    }

    if (auto updated_steps = simplePushDownOverStep<DistinctStep>(parent_node, true, nodes, child))
        return updated_steps;

    if (auto updated_steps = simplePushDownOverStep<BuildRuntimeFilterStep>(parent_node, true, nodes, child))
        return updated_steps;

    if (auto updated_steps = tryPushDownOverJoinStep(parent_node, nodes, child_node))
        return updated_steps;

    /// TODO.
    /// We can filter earlier if expression does not depend on WITH FILL columns.
    /// But we cannot just push down condition, because other column may be filled with defaults.
    ///
    /// It is possible to filter columns before and after WITH FILL, but such change is not idempotent.
    /// So, appliying this to pair (Filter -> Filling) several times will create several similar filters.
    // if (auto * filling = typeid_cast<FillingStep *>(child.get()))
    // {
    // }

    /// Same reason for Cube
    // if (auto * cube = typeid_cast<CubeStep *>(child.get()))
    // {
    // }

    if (typeid_cast<SortingStep *>(child.get()))
    {
        Names allowed_inputs = child->getOutputHeader()->getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, false, nodes, allowed_inputs))
            return updated_steps;
    }

    if (typeid_cast<CreateSetAndFilterOnTheFlyStep *>(child.get()))
    {
        Names allowed_inputs = child->getOutputHeader()->getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, false, nodes, allowed_inputs))
            return updated_steps;
    }

    if (auto * union_step = typeid_cast<UnionStep *>(child.get()))
    {
        /// This rewrite forces every union branch input header to the pushed-down filter's
        /// output header, which assumes the union forwards each branch unchanged. Skip it
        /// when the union normalizes a branch (its output differs from some input header),
        /// e.g. it drops a Const that diverged across branches. Otherwise a branch still
        /// outputting Const would get a full input header and the mismatch would move here.
        const auto & union_output = *union_step->getOutputHeader();
        for (const auto & input_header : union_step->getInputHeaders())
            if (!blocksHaveEqualStructure(*input_header, union_output))
                return 0;

        /// Union does not change header.
        /// We can push down filter and update header.
        auto union_input_headers = child->getInputHeaders();
        auto expected_output = filter->getOutputHeader();

        for (auto & input_header : union_input_headers)
            input_header = expected_output;

        ///                - Something
        /// Filter - Union - Something
        ///                - Something

        child = std::make_unique<UnionStep>(union_input_headers, union_step->getMaxThreads(), union_step->isNarrowingAllowed());

        std::swap(parent, child);
        std::swap(parent_node->children, child_node->children);
        std::swap(parent_node->children.front(), child_node->children.front());

        ///       - Filter - Something
        /// Union - Something
        ///       - Something

        for (size_t i = 1; i < parent_node->children.size(); ++i)
        {
            auto & filter_node = nodes.emplace_back();
            filter_node.children.push_back(parent_node->children[i]);
            parent_node->children[i] = &filter_node;

            auto new_filter = std::make_unique<FilterStep>(
                filter_node.children.front()->step->getOutputHeader(),
                filter->getExpression().clone(),
                filter->getFilterColumnName(),
                filter->removesFilterColumn());
            new_filter->setOptimizeConstantColumnsAfterFilter(filter->getOptimizeConstantColumnsAfterFilter());
            filter_node.step = std::move(new_filter);
        }

        ///       - Filter - Something
        /// Union - Filter - Something
        ///       - Filter - Something

        return 3;
    }

    /// A filter left above the local fragment (see below) would keep this one from reaching it, while the
    /// remote step gathers every filter in the chain above it. So join the two, and split them together.
    if (typeid_cast<FilterStep *>(child.get()) && child_node->children.size() == 1
        && typeid_cast<ReadFromLocalParallelReplicaStep *>(child_node->children.front()->step.get()))
        return tryMergeFilters(parent_node, nodes, settings);

    if (auto * parallel_replicas_local_plan = typeid_cast<ReadFromLocalParallelReplicaStep *>(child.get()))
    {
        if (!parallel_replicas_local_plan->getContext()->getSettingsRef()[Setting::parallel_replicas_filter_pushdown])
            return 0;

        /// The replicas get a condition pushed here only through the splice into the query they are sent,
        /// which carries it conjunct by conjunct and may leave some out. What this copy of the fragment
        /// holds and they do not must not decide how the read is ordered - an equality fixes a sort key
        /// column, the read goes in order, and the initiator announces `WithOrder` to the shared
        /// coordinator against the replicas' `Default`. Nor may they hold what it does not, for the same
        /// reason the other way round. So a conjunct they miss stays above unless it cannot fix any column,
        /// even one outside the sorting key; one that cannot - a join runtime filter, which the splice
        /// never carries, or a range - still prunes here.
        const auto * condition = filter->getExpression().tryFindInOutputs(filter->getFilterColumnName());
        if (!condition)
            return 0;

        const auto missed = parallel_replicas_local_plan->conjunctsTheReplicasMiss(*condition);
        ActionsDAG::NodeRawConstPtrs to_push;
        bool some_stay_above = false;
        for (const auto * atom : ActionsDAG::extractConjunctionAtoms(condition))
        {
            if (std::ranges::find(missed, atom) != missed.end() && mayFixColumn(atom))
                some_stay_above = true;
            else
                to_push.push_back(atom);
        }

        if (to_push.empty())
            return 0;

        if (some_stay_above)
        {
            auto pushed = filter->getExpression().splitActionsForFilterPushDown(
                filter->getFilterColumnName(),
                filter->removesFilterColumn(),
                to_push,
                parallel_replicas_local_plan->getOutputHeader()->getColumnsWithTypeAndName());
            if (!pushed)
                return 0;

            String pushed_column = pushed->dag.getOutputs()[pushed->filter_pos]->result_name;
            parallel_replicas_local_plan->addFilter({std::move(pushed->dag), std::move(pushed_column), pushed->remove_filter});
            filter->updateInputHeader(parallel_replicas_local_plan->getOutputHeader());
            return 2;
        }

        // actual push down will be done when plan for local parallel replica will be optimized
        FilterDAGInfo info{filter->getExpression().clone(), filter->getFilterColumnName(), filter->removesFilterColumn()};
        parallel_replicas_local_plan->addFilter(std::move(info));
        std::swap(*parent_node, *child_node);
        return 1;
    }

    if (auto * read_from_merge = typeid_cast<ReadFromMerge *>(child.get()))
    {
        FilterDAGInfo info{filter->getExpression().clone(), filter->getFilterColumnName(), filter->removesFilterColumn()};
        read_from_merge->addFilter(std::move(info));
        std::swap(*parent_node, *child_node);
        return 1;
    }

    if (auto updated_steps = simplePushDownOverStep<CommonSubplanStep>(parent_node, false, nodes, child))
        return updated_steps;

    return 0;
}

}
