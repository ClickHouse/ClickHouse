#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Core/Joins.h>
#include <Core/SortDescription.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>

#include <queue>


namespace DB::QueryPlanOptimizations
{

namespace
{

const ActionsDAG::Node * dealiasNode(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

/// Information about a single volume-reducing function output in the parent's DAG.
struct FunctionInfo
{
    /// The output node in the parent's DAG (may be an ALIAS wrapping the function).
    const ActionsDAG::Node * output_node;
    /// The dealiased FUNCTION node.
    const ActionsDAG::Node * function_node;
};

/// A source column that is only consumed by volume-reducing functions and can be pushed down.
struct PushdownCandidate
{
    const ActionsDAG::Node * source_input_node;
    std::vector<FunctionInfo> functions;
};

/// Path classification for BFS traversal.
enum class PathKind : uint8_t
{
    INITIAL,
    VR,
    NON_VR,
};


/// BFS classification of INPUT nodes in the parent DAG.
/// Traverses from output nodes, propagating path kind through the DAG.
/// VR functions act as barriers: valid VR functions always propagate VR to children.
/// Returns a map of source column name -> PushdownCandidate.
std::unordered_map<std::string, PushdownCandidate>
classifyAndCollectCandidates(const ActionsDAG & parent_dag)
{
    /// Track classification of INPUT nodes (pessimistic: any NON_VR path wins).
    std::unordered_map<const ActionsDAG::Node *, PathKind> input_classification;

    auto classify_input = [&](const ActionsDAG::Node * input_node, PathKind kind)
    {
        auto it = input_classification.find(input_node);
        if (it == input_classification.end())
            input_classification[input_node] = kind;
        else if (kind == PathKind::NON_VR)
            it->second = PathKind::NON_VR;
    };

    /// Track which non-INPUT nodes have been processed to avoid redundant work.
    /// Each function node propagates a deterministic path kind (based on its own properties,
    /// not the incoming path), so processing once is sufficient.
    std::unordered_set<const ActionsDAG::Node *> processed;

    /// Set of valid VR function nodes found during BFS.
    std::unordered_set<const ActionsDAG::Node *> valid_vr_nodes;

    /// BFS queue: (node, path_kind from parent).
    std::queue<std::pair<const ActionsDAG::Node *, PathKind>> bfs_queue;

    for (const auto * output : parent_dag.getOutputs())
        bfs_queue.push({output, PathKind::INITIAL});

    while (!bfs_queue.empty())
    {
        auto [node, path_kind] = bfs_queue.front();
        bfs_queue.pop();

        const auto * actual = dealiasNode(node);

        if (actual->type == ActionsDAG::ActionType::INPUT)
        {
            PathKind effective = (path_kind == PathKind::INITIAL) ? PathKind::NON_VR : path_kind;
            classify_input(actual, effective);
            continue;
        }

        if (actual->type == ActionsDAG::ActionType::COLUMN)
            continue;

        /// Process each non-INPUT, non-COLUMN node only once.
        if (processed.contains(actual))
            continue;
        processed.insert(actual);

        if (actual->type == ActionsDAG::ActionType::FUNCTION)
        {
            bool is_vr = actual->function_base
                && actual->function_base->isSuitableForPushDownBeforeFilter();

            /// Validity check: exactly one distinct INPUT child, rest are COLUMN.
            bool is_valid_vr = false;
            if (is_vr)
            {
                const ActionsDAG::Node * source_input = nullptr;
                bool valid = true;
                for (const auto * child : actual->children)
                {
                    const auto * child_actual = dealiasNode(child);
                    if (child_actual->type == ActionsDAG::ActionType::INPUT)
                    {
                        if (source_input && source_input != child_actual)
                        {
                            valid = false;
                            break;
                        }
                        source_input = child_actual;
                    }
                    else if (child_actual->type != ActionsDAG::ActionType::COLUMN)
                    {
                        valid = false;
                        break;
                    }
                }
                is_valid_vr = valid && source_input != nullptr;
            }

            PathKind propagate_kind = is_valid_vr ? PathKind::VR : PathKind::NON_VR;

            if (is_valid_vr)
                valid_vr_nodes.insert(actual);

            for (const auto * child : actual->children)
                bfs_queue.push({child, propagate_kind});

            continue;
        }

        /// ARRAY_JOIN or other types — propagate NON_VR.
        for (const auto * child : actual->children)
            bfs_queue.push({child, PathKind::NON_VR});
    }

    /// Collect pushable candidates: output VR functions whose INPUT children are VR-only.
    std::unordered_map<std::string, PushdownCandidate> candidates;

    for (const auto * output : parent_dag.getOutputs())
    {
        const auto * func_node = dealiasNode(output);
        if (!valid_vr_nodes.contains(func_node))
            continue;

        /// Find the source INPUT child.
        const ActionsDAG::Node * source_input = nullptr;
        for (const auto * child : func_node->children)
        {
            const auto * child_actual = dealiasNode(child);
            if (child_actual->type == ActionsDAG::ActionType::INPUT)
            {
                source_input = child_actual;
                break;
            }
        }
        chassert(source_input);

        auto it = input_classification.find(source_input);
        if (it == input_classification.end() || it->second != PathKind::VR)
            continue;

        auto & cand = candidates[source_input->result_name];
        cand.source_input_node = source_input;
        cand.functions.push_back({output, func_node});
    }

    /// Safety verification: ensure no non-candidate function uses a candidate source column.
    /// This catches cases where a non-output VR function (acting as barrier in BFS)
    /// uses the source column but is not being pushed down.
    std::unordered_set<const ActionsDAG::Node *> candidate_func_nodes;
    for (const auto & [name, cand] : candidates)
        for (const auto & func : cand.functions)
            candidate_func_nodes.insert(func.function_node);

    for (const auto & node : parent_dag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || candidate_func_nodes.contains(&node))
            continue;
        for (const auto * child : node.children)
        {
            const auto * child_actual = dealiasNode(child);
            if (child_actual->type == ActionsDAG::ActionType::INPUT)
                candidates.erase(child_actual->result_name);
        }
    }

    return candidates;
}


/// Check whether a column is needed by a step's own operation (sort key, distinct key, etc.).
/// Returns true if the step needs this column — meaning we cannot remove it from the data stream.
bool isColumnUsedByStep(const IQueryPlanStep * step, const std::string & column_name)
{
    if (const auto * sorting = typeid_cast<const SortingStep *>(step))
    {
        for (const auto & col : sorting->getSortDescription())
            if (col.column_name == column_name)
                return true;
        return false;
    }

    if (typeid_cast<const LimitStep *>(step))
        return false;

    if (const auto * distinct = typeid_cast<const DistinctStep *>(step))
    {
        for (const auto & name : distinct->getColumnNames())
            if (name == column_name)
                return true;
        return false;
    }

    if (const auto * expr = typeid_cast<const ExpressionStep *>(step))
    {
        for (const auto & node : expr->getExpression().getNodes())
        {
            if (node.type != ActionsDAG::ActionType::FUNCTION)
                continue;
            for (const auto * child : node.children)
                if (dealiasNode(child)->type == ActionsDAG::ActionType::INPUT && dealiasNode(child)->result_name == column_name)
                    return true;
        }
        return false;
    }

    if (const auto * filter = typeid_cast<const FilterStep *>(step))
    {
        for (const auto & node : filter->getExpression().getNodes())
        {
            if (node.type != ActionsDAG::ActionType::FUNCTION)
                continue;
            for (const auto * child : node.children)
                if (dealiasNode(child)->type == ActionsDAG::ActionType::INPUT && dealiasNode(child)->result_name == column_name)
                    return true;
        }
        return false;
    }

    /// Unknown step type — conservatively assume the column is used.
    return true;
}


/// Build an ActionsDAG that computes the VR functions from grandchild output.
/// When keep_source_columns is true, the source columns are kept in the outputs
/// (used for JoinStepLogical where the join still references them internally).
ActionsDAG buildPushedDAG(
    const SharedHeader & grandchild_header,
    const std::unordered_map<std::string, PushdownCandidate> & cands,
    bool keep_source_columns)
{
    ActionsDAG new_dag;

    std::unordered_map<std::string, const ActionsDAG::Node *> new_dag_inputs;
    for (const auto & col : *grandchild_header)
    {
        const auto & input = new_dag.addInput(col.name, col.type);
        new_dag_inputs[col.name] = &input;
    }

    for (const auto & [source_name, cand] : cands)
    {
        auto it_input = new_dag_inputs.find(source_name);
        chassert(it_input != new_dag_inputs.end());
        const auto * source_in_new_dag = it_input->second;

        for (const auto & func : cand.functions)
        {
            ActionsDAG::NodeRawConstPtrs children;
            for (const auto * orig_child : func.function_node->children)
            {
                const auto * actual = dealiasNode(orig_child);
                if (actual->type == ActionsDAG::ActionType::INPUT)
                {
                    children.push_back(source_in_new_dag);
                }
                else if (actual->type == ActionsDAG::ActionType::COLUMN)
                {
                    const auto & const_node = new_dag.addColumn(
                        {actual->column, actual->result_type, actual->result_name});
                    children.push_back(&const_node);
                }
            }

            const auto & new_func = new_dag.addFunction(
                func.function_node->function_base,
                std::move(children),
                func.output_node->result_name);
            new_dag.addOrReplaceInOutputs(new_func);
        }

        if (!keep_source_columns)
        {
            auto & outputs = new_dag.getOutputs();
            std::erase_if(outputs, [&](const ActionsDAG::Node * node) { return node->result_name == source_name; });
        }
    }

    return new_dag;
}


/// For ExpressionStep / FilterStep children whose DAG passes the source column through,
/// update the DAG: remove the old pass-through and add new pass-throughs for the function results.
void updateChildDAGForPushedColumns(
    IQueryPlanStep * step,
    const std::unordered_map<std::string, PushdownCandidate> & candidates)
{
    ActionsDAG * child_dag = nullptr;
    if (auto * expr = typeid_cast<ExpressionStep *>(step))
        child_dag = &expr->getExpression();
    else if (auto * filter = typeid_cast<FilterStep *>(step))
        child_dag = &filter->getExpression();

    if (!child_dag)
        return;

    for (const auto & [source_name, cand] : candidates)
    {
        /// Remove the source column from outputs.
        auto & outputs = child_dag->getOutputs();
        std::erase_if(outputs, [&](const ActionsDAG::Node * node)
        {
            const auto * actual = dealiasNode(node);
            return actual->type == ActionsDAG::ActionType::INPUT && actual->result_name == source_name;
        });

        /// Add pass-through INPUT nodes for each function result.
        for (const auto & func : cand.functions)
        {
            const auto & new_input = child_dag->addInput(func.output_node->result_name, func.output_node->result_type);
            child_dag->addOrReplaceInOutputs(new_input);
        }
    }

    child_dag->removeUnusedActions();
}


/// Replace each pushed VR function output in the parent DAG with a simple INPUT node.
void updateParentDAG(
    ActionsDAG & parent_dag,
    const std::unordered_map<std::string, PushdownCandidate> & candidates)
{
    for (const auto & [source_name, cand] : candidates)
    {
        for (const auto & func : cand.functions)
        {
            const auto & new_input = parent_dag.addInput(
                func.output_node->result_name,
                func.output_node->result_type);

            auto & outputs = parent_dag.getOutputs();
            for (auto & out : outputs)
            {
                if (out == func.output_node)
                    out = &new_input;
            }
        }
    }
    parent_dag.removeUnusedActions();
}


/// Collect all INPUT column names referenced by the join operator's expression.
std::unordered_set<std::string> collectJoinKeyColumns(const JoinOperator & join_op)
{
    std::unordered_set<std::string> result;

    auto collect_inputs = [&](const auto & self, const ActionsDAG::Node * node) -> void
    {
        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            result.insert(node->result_name);
            return;
        }
        for (const auto * child : node->children)
            self(self, child);
    };

    for (const auto & expr_ref : join_op.expression)
        collect_inputs(collect_inputs, expr_ref.getNode());

    for (const auto & filter_ref : join_op.residual_filter)
        collect_inputs(collect_inputs, filter_ref.getNode());

    return result;
}


/// Determine which join sides allow pushdown based on join kind and strictness.
std::pair<bool, bool> getJoinPushdownSides(const JoinOperator & join_op)
{
    bool push_to_left = true;
    bool push_to_right = true;

    switch (join_op.kind)
    {
        case JoinKind::Full:
        case JoinKind::Paste:
            return {false, false};

        case JoinKind::Left:
            if (join_op.strictness == JoinStrictness::Anti)
                push_to_left = false;
            else
                push_to_right = false;
            break;

        case JoinKind::Right:
            if (join_op.strictness == JoinStrictness::Anti)
                push_to_right = false;
            else
                push_to_left = false;
            break;

        default:
            break;
    }

    if (join_op.strictness == JoinStrictness::Asof)
        push_to_right = false;

    return {push_to_left, push_to_right};
}


/// Check grandchild output for each side's candidates.
/// Returns false if we should bail out entirely (name collision).
bool checkGrandchildOutput(
    std::unordered_map<std::string, PushdownCandidate> & side_candidates,
    const SharedHeader & grandchild_header)
{
    for (auto it = side_candidates.begin(); it != side_candidates.end(); )
    {
        if (!grandchild_header->has(it->first))
        {
            it = side_candidates.erase(it);
            continue;
        }
        bool has_collision = false;
        for (const auto & func : it->second.functions)
        {
            if (grandchild_header->has(func.output_node->result_name))
                has_collision = true;
        }
        if (has_collision)
            return false;
        ++it;
    }
    return true;
}


/// Handle pushdown through a JoinStepLogical child.
size_t handleJoinPushDown(
    QueryPlan::Node * parent_node,
    QueryPlan::Node * child_node,
    QueryPlan::Nodes & nodes,
    ActionsDAG & parent_dag,
    std::unordered_map<std::string, PushdownCandidate> & candidates)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(child_node->step.get());
    chassert(join_step);

    if (child_node->children.size() != 2)
        return 0;

    const auto & join_op = join_step->getJoinOperator();
    auto [push_to_left, push_to_right] = getJoinPushdownSides(join_op);

    /// Disable right-side pushdown for JoinStepLogicalLookup (dictionary join).
    if (typeid_cast<JoinStepLogicalLookup *>(child_node->children.back()->step.get()))
        push_to_right = false;

    if (!push_to_left && !push_to_right)
        return 0;

    /// Disable pushdown on type-changing sides (join_use_nulls).
    auto type_changing = join_step->typeChangingSides();
    if (type_changing.contains(JoinTableSide::Left))
        push_to_left = false;
    if (type_changing.contains(JoinTableSide::Right))
        push_to_right = false;

    if (!push_to_left && !push_to_right)
        return 0;

    auto join_key_columns = collectJoinKeyColumns(join_op);

    const auto & input_headers = child_node->step->getInputHeaders();
    const auto & left_header = input_headers[0];
    const auto & right_header = input_headers[1];

    /// Split candidates per join side.
    std::unordered_map<std::string, PushdownCandidate> left_candidates;
    std::unordered_map<std::string, PushdownCandidate> right_candidates;

    for (auto & [source_name, cand] : candidates)
    {
        if (join_key_columns.contains(source_name))
            continue;

        bool in_left = left_header->has(source_name);
        bool in_right = right_header->has(source_name);

        /// Column present in both sides is ambiguous — exclude.
        if (in_left && in_right)
            continue;

        if (in_left && push_to_left)
            left_candidates[source_name] = cand;
        else if (in_right && push_to_right)
            right_candidates[source_name] = cand;
    }

    if (left_candidates.empty() && right_candidates.empty())
        return 0;

    /// Phase 4b: Grandchild output checks per side.
    const auto & left_grandchild_header = child_node->children[0]->step->getOutputHeader();
    const auto & right_grandchild_header = child_node->children[1]->step->getOutputHeader();

    if (!checkGrandchildOutput(left_candidates, left_grandchild_header))
        return 0;
    if (!checkGrandchildOutput(right_candidates, right_grandchild_header))
        return 0;

    if (left_candidates.empty() && right_candidates.empty())
        return 0;

    /// Phase 5+6: Build DAGs and insert ExpressionSteps per side.
    /// Keep source columns in pushed DAGs for joins (the join's expression_actions DAG
    /// still references them; tryRemoveUnusedColumns will clean up during re-traversal).
    std::unordered_map<std::string, PushdownCandidate> all_pushed;

    if (!left_candidates.empty())
    {
        auto left_dag = buildPushedDAG(left_grandchild_header, left_candidates, /*keep_source_columns=*/ true);

        auto left_step = std::make_unique<ExpressionStep>(left_grandchild_header, std::move(left_dag));
        left_step->setStepDescription("Compute volume-reducing functions");

        auto & inserted_left = nodes.emplace_back();
        inserted_left.step = std::move(left_step);
        inserted_left.children = {child_node->children[0]};
        child_node->children[0] = &inserted_left;

        /// Add pass-through columns to join so VR results appear in the join's output.
        for (const auto & [source_name, cand] : left_candidates)
            for (const auto & func : cand.functions)
                join_step->addPassThroughColumn(func.output_node->result_name, func.output_node->result_type, JoinTableSide::Left);

        child_node->step->updateInputHeader(inserted_left.step->getOutputHeader(), 0);

        all_pushed.insert(left_candidates.begin(), left_candidates.end());
    }

    if (!right_candidates.empty())
    {
        auto right_dag = buildPushedDAG(right_grandchild_header, right_candidates, /*keep_source_columns=*/ true);

        auto right_step = std::make_unique<ExpressionStep>(right_grandchild_header, std::move(right_dag));
        right_step->setStepDescription("Compute volume-reducing functions");

        auto & inserted_right = nodes.emplace_back();
        inserted_right.step = std::move(right_step);
        inserted_right.children = {child_node->children[1]};
        child_node->children[1] = &inserted_right;

        for (const auto & [source_name, cand] : right_candidates)
            for (const auto & func : cand.functions)
                join_step->addPassThroughColumn(func.output_node->result_name, func.output_node->result_type, JoinTableSide::Right);

        child_node->step->updateInputHeader(inserted_right.step->getOutputHeader(), 1);

        all_pushed.insert(right_candidates.begin(), right_candidates.end());
    }

    /// Phase 8: Update parent DAG.
    updateParentDAG(parent_dag, all_pushed);

    /// Phase 9: Update parent input header.
    parent_node->step->updateInputHeader(child_node->step->getOutputHeader());

    return 2;
}


/// Handle pushdown through a single-child step.
size_t handleSingleChildPushDown(
    QueryPlan::Node * parent_node,
    QueryPlan::Node * child_node,
    QueryPlan::Nodes & nodes,
    ActionsDAG & parent_dag,
    std::unordered_map<std::string, PushdownCandidate> & candidates)
{
    auto & child_step = child_node->step;

    if (child_node->children.size() != 1)
        return 0;

    for (auto it = candidates.begin(); it != candidates.end(); )
    {
        if (isColumnUsedByStep(child_step.get(), it->first))
            it = candidates.erase(it);
        else
            ++it;
    }

    if (candidates.empty())
        return 0;

    /// Phase 4b: Grandchild output checks.
    const auto & grandchild_output = child_node->children.front()->step->getOutputHeader();
    if (!checkGrandchildOutput(candidates, grandchild_output))
        return 0;

    if (candidates.empty())
        return 0;

    /// Phase 5: Build pushed DAG.
    auto new_dag = buildPushedDAG(grandchild_output, candidates, /*keep_source_columns=*/ false);

    /// Phase 6: Insert the new ExpressionStep between child and grandchild.
    auto new_step = std::make_unique<ExpressionStep>(grandchild_output, std::move(new_dag));
    new_step->setStepDescription("Compute volume-reducing functions");

    auto & inserted_node = nodes.emplace_back();
    inserted_node.step = std::move(new_step);
    inserted_node.children.swap(child_node->children);
    child_node->children = {&inserted_node};

    /// Phase 7: Update child step.
    updateChildDAGForPushedColumns(child_step.get(), candidates);
    child_step->updateInputHeader(inserted_node.step->getOutputHeader());

    /// Phase 8: Update parent DAG.
    updateParentDAG(parent_dag, candidates);

    /// Phase 9: Update parent input header.
    parent_node->step->updateInputHeader(child_step->getOutputHeader());

    return 2;
}


} // anonymous namespace


size_t tryPushDownVolumeReducingFunctions(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
    /// --- Phase 1: Validate parent --------------------------------------------------------

    if (parent_node->children.size() != 1)
        return 0;

    auto * parent_expr = typeid_cast<ExpressionStep *>(parent_node->step.get());
    auto * parent_filter = typeid_cast<FilterStep *>(parent_node->step.get());
    if (!parent_expr && !parent_filter)
        return 0;

    auto & parent_dag = parent_expr ? parent_expr->getExpression() : parent_filter->getExpression();

    if (parent_dag.hasArrayJoin())
        return 0;

    /// --- Phase 2+3: BFS Classification and Candidate Collection --------------------------

    auto candidates = classifyAndCollectCandidates(parent_dag);

    if (candidates.empty())
        return 0;

    /// --- Phase 4+: Child step compatibility and pushdown ---------------------------------

    auto * child_node = parent_node->children.front();
    auto * join_step = typeid_cast<JoinStepLogical *>(child_node->step.get());

    if (join_step)
        return handleJoinPushDown(parent_node, child_node, nodes, parent_dag, candidates);
    else
        return handleSingleChildPushDown(parent_node, child_node, nodes, parent_dag, candidates);
}

}
