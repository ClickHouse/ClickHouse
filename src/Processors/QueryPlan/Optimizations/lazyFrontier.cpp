#include <Processors/QueryPlan/Optimizations/lazyFrontier.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <DataTypes/DataTypeFunction.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB::QueryPlanOptimizations
{

Placement LazyFrontier::at(const ActionsDAG::Node * node) const
{
    if (const auto it = placement.find(node); it != placement.end())
        return it->second;

    return {};
}

bool LazyFrontier::defersAnything() const
{
    return std::ranges::any_of(placement, [](const auto & entry)
    {
        const auto above = entry.second.above;
        return above == Placement::Above::LazyRead || above == Placement::Above::Recomputed;
    });
}

namespace
{

/// A value that does not answer the same twice cannot be recomputed above the `LIMIT`: the filter below
/// already used one answer, and the result has to agree with it. Same for a function whose answer depends
/// on the rows around it, which above the `LIMIT` are no longer the same rows.
///
/// A higher-order function answers as its lambdas do, so it is recomputed only with them. A lambda is as deterministic as
/// its body, which its capture function says, or, for a lambda that captures nothing, the function of its constant.
bool canBeRecomputed(const ActionsDAG::Node * node)
{
    if (node->type == ActionsDAG::ActionType::ARRAY_JOIN)
        return false;

    if (node->type == ActionsDAG::ActionType::COLUMN)
    {
        if (!node->is_deterministic_constant)
            return false;

        if (!WhichDataType(node->result_type).isFunction() || !node->column)
            return true;

        const auto full_column = node->column->convertToFullColumnIfConst();
        const auto * column_function = typeid_cast<const ColumnFunction *>(full_column.get());
        if (!column_function)
            return true;

        const auto & function = column_function->getFunction();
        return function->isDeterministicInScopeOfQuery() && !function->isStateful();
    }

    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return true;

    if (!node->function_base->isDeterministicInScopeOfQuery() || node->function_base->isStateful())
        return false;

    for (const auto * child : node->children)
        if (WhichDataType(child->result_type).isFunction() && !canBeRecomputed(child))
            return false;

    return true;
}

/// Which source an input belongs to, or nullopt for a node that is not an input.
std::optional<size_t> findSourceOfInput(const MergedPlanDAG & merged, const ActionsDAG::Node * node)
{
    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};

    const auto & node_sources = merged.getSources(node);
    if (node_sources.count() != 1)
        return {};

    return *node_sources.begin();
}

/// Whether evaluating the node on rows it was never meant to see is harmless: the rows a join stuffed hold the
/// defaults, so the function must not throw on any value. Being cheap is not enough: a comparison of decimals of
/// different scales is not worth evaluating lazily, but rescaling can overflow.
bool canEvaluateOnStuffedRows(const ActionsDAG::Node * node)
{
    DataTypesWithConstInfo arguments;
    arguments.reserve(node->children.size());
    for (const auto * child : node->children)
        arguments.push_back({child->result_type, child->column && isColumnConst(*child->column)});

    /// Only a function implemented by `IFunction` says whether it can throw; any other one is assumed to.
    const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node->function_base.get());
    return adaptor && !adaptor->getFunction()->canThrow(arguments);
}

class FrontierChooser
{
public:
    FrontierChooser(
        const MergedPlanDAG & merged_,
        const std::vector<bool> & lazy_sources_,
        const std::unordered_set<const MergedPlanDAG::Stuffing *> & masked_stuffings_)
        : merged(merged_), lazy_sources(lazy_sources_), masked_stuffings(masked_stuffings_)
    {
    }

    /// Everything a filter, a join condition or the sort order needs is computed below the `LIMIT`,
    /// together with everything those values are computed from.
    void markComputedBelow(const ActionsDAG::NodeRawConstPtrs & roots)
    {
        /// Stopping at what is marked already is what keeps this linear over the whole run: a value is
        /// marked once, and only a value being marked has its own children looked at.
        const auto is_marked = [this](const ActionsDAG::Node * node) { return frontier.at(node).computed_below; };

        for (const auto * node : findReachableNodes(roots, is_marked))
            frontier.placement[node].computed_below = true;
    }

    /// Values the main branch has to hand over anyway, the sort keys above all: they are part of the
    /// block the `LIMIT` produces whatever this decides, so taking them costs nothing.
    void markAlreadyCrossing(const ActionsDAG::NodeRawConstPtrs & nodes) { already_crossing.insert(nodes.begin(), nodes.end()); }

    /// Places `node` and everything it reads. Every value can be placed: what cannot be had above the
    /// `LIMIT` is computed below it and crosses, which is what the plan did anyway.
    void place(const ActionsDAG::Node * root)
    {
        struct Frame
        {
            const ActionsDAG::Node * node = nullptr;
            size_t next_child = 0;
        };

        /// Only a value to be recomputed needs its children placed first, so those are the only frames
        /// that stay on the stack. An explicit one at that: expression DAGs get deep.
        std::vector<Frame> stack{{root}};
        while (!stack.empty())
        {
            auto & frame = stack.back();
            const auto * node = frame.node;

            if (frame.next_child == 0)
            {
                /// Another value that reads it may have placed it already.
                if (frontier.at(node).above != Placement::Above::No)
                {
                    stack.pop_back();
                    continue;
                }

                if (const auto decision = decide(node); decision != Decision::Recompute)
                {
                    if (decision == Decision::LazyRead)
                        frontier.placement[node].above = Placement::Above::LazyRead;
                    else
                        cross(node);

                    stack.pop_back();
                    continue;
                }
            }

            if (frame.next_child < node->children.size())
            {
                const auto * child = node->children[frame.next_child];
                ++frame.next_child;
                stack.push_back({child});
                continue;
            }

            frontier.placement[node].above = Placement::Above::Recomputed;
            stack.pop_back();
        }
    }

    LazyFrontier takeFrontier() { return std::move(frontier); }

private:
    enum class Decision : uint8_t
    {
        Cross,      /// the main branch computes it and hands it over
        LazyRead,   /// a second read of its source returns it
        Recompute,  /// computed again above the `LIMIT`, once what it reads is placed
    };

    Decision decide(const ActionsDAG::Node * node) const
    {
        const auto source = findSourceOfInput(merged, node);

        /// Without a second read of its source there is no way to get a column above the `LIMIT`. An
        /// input reading no source, or more than one, is not something this can place either, and an
        /// `arrayJoin` changes the number of rows the `LIMIT` already counted.
        const bool can_be_had_above = source
            ? lazy_sources[*source]
            : node->type != ActionsDAG::ActionType::INPUT && node->type != ActionsDAG::ActionType::ARRAY_JOIN;

        if (!can_be_had_above)
            return Decision::Cross;

        /// A lambda is not a column a step can hand over. It is placed only for the higher-order function that reads it,
        /// which is recomputed only when the lambda can be as well, see `canBeRecomputed`.
        if (WhichDataType(node->result_type).isFunction())
            return Decision::Recompute;

        /// How to have it above, once the rest of this settles whether to: a source column is read a
        /// second time, anything else is computed a second time.
        const auto have_it_above = source ? Decision::LazyRead : Decision::Recompute;

        /// A function or a constant a join can stuff is recomputed above the `LIMIT` under that join's mask, so on
        /// every row, the stuffed ones included. That needs a mask, and for a function one harmless to evaluate
        /// there. A source column read lazily needs neither, as the lazy read stands the default at a row nothing
        /// matched, and nor does an alias, which is whatever its child is, stuffed rows included.
        if (node->type == ActionsDAG::ActionType::FUNCTION || node->type == ActionsDAG::ActionType::COLUMN)
        {
            if (const auto * stuffing = merged.getNearestStuffing(node))
                if (!masked_stuffings.contains(stuffing)
                    || (node->type == ActionsDAG::ActionType::FUNCTION && !canEvaluateOnStuffedRows(node)))
                    return Decision::Cross;
        }

        /// A value that does not answer the same twice is computed where the plan computes it, and crosses as a column.
        /// Where something below the `LIMIT` uses it, the result has to agree with that; where nothing does, it is still
        /// computed for the rows there - one value per row a join then repeats, a number that counts the rows before the
        /// `LIMIT`. What is computed from it may still be recomputed above, since that gives the same answer.
        if (!canBeRecomputed(node))
            return Decision::Cross;

        /// Nothing below the `LIMIT` computes it, so there is nothing to hand over or to agree with.
        if (!frontier.at(node).computed_below)
            return have_it_above;

        /// The main branch hands this one over regardless, so there is nothing to weigh up.
        if (already_crossing.contains(node))
            return Decision::Cross;

        return preferRecomputing(node) ? have_it_above : Decision::Cross;
    }

    /// Whether to compute a value the main branch already computes a second time above the `LIMIT`,
    /// rather than have the main branch hand it over.
    ///
    /// A crossing column is replicated by every join above it, and a hash join copies it into its build
    /// side, over every row that reaches there; recomputing touches at most `limit` rows. Where a join
    /// sits above the value that trade is worth taking, and where none does the value is reused, since
    /// carrying it past a sort costs little and reading its inputs again costs something.
    ///
    /// Which side of a join becomes the build side is not known here - the joins are still logical, and
    /// `convertLogicalJoinToPhysical` and the `join_swap_table` swap both come later - so the answer
    /// cannot be sharpened by asking.
    ///
    /// What this does not weigh is how expensive the expression is. Recomputing a regular expression
    /// match over `limit` rows can cost more than carrying a column, and `limit` reaches
    /// `query_plan_max_limit_for_lazy_materialization`, so this is the place where a per-function
    /// estimate belongs once there is one to ask.
    bool preferRecomputing(const ActionsDAG::Node * node) const { return merged.hasJoinAbove(node); }

    /// A crossing value is computed below the `LIMIT`, so everything it reads is computed there too.
    void cross(const ActionsDAG::Node * node)
    {
        markComputedBelow({node});
        frontier.placement[node].above = Placement::Above::Crossing;
    }

    const MergedPlanDAG & merged;
    const std::vector<bool> & lazy_sources;
    const std::unordered_set<const MergedPlanDAG::Stuffing *> & masked_stuffings;
    NodeSet already_crossing;
    LazyFrontier frontier;
};

}

LazyFrontier chooseLazyFrontier(
    const MergedPlanDAG & merged,
    const std::vector<size_t> & eager_output_positions,
    const std::vector<bool> & lazy_sources,
    const std::unordered_set<const MergedPlanDAG::Stuffing *> & masked_stuffings)
{
    chassert(lazy_sources.size() == merged.sources.size());

    const auto & outputs = merged.getOutputs();

    FrontierChooser chooser(merged, lazy_sources, masked_stuffings);

    ActionsDAG::NodeRawConstPtrs computed_below_roots = merged.filter_nodes;
    computed_below_roots.append_range(merged.join_condition_nodes);
    computed_below_roots.append_range(merged.step_read_nodes);

    ActionsDAG::NodeRawConstPtrs already_crossing;
    for (size_t position : eager_output_positions)
    {
        computed_below_roots.push_back(outputs[position]);
        already_crossing.push_back(outputs[position]);
    }

    chooser.markComputedBelow(computed_below_roots);
    chooser.markAlreadyCrossing(already_crossing);

    for (const auto * output : outputs)
        chooser.place(output);

    return chooser.takeFrontier();
}

std::vector<NodeSet> collectLazyReads(const MergedPlanDAG & merged, const LazyFrontier & frontier)
{
    std::vector<NodeSet> reads(merged.sources.size());

    for (const auto & [node, placed] : frontier.placement)
    {
        if (placed.above != Placement::Above::LazyRead)
            continue;

        const auto & node_sources = merged.getSources(node);
        chassert(node_sources.count() == 1);
        reads[*node_sources.begin()].insert(node);
    }

    return reads;
}

}
