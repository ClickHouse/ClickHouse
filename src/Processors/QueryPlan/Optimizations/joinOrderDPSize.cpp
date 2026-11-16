#include <Processors/QueryPlan/Optimizations/joinOrderAlgorithms.h>
#include <Processors/QueryPlan/Optimizations/joinOrderBitSet.h>
#include <Processors/QueryPlan/Optimizations/joinOrderDP.h>

#include <Interpreters/ProcessList.h>

#include <utility>
#include <vector>

namespace DB
{

namespace
{

class DPSizeJoinOrderOptimizer
{
public:
    DPSizeJoinOrderOptimizer(
        QueryGraph & query_graph_,
        UInt64 max_searched_plans_,
        QueryStatusPtr query_status_,
        std::function<bool()> interactive_cancel_callback_)
        : query_graph(query_graph_)
        , max_searched_plans(max_searched_plans_)
        , query_status(std::move(query_status_))
        , interactive_cancel_callback(std::move(interactive_cancel_callback_))
    {
    }

    DPJoinEntryPtr solve();

private:
    /// Periodically called from potentially long running optimization to check time limits and send progress
    void checkLimits();

    QueryGraph & query_graph;
    PlanMemo dp_table;
    SelectivityCache expression_selectivity;
    size_t searched_plans = 0;
    const UInt64 max_searched_plans;
    LoggerPtr log = DB::getJoinOrderOptimizerLogger();
    QueryStatusPtr query_status;
    std::function<bool()> interactive_cancel_callback;
};

void DPSizeJoinOrderOptimizer::checkLimits()
{
    if (query_status)
        query_status->checkTimeLimit();
    if (interactive_cancel_callback)
        interactive_cancel_callback();
}

DPJoinEntryPtr DPSizeJoinOrderOptimizer::solve()
{
    const size_t total_relations_count = query_graph.relation_stats.size();

    /// Components by size (index 0 is not used that why the size is N+1)
    std::vector<std::unordered_map<BitSet, DPJoinEntryPtr>> components(total_relations_count + 1);

    /// Populate DP table for components of size=1.
    /// Also reset the per-edge selectivity cache so an earlier algorithm in the
    /// fallback chain cannot leak cached `1.0` defaults from a partial `dp_table`.
    dp_table.clear();
    expression_selectivity.clear();
    searched_plans = 0;
    for (size_t i = 0; i < total_relations_count; ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        auto entry = std::make_shared<DPJoinEntry>(i, rel.estimated_rows, rel.column_stats);
        components[1][entry->relations] = entry;
        dp_table[entry->relations] = entry;
    }

    /// Iteratively build components of size from 2 to N
    for (size_t component_size = 2; component_size <= total_relations_count; ++component_size)
    {
        for (size_t smaller_component_size = 1; smaller_component_size <= component_size / 2; ++smaller_component_size)
        {
            const size_t bigger_component_size = component_size - smaller_component_size;

            for (const auto & [_, right] : components[smaller_component_size])
            {
                for (const auto & [_, left] : components[bigger_component_size])
                {
                    /// Do components overlap?
                    if (left->relations & right->relations)
                        continue;

                    /// If both components are of the same size then check each pair just once, not twice
                    if (smaller_component_size == bigger_component_size && *left->relations.begin() > *right->relations.begin())
                        continue;

                    ++searched_plans;
                    if (max_searched_plans && searched_plans > max_searched_plans)
                    {
                        LOG_TRACE(log, "Exceeded the limit of {} searched plans, falling back", max_searched_plans);
                        return nullptr;
                    }
                    /// `checkLimits` invokes the interactive cancel callback, which can send progress over
                    /// the network. Poll it once every few thousand pairs instead of on every one.
                    if ((searched_plans & 0xFFF) == 0)
                        checkLimits();

                    auto join_kind = isValidJoinOrder(query_graph, left->relations, right->relations);
                    if (!join_kind)
                        continue;

                    /// FIXME: Restrict to Inner joins for now because isValidJoinOrder seems to not handle non-Inner joins with swapped inputs correctly
                    if (*join_kind != JoinKind::Inner)
                        continue;

                    auto applicable_edge = getApplicableExpressions(query_graph, left->relations, right->relations);
                    /// Keep the edges that connect left and right, plus non-connecting single-table filters
                    /// and constants, which DPsize attaches at the join that introduces their relation
                    /// (unlike DPhyp, which handles them separately via the hyperedge graph).
                    std::vector<JoinActionRef *> edge;
                    for (auto & edge_it : applicable_edge)
                    {
                        if (connects(edge_it, left->relations, right->relations))
                        {
                            LOG_TEST(log, "Adding predicate connecting {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                            edge.push_back(edge_it);
                        }
                        else
                        {
                            /// Non-connecting predicate. A single-table filter (references exactly one
                            /// relation) or a constant (references none) must still be applied; a predicate
                            /// spanning two or more relations was already applied in a sub-join and is skipped.
                            ///
                            /// Attach a single-table filter at the join that introduces its relation (the
                            /// side equal to that relation) so it filters as low as possible, and a constant
                            /// at the earliest (component_size == 2) join. Two earlier conditions each
                            /// silently dropped the predicate, changing the query result:
                            ///   - `component_size == 2` drops the filter whenever its relation is introduced
                            ///     against an already-multi-relation component (that step has size > 2).
                            ///   - `fromLeft() || fromRight() || fromNone()` drops it for any single-table
                            ///     filter on a relation whose id is >= 2, because `fromLeft`/`fromRight` test
                            ///     relation ids 0 and 1 specifically (they describe the two inputs of a binary
                            ///     join step, not "references a single relation").
                            auto filter_rel = edge_it->getSourceRelations().getSingleBit();
                            bool relation_introduced = filter_rel.has_value()
                                && (left->relations.getSingleBit() == filter_rel || right->relations.getSingleBit() == filter_rel);
                            bool constant_at_earliest_join = edge_it->fromNone() && component_size == 2;
                            if (relation_introduced || constant_at_earliest_join)
                            {
                                LOG_TEST(log, "Adding non-connecting predicate for {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                                edge.push_back(edge_it);
                            }
                            else
                            {
                                LOG_TEST(log, "Skipping non-connecting predicate for {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                            }
                        }
                    }

                    bool connected = !edge.empty()
                        || query_graph.areTransitivelyConnected(left->relations, right->relations);

                    LOG_TEST(log, "Considering join between {} and {}, predicates count: {}, connected: {}",
                        left->dump(), right->dump(), edge.size(), connected);

                    if (!connected)
                        continue;

                    auto new_entry = evaluateJoin(query_graph, dp_table, expression_selectivity, left, right, *join_kind, edge, log);
                    if (new_entry)
                        components[component_size][new_entry->relations] = new_entry;
                }
            }
        }
    }

    auto best_full_plan = dp_table.find(BitSet::allSet(total_relations_count));
    if (best_full_plan != dp_table.end())
        return best_full_plan->second;

    LOG_TRACE(log, "Failed to find best plan using DPsize algorithm");
    return nullptr;
}

}

DPJoinEntryPtr solveDPSizeJoinOrder(
    QueryGraph & query_graph,
    UInt64 max_searched_plans,
    QueryStatusPtr query_status,
    std::function<bool()> interactive_cancel_callback)
{
    return DPSizeJoinOrderOptimizer(
        query_graph,
        max_searched_plans,
        std::move(query_status),
        std::move(interactive_cancel_callback)).solve();
}

}
