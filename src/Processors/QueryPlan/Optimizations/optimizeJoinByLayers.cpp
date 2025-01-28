#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/TableJoin.h>

namespace DB
{
namespace QueryPlanOptimizations
{

ReadFromMergeTree * findReadingStep(const QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (reading->isQueryWithFinal())
            return nullptr;
        if (reading->readsInOrder())
            return nullptr;

        return reading;
    }

    return nullptr;
}

ActionsDAG makeSourceDAG(ReadFromMergeTree & source)
{
    if (const auto & prewhere_info = source.getPrewhereInfo())
        return prewhere_info->prewhere_actions.clone();

    return ActionsDAG(source.getOutputHeader().getColumnsWithTypeAndName());
}

/// This function builds a common DAG which is a merge of DAGs from Filter and Expression steps chain.
bool updateDAG(const QueryPlan::Node & node, ActionsDAG & dag)
{
    if (node.children.size() != 1)
        return false;

    IQueryPlanStep * step = node.step.get();

    if (typeid_cast<DistinctStep *>(step))
        return true;

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
    {
        dag.mergeInplace(expression->getExpression().clone());
        return true;
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        dag.mergeInplace(filter->getExpression().clone());
        return true;
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(step))
    {
        const auto & array_joined_columns = array_join->getColumns();

        std::unordered_set<std::string_view> keys_set(array_joined_columns.begin(), array_joined_columns.end());

        /// Remove array joined columns from outputs.
        /// Types are changed after ARRAY JOIN, and we can't use this columns anyway.
        ActionsDAG::NodeRawConstPtrs outputs;
        outputs.reserve(dag.getOutputs().size());

        for (const auto & output : dag.getOutputs())
        {
            if (!keys_set.contains(output->result_name))
                outputs.push_back(output);
        }

        dag.getOutputs() = std::move(outputs);
        return true;
    }

    return false;
}

size_t findCommonPromaryKeyPrefixByJoinKey(
    ReadFromMergeTree * lhs_reading, const ActionsDAG & lhs_dag,
    ReadFromMergeTree * rhs_reading, const ActionsDAG & rhs_dag,
    const TableJoin::JoinOnClause & clause)
{
    // std::cerr << "optimizeJoinByLayers 4\n";

    const auto & lhs_pk = lhs_reading->getStorageMetadata()->getPrimaryKey();
    if (lhs_pk.column_names.empty())
        return 0;

    const auto & rhs_pk = rhs_reading->getStorageMetadata()->getPrimaryKey();
    if (rhs_pk.column_names.empty())
        return 0;

    std::unordered_map<std::string_view, const ActionsDAG::Node *> lhs_outputs;
    std::unordered_map<std::string_view, const ActionsDAG::Node *> rhs_outputs;

    for (const auto & output : lhs_dag.getOutputs())
        lhs_outputs.emplace(output->result_name, output);
    for (const auto & output : rhs_dag.getOutputs())
        rhs_outputs.emplace(output->result_name, output);

    const auto & lhs_pk_dag = lhs_pk.expression->getActionsDAG();
    auto lhs_matches = matchTrees(lhs_pk_dag.getOutputs(), lhs_dag, false);
    const auto & rhs_pk_dag = rhs_pk.expression->getActionsDAG();
    auto rhs_matches = matchTrees(rhs_pk_dag.getOutputs(), rhs_dag, false);

    size_t useful_pk_columns = 0;

    for (size_t pos = 0; pos < lhs_pk_dag.getOutputs().size() && pos < rhs_pk_dag.getOutputs().size(); ++pos)
    {
        size_t keys_size = clause.key_names_left.size();

        for (size_t i = 0; i < keys_size && useful_pk_columns <= pos; ++i)
        {
            const auto & left_name = clause.key_names_left[i];
            const auto & right_name = clause.key_names_right[i];

            // std::cerr << left_name << ' ' << right_name << std::endl;

            auto it = lhs_outputs.find(left_name);
            auto jt = rhs_outputs.find(right_name);
            if (it == lhs_outputs.end() || jt == rhs_outputs.end())
                continue;

            auto lhs_match = lhs_matches.find(it->second);
            auto rhs_match = rhs_matches.find(jt->second);
            if (lhs_match == lhs_matches.end() || rhs_match == rhs_matches.end())
                continue;

            if (lhs_match->second.monotonicity || rhs_match->second.monotonicity)
                continue;

            if (lhs_match->second.node == lhs_pk_dag.getOutputs()[pos] && rhs_match->second.node == rhs_pk_dag.getOutputs()[pos])
                ++useful_pk_columns;
        }

        if (useful_pk_columns < pos)
            break;
    }

    return useful_pk_columns;
}

struct JoinsAndSourcesWithCommonPrimaryKeyPrefix
{
    std::list<JoinStep *> joins;
    std::list<ReadFromMergeTree *> sources;
    size_t common_prefix = std::numeric_limits<size_t>::max();
};

static void apply(struct JoinsAndSourcesWithCommonPrimaryKeyPrefix & data)
{
    // std::cerr << "... apply for prefix " << data.common_prefix << " and joins " << data.joins.size() << std::endl;

    if (data.common_prefix == 0 || data.joins.size() == 0)
        return;

    RangesInDataParts all_parts;
    std::vector<ReadFromMergeTree::AnalysisResultPtr> analysis_results;
    for (auto & source : data.sources)
    {
        auto analysis_result = source->getAnalyzedResult();
        if (!analysis_result)
        {
            analysis_result = source->selectRangesToRead();
            source->setAnalyzedResult(analysis_result);
        }

        size_t added_parts = all_parts.size();
        for (const auto & part : analysis_result->parts_with_ranges)
        {
            all_parts.push_back(part);
            all_parts.back().part_index_in_query += added_parts;
        }

        analysis_results.push_back(std::move(analysis_result));
    }

    auto logger = getLogger("optimizeJoinByLayers");
    auto all_split = splitIntersectingPartsRangesIntoLayers(all_parts, data.sources.front()->getNumStreams(), data.common_prefix, logger);
    std::vector<SplitPartsByRanges> splits(analysis_results.size());
    splits[0].borders = std::move(all_split.borders);
    for (size_t i = 1; i < splits.size(); ++i)
        splits[i].borders = splits[0].borders;

    for (auto & layer : all_split.layers)
    {
        std::sort(layer.begin(), layer.end(),
            [](const RangesInDataPart & lhs, const RangesInDataPart & rhs)
            { return lhs.part_index_in_query < rhs.part_index_in_query; });

        size_t next_part = 0;
        size_t sum_parts = 0;
        for (size_t i = 0; i < splits.size(); ++i)
        {
            auto & new_layer = splits[i].layers.emplace_back();
            size_t num_parts_in_source = analysis_results[i]->parts_with_ranges.size();
            while (next_part < layer.size() && layer[next_part].part_index_in_query < sum_parts + num_parts_in_source)
            {
                auto & new_part_range = new_layer.emplace_back(layer[next_part]);
                new_part_range.part_index_in_query -= sum_parts;
                ++next_part;
            }
            sum_parts += num_parts_in_source;
        }
    }

    for (size_t i = 0; i < splits.size(); ++i)
        analysis_results[i]->split_parts = std::move(splits[i]);

    for (const auto & join_step : data.joins)
        join_step->enableJoinByLayers(data.common_prefix);
}

void optimizeJoinByLayers(QueryPlan::Node & root)
{
    struct Result
    {
        JoinsAndSourcesWithCommonPrimaryKeyPrefix joins;
        ActionsDAG dag; /// For the leftmost source
    };

    struct Frame
    {
        const QueryPlan::Node * node;
        size_t next_child_to_process = 0;
        std::vector<std::optional<Result>> results{};
    };

    std::optional<Result> result;
    std::stack<Frame> stack;
    stack.push({&root});

    while (!stack.empty())
    {
        auto & frame = stack.top();
        if (frame.next_child_to_process > 0)
            frame.results.push_back(std::move(result));

        if (frame.next_child_to_process < frame.node->children.size())
        {
            stack.push({frame.node->children[frame.next_child_to_process]});
            ++frame.next_child_to_process;
            continue;
        }

        result = {};

        if (auto * join_step = typeid_cast<JoinStep *>(frame.node->step.get()))
        {
            const auto & join = join_step->getJoin();

            // std::cerr << "Processing Join\n";
            // WriteBufferFromOwnString out;
            // IQueryPlanStep::FormatSettings settings{out};
            // join_step->describeActions(settings);
            // std::cerr << out.stringView() << std::endl;

            auto * hash_join = typeid_cast<HashJoin *>(join.get());
            auto * concurrent_hash_join = typeid_cast<ConcurrentHashJoin *>(join.get());
            bool is_algo_supported = hash_join || concurrent_hash_join;

            bool can_split_left_table = frame.results.front() != std::nullopt && is_algo_supported && !join->hasDelayedBlocks();
            // std::cerr << "can_split_left_table " << can_split_left_table << std::endl;

            const auto & table_join = join->getTableJoin();
            auto kind = table_join.kind();
            auto strictness = table_join.strictness();
            const auto & clauses = table_join.getClauses();

            bool can_split_join = frame.results.back() != std::nullopt && can_split_left_table
                && (isLeft(kind) || isRight(kind) || isInner(kind) || isFull(kind))
                && strictness != JoinStrictness::Asof
                && clauses.size() == 1;

            // std::cerr << "can_split_join " << can_split_join << std::endl;

            size_t common_prefix = 0;
            if (can_split_join)
            {
                // std::cerr << frame.results.front()->dag.dumpDAG() << std::endl;
                // std::cerr << frame.results.back()->dag.dumpDAG() << std::endl;

                common_prefix = findCommonPromaryKeyPrefixByJoinKey(
                    frame.results.front()->joins.sources.front(), frame.results.front()->dag,
                    frame.results.back()->joins.sources.front(), frame.results.back()->dag,
                    clauses[0]);
            }

            // std::cerr << "common_prefix " << common_prefix << std::endl;

            if (common_prefix > 0)
            {
                result = std::move(frame.results.front());
                result->joins.joins.emplace_back(join_step);
                result->joins.joins.splice(result->joins.joins.end(), std::move(frame.results.back()->joins.joins));
                result->joins.sources.splice(result->joins.sources.end(), std::move(frame.results.back()->joins.sources));

                /// Here we choose the minimal common prefix.
                /// Applying optimization to more joins is potentially better.
                /// Hopefully, even the first PK column would be enough to shard the data.
                result->joins.common_prefix = std::min(result->joins.common_prefix, common_prefix);
                result->joins.common_prefix = std::min(result->joins.common_prefix, frame.results.back()->joins.common_prefix);

                frame.results.back() = std::nullopt;
            }
            else if (can_split_left_table)
            {
                /// TODO : check if any type conversion is needed for join_use_nulls.
                result = std::move(frame.results.front());
            }
        }
        else if (auto * creating_sets = typeid_cast<DelayedCreatingSetsStep *>(frame.node->step.get()))
        {
            result = std::move(frame.results.front());
        }
        else if (auto * source = findReadingStep(*frame.node))
        {
            result.emplace();
            result->joins.sources.emplace_back(source);
            result->dag = makeSourceDAG(*source);
        }
        else if (frame.results.size() == 1 && frame.results[0])
        {
            if (updateDAG(*frame.node, frame.results[0]->dag))
                result = std::move(frame.results[0]);
        }

        for (auto & cur_result : frame.results)
            if (cur_result)
                apply(cur_result->joins);

        stack.pop();
    }

    if (result)
        apply(result->joins);
}

}
}
