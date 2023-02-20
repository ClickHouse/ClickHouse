#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsLogical.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/logger_useful.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Parsers/queryToString.h>
#include <stack>

namespace DB::QueryPlanOptimizations
{

static QueryPlan::Node * findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        return &node;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return findReadingStep(*node.children.front());

    return nullptr;
}

static void appendExpression(ActionsDAGPtr & dag, const ActionsDAGPtr & expression)
{
    if (dag)
        dag->mergeInplace(std::move(*expression->clone()));
    else
        dag = expression->clone();
}


/// This function builds a common DAG which is a gerge of DAGs from Filter and Expression steps chain.
static bool buildAggregatingDAG(QueryPlan::Node & node, ActionsDAGPtr & dag, ActionsDAG::NodeRawConstPtrs & filter_nodes, bool & need_remove_column)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        //std::cerr << "============ Found ReadFromMergeTreen";
        if (const auto * prewhere_info = reading->getPrewhereInfo())
        {
            //std::cerr << "============ Found prewhere info\n";
            if (prewhere_info->row_level_filter)
            {
                appendExpression(dag, prewhere_info->row_level_filter);
                if (const auto * filter_node = dag->tryFindInOutputs(prewhere_info->row_level_column_name))
                    filter_nodes.push_back(filter_node);
                else
                    return false;
            }

            if (prewhere_info->prewhere_actions)
            {
                //std::cerr << "============ Found prewhere actions\n";
                appendExpression(dag, prewhere_info->prewhere_actions);
                //std::cerr << "============ Cur dag \n" << dag->dumpDAG();
                need_remove_column = prewhere_info->remove_prewhere_column;
                if (const auto * filter_node = dag->tryFindInOutputs(prewhere_info->prewhere_column_name))
                    filter_nodes.push_back(filter_node);
                else
                    return false;
            }
        }
        return true;
    }

    if (node.children.size() != 1)
        return false;

    if (!buildAggregatingDAG(*node.children.front(), dag, filter_nodes, need_remove_column))
        return false;

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
    {
        const auto & actions = expression->getExpression();
        if (actions->hasArrayJoin())
            return false;

        appendExpression(dag, actions);
        //std::cerr << "============ Cur e dag \n" << dag->dumpDAG();
        need_remove_column = false;
        return true;
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        const auto & actions = filter->getExpression();
        if (actions->hasArrayJoin())
            return false;

        appendExpression(dag, actions);
        //std::cerr << "============ Cur f dag \n" << dag->dumpDAG();
        need_remove_column = filter->removesFilterColumn();
        const auto * filter_expression = dag->tryFindInOutputs(filter->getFilterColumnName());
        if (!filter_expression)
            return false;

        filter_nodes.push_back(filter_expression);
        return true;
    }

    return false;
}

struct AggregateProjectionInfo
{
    ActionsDAGPtr before_aggregation;
    NamesAndTypesList keys;
    AggregateDescriptions aggregates;

    /// A context copy from interpreter which was used for analysis.
    /// Just in case it is used by some function.
    ContextPtr context;
};

AggregateProjectionInfo getAggregatingProjectionInfo(
    const ProjectionDescription & projection,
    const ContextPtr & context,
    const StorageMetadataPtr & metadata_snapshot,
    const Block & key_virtual_columns)
{
    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Proj query : {}", queryToString(projection.query_ast));
    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Sample for keys : {}", projection.sample_block_for_keys.dumpStructure());
    /// This is a bad approach.
    /// We'd better have a separate interpreter for projections.
    /// Now it's not obvious we didn't miss anything here.
    InterpreterSelectQuery interpreter(
        projection.query_ast,
        context,
        Pipe(std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlock())),
        SelectQueryOptions{QueryProcessingStage::WithMergeableState});

    const auto & analysis_result = interpreter.getAnalysisResult();
    const auto & query_analyzer = interpreter.getQueryAnalyzer();

    AggregateProjectionInfo info;
    info.context = interpreter.getContext();
    info.before_aggregation = analysis_result.before_aggregation;
    info.keys = query_analyzer->aggregationKeys();
    info.aggregates = query_analyzer->aggregates();

    for (const auto & virt_column : key_virtual_columns)
    {
        const auto * input = &info.before_aggregation->addInput(virt_column);
        info.before_aggregation->getOutputs().push_back(input);
        info.keys.push_back(NameAndTypePair{virt_column.name, virt_column.type});
    }

    return info;
}

struct AggregateProjectionCandidate
{
    AggregateProjectionInfo info;
    const ProjectionDescription * projection;
    ActionsDAGPtr dag;

    MergeTreeDataSelectAnalysisResultPtr merge_tree_projection_select_result_ptr;
    MergeTreeDataSelectAnalysisResultPtr merge_tree_normal_select_result_ptr;

    size_t sum_marks = 0;
};

struct NormalProjectionCandidate
{
    const ProjectionDescription * projection;

    MergeTreeDataSelectAnalysisResultPtr merge_tree_projection_select_result_ptr;
    MergeTreeDataSelectAnalysisResultPtr merge_tree_normal_select_result_ptr;

    size_t sum_marks = 0;
};

ActionsDAGPtr analyzeAggregateProjection(
    const AggregateProjectionInfo & info,
    ActionsDAG & query_dag,
    const ActionsDAG::Node * filter_node,
    const Names & keys,
    const AggregateDescriptions & aggregates)
{

    ActionsDAG::NodeRawConstPtrs key_nodes;
    std::unordered_set<const ActionsDAG::Node *> aggregate_args;

    std::unordered_map<std::string, const ActionsDAG::Node *> index;
    for (const auto * output : query_dag.getOutputs())
        index.emplace(output->result_name, output);

    std::unordered_map<std::string, const ActionsDAG::Node *> proj_index;
    for (const auto * output : info.before_aggregation->getOutputs())
        proj_index.emplace(output->result_name, output);

    key_nodes.reserve(keys.size() + 1);

    if (filter_node)
        key_nodes.push_back(filter_node);

    for (const auto & key : keys)
    {
        auto it = index.find(key);
        /// This should not happen ideally.
        if (it == index.end())
        {
            LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot find key {} in query DAG", key);
            return {};
        }

        key_nodes.push_back(it->second);
    }

    for (const auto & aggregate : aggregates)
    {
        for (const auto & argument : aggregate.argument_names)
        {
            auto it = index.find(argument);
            /// This should not happen ideally.
            if (it == index.end())
            {
                LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot find arg {} for agg functions {}", argument, aggregate.column_name);
                return {};
            }

            aggregate_args.insert(it->second);
        }
    }

    MatchedTrees::Matches matches = matchTrees(*info.before_aggregation, query_dag);
    for (const auto & [node, match] : matches)
    {
        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Match {} {} -> {} {} (with monotonicity : {})",
            static_cast<const void *>(node), node->result_name,
            static_cast<const void *>(match.node), (match.node ? match.node->result_name : ""), match.monotonicity != std::nullopt);
    }

    std::unordered_map<std::string, std::list<size_t>> projection_aggregate_functions;
    for (size_t i = 0; i < info.aggregates.size(); ++i)
        projection_aggregate_functions[info.aggregates[i].function->getName()].push_back(i);

    std::unordered_set<const ActionsDAG::Node *> split_nodes;

    struct AggFuncMatch
    {
        /// idx in projection
        size_t idx;
        /// nodes in query DAG
        ActionsDAG::NodeRawConstPtrs args;
    };

    std::vector<AggFuncMatch> aggregate_function_matches;
    aggregate_function_matches.reserve(aggregates.size());

    for (const auto & aggregate : aggregates)
    {
        auto it = projection_aggregate_functions.find(aggregate.function->getName());
        if (it == projection_aggregate_functions.end())
        {
            LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot match agg func {} by name {}", aggregate.column_name, aggregate.function->getName());
            return {};
        }
        auto & candidates = it->second;

        std::optional<AggFuncMatch> match;

        for (size_t idx : candidates)
        {
            const auto & candidate = info.aggregates[idx];

            /// Note: this check is a bit strict.
            /// We check that aggregate function names, argument types and parameters are equal.
            /// In some cases it's possible only to check that states are equal,
            /// e.g. for quantile(0.3)(...) and quantile(0.5)(...).
            /// But also functions sum(...) and sumIf(...) will have equal states,
            /// and we can't replace one to another from projection.
            if (!candidate.function->getStateType()->equals(*aggregate.function->getStateType()))
            {
                LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot match agg func {} vs {} by state {} vs {}",
                    aggregate.column_name, candidate.column_name,
                    candidate.function->getStateType()->getName(), aggregate.function->getStateType()->getName());
                continue;
            }

            if (typeid_cast<const AggregateFunctionCount *>(candidate.function.get()))
            {
                bool all_args_not_null = true;
                for (const auto & query_name : aggregate.argument_names)
                {
                    auto jt = index.find(query_name);

                    if (jt == index.end() || jt->second->result_type->isNullable())
                    {
                        all_args_not_null = false;
                        break;
                    }
                }

                for (const auto & proj_name : candidate.argument_names)
                {
                    auto kt = proj_index.find(proj_name);

                    if (kt == proj_index.end() || kt->second->result_type->isNullable())
                    {
                        all_args_not_null = false;
                        break;
                    }
                }

                if (all_args_not_null)
                {
                    /// we can ignore arguments for count()
                    match = AggFuncMatch{idx, {}};
                    break;
                }
            }

            if (aggregate.argument_names.size() != candidate.argument_names.size())
                continue;

            size_t num_args = aggregate.argument_names.size();
            ActionsDAG::NodeRawConstPtrs args;
            args.reserve(num_args);
            for (size_t arg = 0; arg < num_args; ++arg)
            {
                const auto & query_name = aggregate.argument_names[arg];
                const auto & proj_name = candidate.argument_names[arg];

                auto jt = index.find(query_name);
                /// This should not happen ideally.
                if (jt == index.end())
                {
                    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot match agg func {} vs {} : can't find arg {} in query dag",
                        aggregate.column_name, candidate.column_name, query_name);
                    break;
                }

                const auto * query_node = jt->second;

                auto kt = proj_index.find(proj_name);
                /// This should not happen ideally.
                if (kt == proj_index.end())
                {
                    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot match agg func {} vs {} : can't find arg {} in proj dag",
                        aggregate.column_name, candidate.column_name, proj_name);
                    break;
                }

                const auto * proj_node = kt->second;

                auto mt = matches.find(query_node);
                if (mt == matches.end())
                {
                    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot match agg func {} vs {} : can't match arg {} vs {} : no node in map",
                        aggregate.column_name, candidate.column_name, query_name, proj_name);
                    break;
                }

                const auto & node_match = mt->second;
                if (node_match.node != proj_node || node_match.monotonicity)
                {
                    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot match agg func {} vs {} : can't match arg {} vs {} : no match or monotonicity",
                        aggregate.column_name, candidate.column_name, query_name, proj_name);
                    break;
                }

                args.push_back(query_node);
            }

            if (args.size() < aggregate.argument_names.size())
                continue;

            // for (const auto * node : args)
            //     split_nodes.insert(node);

            match = AggFuncMatch{idx, std::move(args)};
            break;
        }

        if (!match)
            return {};

        aggregate_function_matches.emplace_back(std::move(*match));
    }

    std::unordered_set<const ActionsDAG::Node *> proj_key_nodes;
    for (const auto & key : info.keys)
    {
        auto it = proj_index.find(key.name);
        /// This should not happen ideally.
        if (it == proj_index.end())
            break;

        proj_key_nodes.insert(it->second);
    }

    std::unordered_set<const ActionsDAG::Node *> visited;

    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child_to_visit = 0;
    };

    std::stack<Frame> stack;
    for (const auto * key_node : key_nodes)
    {
        if (visited.contains(key_node))
            continue;

        stack.push({.node = key_node});

        while (!stack.empty())
        {
            auto & frame = stack.top();

            if (frame.next_child_to_visit == 0)
            {
                auto jt = matches.find(frame.node);
                if (jt != matches.end())
                {
                    auto & match = jt->second;
                    if (match.node && !match.monotonicity && proj_key_nodes.contains(match.node))
                    {
                        visited.insert(frame.node);
                        split_nodes.insert(frame.node);
                        stack.pop();
                        continue;
                    }
                }
            }

            if (frame.next_child_to_visit < frame.node->children.size())
            {
                stack.push({.node = frame.node->children[frame.next_child_to_visit]});
                ++frame.next_child_to_visit;
                continue;
            }

            /// Not a match and there is no matched child.
            if (frame.node->type == ActionsDAG::ActionType::INPUT)
            {
                LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot find match for {}", frame.node->result_name);
                return {};
            }

            /// Not a match, but all children matched.
            visited.insert(frame.node);
            stack.pop();
        }
    }

    std::unordered_map<const ActionsDAG::Node *, std::string> new_inputs;
    for (const auto * node : split_nodes)
        new_inputs[node] = matches[node].node->result_name;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Folding actions by projection");
    auto proj_dag = query_dag.foldActionsByProjection(new_inputs, key_nodes);
    auto & proj_dag_outputs =  proj_dag->getOutputs();
    for (const auto & aggregate : aggregates)
        proj_dag_outputs.push_back(&proj_dag->addInput(aggregate.column_name, aggregate.function->getResultType()));

    return proj_dag;
}

bool optimizeUseAggProjections(QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    if (node.children.size() != 1)
        return false;

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return false;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try optimize projection 2");
    if (!aggregating->canUseProjection())
        return false;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try optimize projection 3");
    QueryPlan::Node * reading_node = findReadingStep(*node.children.front());
    if (!reading_node)
        return false;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try optimize projection 4");

    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());
    if (!reading)
        return false;

    /// Probably some projection already was applied.
    if (reading->hasAnalyzedResult())
        return false;

    if (reading->isQueryWithFinal())
        return false;

    if (reading->isQueryWithSampling())
        return false;

    if (reading->isParallelReadingEnabled())
        return false;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try optimize projection 5");

    const auto metadata = reading->getStorageMetadata();
    const auto & projections = metadata->projections;

    bool can_use_minmax_projection = metadata->minmax_count_projection && !reading->getMergeTreeData().has_lightweight_delete_parts.load();

    std::vector<const ProjectionDescription *> agg_projections;
    for (const auto & projection : projections)
        if (projection.type == ProjectionDescription::Type::Aggregate)
            agg_projections.push_back(&projection);

    if (!can_use_minmax_projection && agg_projections.empty())
        return false;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Has agg projection");

    ActionsDAGPtr dag;
    bool need_remove_column = false;
    ActionsDAG::NodeRawConstPtrs filter_nodes;
    if (!buildAggregatingDAG(*node.children.front(), dag, filter_nodes, need_remove_column))
        return false;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Query DAG: {}", dag->dumpDAG());

    const ActionsDAG::Node * filter_node = nullptr;
    if (!filter_nodes.empty())
    {
        filter_node = filter_nodes.front();
        if (filter_nodes.size() > 1)
        {
            FunctionOverloadResolverPtr func_builder_and =
                std::make_unique<FunctionToOverloadResolverAdaptor>(
                    std::make_shared<FunctionAnd>());

            filter_node = &dag->addFunction(func_builder_and, std::move(filter_nodes), {});
        }

        dag->getOutputs().push_back(filter_node);
    }

    ContextPtr context = reading->getContext();

    const auto & keys = aggregating->getParams().keys;
    const auto & aggregates = aggregating->getParams().aggregates;
    Block key_virtual_columns = reading->getMergeTreeData().getSampleBlockWithVirtualColumns();

    std::vector<AggregateProjectionCandidate> candidates;
    std::optional<AggregateProjectionCandidate> minmax_projection;
    Block minmax_count_projection_block;
    MergeTreeData::DataPartsVector minmax_projection_normal_parts;

    const auto & parts = reading->getParts();
    const auto & query_info = reading->getQueryInfo();
    auto query_info_copy = query_info;
    query_info_copy.prewhere_info = nullptr;
    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks;

    AggregateProjectionCandidate * best_candidate = nullptr;

    if (can_use_minmax_projection)
    {
        const auto * projection = &*(metadata->minmax_count_projection);
        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try projection {}", projection->name);
        auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection DAG {}", info.before_aggregation->dumpDAG());
        if (auto proj_dag = analyzeAggregateProjection(info, *dag, filter_node, keys, aggregates))
        {
            LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection analyzed DAG {}", proj_dag->dumpDAG());
            minmax_projection.emplace(AggregateProjectionCandidate{
                .info = std::move(info),
                .projection = projection,
                .dag = std::move(proj_dag),
            });

            minmax_count_projection_block = reading->getMergeTreeData().getMinMaxCountProjectionBlock(
                metadata,
                minmax_projection->dag->getRequiredColumnsNames(),
                filter_node != nullptr,
                query_info,
                parts,
                minmax_projection_normal_parts,
                max_added_blocks.get(),
                context);

            if (!minmax_count_projection_block)
                minmax_projection.reset();
            else
                best_candidate = &*minmax_projection;
        }
    }

    if (!minmax_projection)
    {
        candidates.reserve(agg_projections.size());
        for (const auto * projection : agg_projections)
        {

            LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try projection {}", projection->name);
            auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
            LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection DAG {}", info.before_aggregation->dumpDAG());
            if (auto proj_dag = analyzeAggregateProjection(info, *dag, filter_node, keys, aggregates))
            {
                LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection analyzed DAG {}", proj_dag->dumpDAG());
                candidates.emplace_back(AggregateProjectionCandidate{
                    .info = std::move(info),
                    .projection = projection,
                    .dag = std::move(proj_dag),
                });
            }
        }

        if (candidates.empty())
            return false;
    }

    MergeTreeDataSelectExecutor reader(reading->getMergeTreeData());

    if (context->getSettingsRef().select_sequential_consistency)
    {
        if (const StorageReplicatedMergeTree * replicated = dynamic_cast<const StorageReplicatedMergeTree *>(&reading->getMergeTreeData()))
            max_added_blocks = std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());
    }

    for (auto & candidate : candidates)
    {
        MergeTreeData::DataPartsVector projection_parts;
        MergeTreeData::DataPartsVector normal_parts;
        for (const auto & part : parts)
        {
            const auto & created_projections = part->getProjectionParts();
            auto it = created_projections.find(candidate.projection->name);
            if (it != created_projections.end())
                projection_parts.push_back(it->second);
            else
                normal_parts.push_back(part);
        }

        if (projection_parts.empty())
            continue;

        ActionDAGNodes added_filter_nodes;
        if (filter_node)
            added_filter_nodes.nodes.push_back(candidate.dag->getOutputs().front());

        auto projection_result_ptr = reader.estimateNumMarksToRead(
            std::move(projection_parts),
            nullptr,
            candidate.dag->getRequiredColumnsNames(),
            metadata,
            candidate.projection->metadata,
            query_info, /// How it is actually used? I hope that for index we need only added_filter_nodes
            added_filter_nodes,
            context,
            context->getSettingsRef().max_threads,
            max_added_blocks);

        if (projection_result_ptr->error())
            continue;

        candidate.merge_tree_projection_select_result_ptr = std::move(projection_result_ptr);
        candidate.sum_marks += candidate.merge_tree_projection_select_result_ptr->marks();

        if (!normal_parts.empty())
        {
            auto normal_result_ptr = reading->selectRangesToRead(std::move(normal_parts));

            if (normal_result_ptr->error())
                continue;

            if (normal_result_ptr->marks() != 0)
            {
                candidate.sum_marks += normal_result_ptr->marks();
                candidate.merge_tree_normal_select_result_ptr = std::move(normal_result_ptr);
            }
        }

        if (best_candidate == nullptr || best_candidate->sum_marks > candidate.sum_marks)
            best_candidate = &candidate;
    }

    if (!best_candidate && !minmax_projection)
        return false;

    QueryPlanStepPtr projection_reading;
    bool has_nornal_parts;

    if (minmax_projection)
    {
        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Minmax proj block {}", minmax_count_projection_block.dumpStructure());

        Pipe pipe(std::make_shared<SourceFromSingleChunk>(std::move(minmax_count_projection_block)));
        projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));

        has_nornal_parts = !minmax_projection_normal_parts.empty();
        if (has_nornal_parts)
            reading->resetParts(std::move(minmax_projection_normal_parts));
    }
    else
    {
        auto storage_snapshot = reading->getStorageSnapshot();
        auto proj_snapshot = std::make_shared<StorageSnapshot>(
            storage_snapshot->storage, storage_snapshot->metadata, storage_snapshot->object_columns); //, storage_snapshot->data);
        proj_snapshot->addProjection(best_candidate->projection);

        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Proj snapshot {}", proj_snapshot->getColumns(GetColumnsOptions::Kind::All).toString());

        projection_reading = reader.readFromParts(
            {},
            best_candidate->dag->getRequiredColumnsNames(),
            proj_snapshot,
            query_info_copy,
            context,
            reading->getMaxBlockSize(),
            reading->getNumStreams(),
            max_added_blocks,
            best_candidate->merge_tree_projection_select_result_ptr,
            reading->isParallelReadingEnabled());

        if (!projection_reading)
        {
            Pipe pipe(std::make_shared<NullSource>(proj_snapshot->getSampleBlockForColumns(best_candidate->dag->getRequiredColumnsNames())));
            projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        }

        has_nornal_parts = best_candidate->merge_tree_normal_select_result_ptr != nullptr;
        if (has_nornal_parts)
            reading->setAnalyzedResult(std::move(best_candidate->merge_tree_normal_select_result_ptr));
    }

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection reading header {}", projection_reading->getOutputStream().header.dumpStructure());

    projection_reading->setStepDescription(best_candidate->projection->name);

    auto & projection_reading_node = nodes.emplace_back(QueryPlan::Node{.step = std::move(projection_reading)});
    auto & expr_or_filter_node = nodes.emplace_back();

    if (filter_node)
    {
        expr_or_filter_node.step = std::make_unique<FilterStep>(
            projection_reading_node.step->getOutputStream(),
            best_candidate->dag,
            best_candidate->dag->getOutputs().front()->result_name,
            true);
    }
    else
        expr_or_filter_node.step = std::make_unique<ExpressionStep>(
            projection_reading_node.step->getOutputStream(),
            best_candidate->dag);

    expr_or_filter_node.children.push_back(&projection_reading_node);

    if (!has_nornal_parts)
    {
        /// All parts are taken from projection

        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Expr stream {}", expr_or_filter_node.step->getOutputStream().header.dumpStructure());
        aggregating->requestOnlyMergeForAggregateProjection(expr_or_filter_node.step->getOutputStream());
        node.children.front() = &expr_or_filter_node;

        //optimizeAggregationInOrder(node, nodes);
    }
    else
    {
        node.step = aggregating->convertToAggregatingProjection(expr_or_filter_node.step->getOutputStream());
        node.children.push_back(&expr_or_filter_node);
    }

    return true;
}


bool optimizeUseNormalProjections(Stack & stack, QueryPlan::Nodes & nodes)
{
    const auto & frame = stack.back();

    auto * reading = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!reading)
        return false;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"),
        "Reading {} {} has analyzed result {}",
        reading->getName(), reading->getStepDescription(), reading->hasAnalyzedResult());

    /// Probably some projection already was applied.
    if (reading->hasAnalyzedResult())
        return false;

    if (reading->isQueryWithFinal())
        return false;

    if (reading->isQueryWithSampling())
        return false;

    if (reading->isParallelReadingEnabled())
        return false;

    auto iter = stack.rbegin();
    while (iter != stack.rend())
    {
        auto next = std::next(iter);
        if (next == stack.rend())
            break;

        if (!typeid_cast<FilterStep *>(next->node->step.get()) &&
            !typeid_cast<ExpressionStep *>(next->node->step.get()))
            break;

        iter = next;
    }

    if (iter == stack.rbegin())
        return false;

    const auto metadata = reading->getStorageMetadata();
    const auto & projections = metadata->projections;

    std::vector<const ProjectionDescription *> normal_projections;
    for (const auto & projection : projections)
        if (projection.type == ProjectionDescription::Type::Normal)
            normal_projections.push_back(&projection);

    if (normal_projections.empty())
        return false;

    ActionsDAGPtr dag;
    ActionsDAG::NodeRawConstPtrs filter_nodes;
    bool need_remove_column = false;
    if (!buildAggregatingDAG(*iter->node->children.front(), dag, filter_nodes, need_remove_column))
        return false;

    if (dag)
    {
        dag->removeUnusedActions();
        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Query DAG: {}", dag->dumpDAG());
    }

    const ActionsDAG::Node * filter_node = nullptr;
    if (!filter_nodes.empty())
    {
        auto & outputs = dag->getOutputs();
        filter_node = filter_nodes.back();
        if (filter_nodes.size() > 1)
        {
            if (need_remove_column)
            {
                size_t pos = 0;
                while (pos < outputs.size() && outputs[pos] != filter_node)
                    ++pos;

                if (pos < outputs.size())
                    outputs.erase(outputs.begin() + pos);
            }

            FunctionOverloadResolverPtr func_builder_and =
                std::make_unique<FunctionToOverloadResolverAdaptor>(
                    std::make_shared<FunctionAnd>());

            filter_node = &dag->addFunction(func_builder_and, std::move(filter_nodes), {});
            outputs.insert(outputs.begin(), filter_node);
        }
        else if (!need_remove_column)
            outputs.insert(outputs.begin(), filter_node);
    }

    std::list<NormalProjectionCandidate> candidates;
    NormalProjectionCandidate * best_candidate = nullptr;

    //const Block & header = frame.node->step->getOutputStream().header;
    const Names & required_columns = reading->getRealColumnNames();
    const auto & parts = reading->getParts();
    const auto & query_info = reading->getQueryInfo();
    ContextPtr context = reading->getContext();
    MergeTreeDataSelectExecutor reader(reading->getMergeTreeData());

    auto ordinary_reading_select_result = reading->selectRangesToRead(parts);
    size_t ordinary_reading_marks = ordinary_reading_select_result->marks();

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Marks for ordinary reading {}", ordinary_reading_marks);

    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks;
    if (context->getSettingsRef().select_sequential_consistency)
    {
        if (const StorageReplicatedMergeTree * replicated = dynamic_cast<const StorageReplicatedMergeTree *>(&reading->getMergeTreeData()))
            max_added_blocks = std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());
    }

    for (const auto * projection : normal_projections)
    {
        bool has_all_columns = true;
        for (const auto & col : required_columns)
        {
            if (!projection->sample_block.has(col))
            {
                has_all_columns = false;
                break;
            }
        }

        if (!has_all_columns)
            continue;

        MergeTreeData::DataPartsVector projection_parts;
        MergeTreeData::DataPartsVector normal_parts;
        for (const auto & part : parts)
        {
            const auto & created_projections = part->getProjectionParts();
            auto it = created_projections.find(projection->name);
            if (it != created_projections.end())
                projection_parts.push_back(it->second);
            else
                normal_parts.push_back(part);
        }

        if (projection_parts.empty())
            continue;

        ActionDAGNodes added_filter_nodes;
        if (filter_node)
            added_filter_nodes.nodes.push_back(filter_node);

        auto projection_result_ptr = reader.estimateNumMarksToRead(
            std::move(projection_parts),
            nullptr,
            required_columns,
            metadata,
            projection->metadata,
            query_info, /// How it is actually used? I hope that for index we need only added_filter_nodes
            added_filter_nodes,
            context,
            context->getSettingsRef().max_threads,
            max_added_blocks);

        if (projection_result_ptr->error())
            continue;

        auto & candidate = candidates.emplace_back();
        candidate.projection = projection;
        candidate.merge_tree_projection_select_result_ptr = std::move(projection_result_ptr);
        candidate.sum_marks += candidate.merge_tree_projection_select_result_ptr->marks();

        if (!normal_parts.empty())
        {
            auto normal_result_ptr = reading->selectRangesToRead(std::move(normal_parts));

            if (normal_result_ptr->error())
                continue;

            if (normal_result_ptr->marks() != 0)
            {
                candidate.sum_marks += normal_result_ptr->marks();
                candidate.merge_tree_normal_select_result_ptr = std::move(normal_result_ptr);
            }
        }

        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Marks for projection {} {}", projection->name ,candidate.sum_marks);
        if (candidate.sum_marks < ordinary_reading_marks && (best_candidate == nullptr || candidate.sum_marks < best_candidate->sum_marks))
            best_candidate = &candidate;
    }

    if (!best_candidate)
    {
        reading->setAnalyzedResult(std::move(ordinary_reading_select_result));
        return false;
    }

    auto storage_snapshot = reading->getStorageSnapshot();
    auto proj_snapshot = std::make_shared<StorageSnapshot>(
        storage_snapshot->storage, storage_snapshot->metadata, storage_snapshot->object_columns); //, storage_snapshot->data);
    proj_snapshot->addProjection(best_candidate->projection);

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Proj snapshot {}", proj_snapshot->getColumns(GetColumnsOptions::Kind::All).toString());

    auto query_info_copy = query_info;
    query_info_copy.prewhere_info = nullptr;

    auto projection_reading = reader.readFromParts(
        {},
        required_columns,
        proj_snapshot,
        query_info_copy,
        context,
        reading->getMaxBlockSize(),
        reading->getNumStreams(),
        max_added_blocks,
        best_candidate->merge_tree_projection_select_result_ptr,
        reading->isParallelReadingEnabled());

    if (!projection_reading)
    {
        Pipe pipe(std::make_shared<NullSource>(proj_snapshot->getSampleBlockForColumns(required_columns)));
        projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
    }

    bool has_nornal_parts = best_candidate->merge_tree_normal_select_result_ptr != nullptr;
    if (has_nornal_parts)
        reading->setAnalyzedResult(std::move(best_candidate->merge_tree_normal_select_result_ptr));

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection reading header {}", projection_reading->getOutputStream().header.dumpStructure());

    projection_reading->setStepDescription(best_candidate->projection->name);

    auto & projection_reading_node = nodes.emplace_back(QueryPlan::Node{.step = std::move(projection_reading)});
    auto * next_node = &projection_reading_node;

    if (dag)
    {
        auto & expr_or_filter_node = nodes.emplace_back();

        if (filter_node)
        {
            expr_or_filter_node.step = std::make_unique<FilterStep>(
                projection_reading_node.step->getOutputStream(),
                dag,
                filter_node->result_name,
                true);
        }
        else
            expr_or_filter_node.step = std::make_unique<ExpressionStep>(
                projection_reading_node.step->getOutputStream(),
                dag);

        expr_or_filter_node.children.push_back(&projection_reading_node);
        next_node = &expr_or_filter_node;
    }

    if (!has_nornal_parts)
    {
        /// All parts are taken from projection
        iter->node->children.front() = next_node;

        //optimizeAggregationInOrder(node, nodes);
    }
    else
    {
        auto & union_node = nodes.emplace_back();
        DataStreams input_streams = {iter->node->children.front()->step->getOutputStream(), next_node->step->getOutputStream()};
        union_node.step = std::make_unique<UnionStep>(std::move(input_streams));
        union_node.children = {iter->node->children.front(), next_node};
        iter->node->children.front() = &union_node;

        iter->next_child = 0;
        stack.resize(iter.base() - stack.begin() + 1);
    }

    return true;
}

}
