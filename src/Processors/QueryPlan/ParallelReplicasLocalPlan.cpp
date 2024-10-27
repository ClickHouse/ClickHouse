#include <Processors/QueryPlan/ParallelReplicasLocalPlan.h>

#include <Common/checkStackSize.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTFunction.h>
#include <Processors/QueryPlan/ConvertingActions.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Core/Settings.h>
#include <ranges>

namespace DB
{

namespace Setting
{
    extern const SettingsBool merge_tree_determine_task_size_by_prewhere_columns;
    extern const SettingsUInt64 merge_tree_min_bytes_per_task_for_remote_reading;
    extern const SettingsUInt64 parallel_replicas_mark_segment_size;
}

static size_t getApproxSizeOfPart(const IMergeTreeDataPart & part, const Names & columns_to_read)
{
    ColumnSize columns_size{};
    for (const auto & col_name : columns_to_read)
        columns_size.add(part.getColumnSize(col_name));
    /// For compact parts we don't know individual column sizes, let's use whole part size as approximation
    return columns_size.data_compressed ? columns_size.data_compressed : part.getBytesOnDisk();
}

static size_t calculateMinMarksPerTask(
    const RangesInDataPart & part,
    const Names & columns_to_read,
    PrewhereInfoPtr prewhere_info,
    size_t min_marks_for_concurrent_read,
    size_t sum_marks,
    size_t threads,
    const Settings & settings)
{
    size_t min_marks_per_task = min_marks_for_concurrent_read;
    const size_t part_marks_count = part.getMarksCount();
    if (part_marks_count && part.data_part->isStoredOnRemoteDisk())
    {
        /// We assume that most of the time prewhere does it's job good meaning that lion's share of the rows is filtered out.
        /// Which means in turn that for most of the rows we will read only the columns from prewhere clause.
        /// So it makes sense to use only them for the estimation.
        const auto & columns = settings[Setting::merge_tree_determine_task_size_by_prewhere_columns] && prewhere_info
            ? prewhere_info->prewhere_actions.getRequiredColumnsNames()
            : columns_to_read;
        const size_t part_compressed_bytes = getApproxSizeOfPart(*part.data_part, columns);

        const auto avg_mark_bytes = std::max<size_t>(part_compressed_bytes / part_marks_count, 1);
        const auto min_bytes_per_task = settings[Setting::merge_tree_min_bytes_per_task_for_remote_reading];
        /// We're taking min here because number of tasks shouldn't be too low - it will make task stealing impossible.
        /// We also create at least two tasks per thread to have something to steal from a slow thread.
        const auto heuristic_min_marks
            = std::min<size_t>(sum_marks / threads / 2, min_bytes_per_task / avg_mark_bytes);
        if (heuristic_min_marks > min_marks_per_task)
        {
            LOG_TEST(
                &Poco::Logger::get("ParallelReplicasLocalPlan"),
                "Increasing min_marks_per_task from {} to {} based on columns size heuristic",
                min_marks_per_task,
                heuristic_min_marks);
            min_marks_per_task = heuristic_min_marks;
        }
    }

    LOG_TEST(&Poco::Logger::get("ParallelReplicasLocalPlan"), "Will use min_marks_per_task={}", min_marks_per_task);
    return min_marks_per_task;
}

size_t chooseSegmentSize(
    LoggerPtr log, size_t mark_segment_size, size_t min_marks_per_task, size_t threads, size_t sum_marks, size_t number_of_replicas)
{
    /// Mark segment size determines the granularity of work distribution between replicas.
    /// Namely, coordinator will take mark segments of size `mark_segment_size` granules, calculate hash of this segment and assign it to corresponding replica.
    /// Small segments are good when we read a small random subset of a table, big - when we do full-scan over a large table.
    /// With small segments there is a problem: consider a query like `select max(time) from wikistat`. Average size of `time` per granule is ~5KB. So when we
    /// read 128 granules we still read only ~0.5MB of data. With default fs cache segment size of 4MB it means a lot of data will be downloaded and written
    /// in cache for no reason. General case will look like this:
    ///
    ///                                    +---------- useful data
    ///                                    v
    ///                           +------+--+------+
    ///                           |------|++|      |
    ///                           |------|++|      |
    ///                           +------+--+------+
    ///                               ^
    /// predownloaded data -----------+
    ///
    /// Having large segments solves all the problems in this case. Also bigger segments mean less requests (especially for big tables and full-scans).
    /// These three values below chosen mostly intuitively. 128 granules is 1M rows - just a good starting point, 16384 seems to still make sense when reading
    /// billions of rows and 1024 - is a reasonable point in between. We limit our choice to only these three options because when we change segment size
    /// we essentially change distribution of data between replicas and of course we don't want to use simultaneously tens of different distributions, because
    /// it would be a huge waste of cache space.
    constexpr std::array<size_t, 3> borders{128, 1024, 16384};

    LOG_DEBUG(
        log,
        "mark_segment_size={}, min_marks_per_task*threads={}, sum_marks/number_of_replicas^2={}",
        mark_segment_size,
        min_marks_per_task * threads,
        sum_marks / number_of_replicas / number_of_replicas);

    /// Here we take max of two numbers:
    /// * (min_marks_per_task * threads) = the number of marks we request from the coordinator each time - there is no point to have segments smaller than one unit of work for a replica
    /// * (sum_marks / number_of_replicas^2) - we use consistent hashing for work distribution (including work stealing). If we have a really slow replica
    ///   everything except (1/number_of_replicas) portion of its work will be stolen by other replicas. And it owns (1/number_of_replicas) share of total number of marks.
    ///   Also important to note here that sum_marks is calculated after PK analysis, it means in particular that different segment sizes might be used for the
    ///   same table for different queries (it is intentional).
    ///
    /// Positive `mark_segment_size` means it is a user provided value, we have to preserve it.
    if (mark_segment_size == 0)
        mark_segment_size = std::max(min_marks_per_task * threads, sum_marks / number_of_replicas / number_of_replicas);

    /// Squeeze the value to the borders.
    mark_segment_size = std::clamp(mark_segment_size, borders.front(), borders.back());
    /// After we calculated a hopefully good value for segment_size let's just find the maximal border that is not bigger than the chosen value.
    for (auto border : borders | std::views::reverse)
    {
        if (mark_segment_size >= border)
        {
            LOG_DEBUG(log, "Chosen segment size: {}", border);
            return border;
        }
    }

    UNREACHABLE();
}

static void initCoordinator(ParallelReplicasReadingCoordinatorPtr coordinator, const ReadFromMergeTree * read_from_merge_tree, const Settings & settings, size_t replica_num)
{
    CoordinationMode mode{CoordinationMode::Default};
    auto order_info = read_from_merge_tree->getQueryInfo().input_order_info;
    if (order_info)
    {
        chassert(order_info->direction == 1 || order_info->direction == -1);
        mode = order_info->direction == 1 ? CoordinationMode::WithOrder : CoordinationMode::ReverseOrder;
    }

    size_t min_marks_per_task = 0;
    const size_t threads = read_from_merge_tree->getNumStreams();
    const auto analysis = read_from_merge_tree->getAnalyzedResult();
    const auto & ranges = analysis->parts_with_ranges;
    const auto sum_marks = ranges.getMarksCountAllParts();
    const size_t min_marks_for_concurrent_read = read_from_merge_tree->getMinMarksForConcurrentRead(ranges);
    for (const auto & range : ranges)
    {
        min_marks_per_task = std::max(
            min_marks_per_task,
            calculateMinMarksPerTask(
                range, analysis->column_names_to_read, read_from_merge_tree->getPrewhereInfo(), min_marks_for_concurrent_read, sum_marks, threads, settings));
    }

    size_t mark_segment_size = chooseSegmentSize(
        Poco::Logger::getShared("ParallelReplicasLocalPlan"),
        settings[Setting::parallel_replicas_mark_segment_size],
        min_marks_per_task,
        threads,
        sum_marks,
        coordinator->getReplicasCount());

    coordinator->handleInitialAllRangesAnnouncement(
        InitialAllRangesAnnouncement{mode, ranges.getDescriptions(), replica_num, mark_segment_size});
}

std::pair<std::unique_ptr<QueryPlan>, bool> createLocalPlanForParallelReplicas(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    ParallelReplicasReadingCoordinatorPtr coordinator,
    QueryPlanStepPtr analyzed_read_from_merge_tree,
    size_t replica_number)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();
    auto new_context = Context::createCopy(context);

    /// Do not push down limit to local plan, as it will break `rows_before_limit_at_least` counter.
    if (processed_stage == QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit)
        processed_stage = QueryProcessingStage::WithMergeableStateAfterAggregation;

    /// Do not apply AST optimizations, because query
    /// is already optimized and some optimizations
    /// can be applied only for non-distributed tables
    /// and we can produce query, inconsistent with remote plans.
    auto select_query_options = SelectQueryOptions(processed_stage).ignoreASTOptimizations();

    /// For Analyzer, identifier in GROUP BY/ORDER BY/LIMIT BY lists has been resolved to
    /// ConstantNode in QueryTree if it is an alias of a constant, so we should not replace
    /// ConstantNode with ProjectionNode again(https://github.com/ClickHouse/ClickHouse/issues/62289).
    new_context->setSetting("enable_positional_arguments", Field(false));
    new_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
    auto interpreter = InterpreterSelectQueryAnalyzer(query_ast, new_context, select_query_options);
    query_plan = std::make_unique<QueryPlan>(std::move(interpreter).extractQueryPlan());

    QueryPlan::Node * node = query_plan->getRootNode();
    ReadFromMergeTree * reading = nullptr;
    while (node)
    {
        reading = typeid_cast<ReadFromMergeTree *>(node->step.get());
        if (reading)
            break;

        if (!node->children.empty())
            node = node->children.at(0);
        else
            node = nullptr;
    }

    if (!reading)
        /// it can happened if merge tree table is empty, - it'll be replaced with ReadFromPreparedSource
        return {std::move(query_plan), false};

    MergeTreeAllRangesCallback announcement_cb;
    ReadFromMergeTree::AnalysisResultPtr analyzed_result_ptr;
    if (analyzed_read_from_merge_tree.get())
    {
        auto * analyzed_merge_tree = typeid_cast<ReadFromMergeTree *>(analyzed_read_from_merge_tree.get());
        chassert(analyzed_merge_tree);
        analyzed_result_ptr = analyzed_merge_tree->getAnalyzedResult();

        announcement_cb = [coordinator](InitialAllRangesAnnouncement) {};

        initCoordinator(coordinator, analyzed_merge_tree, context->getSettingsRef(), replica_number);
    }
    else
    {
        announcement_cb = [coordinator](InitialAllRangesAnnouncement announcement)
        { coordinator->handleInitialAllRangesAnnouncement(std::move(announcement)); };
    }

    MergeTreeReadTaskCallback read_task_cb = [coordinator](ParallelReadRequest req) -> std::optional<ParallelReadResponse>
    { return coordinator->handleRequest(std::move(req)); };

    auto read_from_merge_tree_parallel_replicas = reading->createLocalParallelReplicasReadingStep(
        std::move(analyzed_result_ptr), std::move(announcement_cb), std::move(read_task_cb), replica_number);
    node->step = std::move(read_from_merge_tree_parallel_replicas);

    addConvertingActions(*query_plan, header, /*has_missing_objects=*/false);

    return {std::move(query_plan), true};
}

}
