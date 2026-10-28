#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnsNumber.h>
#include <Access/EnabledQuota.h>
#include <Access/QuotaUsage.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Settings.h>
#include <Databases/enableAllExperimentalSettings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/Operators.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/getEffectiveMaxThreads.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/TwoPassCountTopKTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <base/defines.h>

#include <algorithm>
#include <unordered_map>

namespace ProfileEvents
{
    extern const Event TwoPassCountTopKPasses;
    extern const Event TwoPassCountTopKFallbacks;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsUInt64 max_rows_to_sort;
    extern const SettingsUInt64 max_bytes_to_sort;
    extern const SettingsBool rows_before_aggregation;
    extern const SettingsDouble limit;
    extern const SettingsDouble offset;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 aggregation_in_order_max_block_bytes;
    extern const SettingsUInt64 aggregation_memory_efficient_merge_threads;
    extern const SettingsBool enable_memory_bound_merging_of_aggregation_results;
    extern const SettingsBool force_primary_key;
    extern const SettingsBool force_index_by_date;
    extern const SettingsString force_data_skipping_indexes;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{

namespace
{

constexpr UInt64 max_top = 1000;
constexpr size_t sample_period = 64;
constexpr UInt64 min_sample_rows_divisor = 200;
constexpr double max_unique_share = 0.9;
constexpr double max_top_share = 0.5;
constexpr UInt64 min_lower_bound = 8;
constexpr size_t max_candidates = 200'000;
constexpr size_t max_sampled_values = 20'000'000;
/// Without granule skipping by the key, a restricted pass covers the most frequent values holding up to this share of the rows.
constexpr UInt64 unpruned_rows_divisor = 10;

/// Every node the passes add carries this prefix, and a query whose own names contain it is not optimized.
constexpr std::string_view reserved_name_prefix = "__two_pass_count_top_k";
constexpr auto in_column_name = "__two_pass_count_top_k_in";
constexpr auto set_column_name = "__two_pass_count_top_k_set";

struct TwoPassTemplate
{
    std::unique_ptr<ReadFromMergeTree> read;
    std::vector<QueryPlanStepPtr> pre_steps;
    std::unique_ptr<AggregatingStep> aggregating;
    std::vector<QueryPlanStepPtr> post_steps;
    std::unique_ptr<SortingStep> sorting;
    size_t limit = 0;
    size_t offset = 0;

    String key_name;
    String key_column;
    DataTypePtr key_type;
    /// The primary or partition key contains the key column, so a restricted pass reads only the granules of its values.
    bool key_prunes_granules = false;

    size_t count_aggregate = 0;
    size_t count_position = 0;

    UInt64 min_rows = 0;
    ContextPtr context;

    size_t top() const { return limit + offset; }
};

using TwoPassTemplatePtr = std::shared_ptr<const TwoPassTemplate>;

template <typename Step>
std::unique_ptr<Step> cloneTyped(const Step & step)
{
    auto cloned = step.clone();
    auto * typed = typeid_cast<Step *>(cloned.get());
    if (!typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Clone of {} has a different type", step.getName());
    cloned.release();
    return std::unique_ptr<Step>(typed);
}

std::unique_ptr<AggregatingStep> makeAggregatingStep(const SharedHeader & header, Aggregator::Params params, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    params.stats_collecting_params.disable();
    const size_t max_threads = getEffectiveMaxThreads(settings);
    const size_t temporary_data_merge_threads = settings[Setting::aggregation_memory_efficient_merge_threads]
        ? static_cast<size_t>(settings[Setting::aggregation_memory_efficient_merge_threads])
        : max_threads;
    return std::make_unique<AggregatingStep>(
        header,
        std::move(params),
        GroupingSetsParamsList{},
        /*final_=*/ true,
        settings[Setting::max_block_size],
        settings[Setting::aggregation_in_order_max_block_bytes],
        max_threads,
        temporary_data_merge_threads,
        /*storage_has_evenly_distributed_read_=*/ false,
        /*group_by_use_nulls_=*/ false,
        SortDescription{},
        SortDescription{},
        /*should_produce_results_in_order_of_bucket_number_=*/ false,
        settings[Setting::enable_memory_bound_merging_of_aggregation_results],
        /*explicit_sorting_required_for_aggregation_in_order_=*/ false);
}

Aggregator::Params countParams(const TwoPassTemplate & templ, Names keys)
{
    Aggregator::Params params = templ.aggregating->getParams();
    AggregateDescription count = params.aggregates[templ.count_aggregate];
    params.keys = std::move(keys);
    params.keys_size = params.keys.size();
    params.aggregates = {std::move(count)};
    params.aggregates_size = 1;
    params.bucket_top_k = 0;
    params.threshold_top_k.reset();
    params.having_prefilter_op = Aggregator::Params::HavingPrefilterOp::Disabled;
    params.top_k.reset();
    return params;
}

const String & countName(const TwoPassTemplate & templ)
{
    return templ.aggregating->getParams().aggregates[templ.count_aggregate].column_name;
}

void convertHeader(QueryPlan & plan, const SharedHeader & expected)
{
    const auto & current = plan.getCurrentHeader();
    if (blocksHaveEqualStructure(*current, *expected))
        return;
    auto dag = ActionsDAG::makeConvertingActions(
        current->getColumnsWithTypeAndName(), expected->getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name, nullptr);
    plan.addStep(std::make_unique<ExpressionStep>(current, std::move(dag)));
}

void addReadAndFilters(QueryPlan & plan, const TwoPassTemplate & templ, std::unique_ptr<ReadFromMergeTree> read)
{
    plan.addStep(std::move(read));
    convertHeader(plan, templ.pre_steps.empty() ? templ.aggregating->getInputHeaders().front() : templ.pre_steps.front()->getInputHeaders().front());
    for (const auto & step : templ.pre_steps)
        plan.addStep(step->clone());
}

void project(QueryPlan & plan, const Names & names)
{
    const auto & header = plan.getCurrentHeader();
    ActionsDAG dag(header->getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs outputs;
    for (const auto & name : names)
        outputs.push_back(&dag.findInOutputs(name));
    dag.getOutputs() = std::move(outputs);
    dag.removeUnusedActions(/*allow_remove_inputs=*/ false);
    plan.addStep(std::make_unique<ExpressionStep>(header, std::move(dag)));
}

void keepEveryNthMark(ReadFromMergeTree::AnalysisResult & analysis, size_t period)
{
    size_t global_mark = 0;
    RangesInDataParts kept;
    for (auto & part : analysis.parts_with_ranges)
    {
        MarkRanges sampled;
        for (const auto & range : part.ranges)
        {
            for (size_t mark = range.begin; mark < range.end; ++mark, ++global_mark)
            {
                if (global_mark % period != 0)
                    continue;
                if (!sampled.empty() && sampled.back().end == mark)
                    sampled.back().end = mark + 1;
                else
                    sampled.emplace_back(mark, mark + 1);
            }
        }
        if (sampled.empty())
            continue;
        part.ranges = std::move(sampled);
        kept.push_back(std::move(part));
    }
    analysis.parts_with_ranges = std::move(kept);
}

MarkRanges intersectRanges(MarkRanges lhs, MarkRanges rhs)
{
    auto by_begin = [](const MarkRange & a, const MarkRange & b) { return a.begin < b.begin; };
    std::sort(lhs.begin(), lhs.end(), by_begin);
    std::sort(rhs.begin(), rhs.end(), by_begin);
    MarkRanges result;
    size_t i = 0;
    size_t j = 0;
    while (i < lhs.size() && j < rhs.size())
    {
        const size_t begin = std::max(lhs[i].begin, rhs[j].begin);
        const size_t end = std::min(lhs[i].end, rhs[j].end);
        if (begin < end)
        {
            if (!result.empty() && result.back().end == begin)
                result.back().end = end;
            else
                result.emplace_back(begin, end);
        }
        if (lhs[i].end < rhs[j].end)
            ++i;
        else
            ++j;
    }
    return result;
}

void restrictRanges(ReadFromMergeTree::AnalysisResult & analysis, const ReadFromMergeTree::AnalysisResult & restriction)
{
    std::unordered_map<const IMergeTreeDataPart *, const MarkRanges *> restricted;
    for (const auto & part : restriction.parts_with_ranges)
        restricted.emplace(part.data_part.get(), &part.ranges);
    RangesInDataParts kept;
    for (auto & part : analysis.parts_with_ranges)
    {
        auto it = restricted.find(part.data_part.get());
        if (it == restricted.end())
            continue;
        part.ranges = intersectRanges(std::move(part.ranges), *it->second);
        if (part.ranges.empty())
            continue;
        kept.push_back(std::move(part));
    }
    analysis.parts_with_ranges = std::move(kept);
}

void updateTotals(ReadFromMergeTree::AnalysisResult & analysis)
{
    UInt64 marks = 0;
    UInt64 rows = 0;
    UInt64 ranges = 0;
    for (auto & part : analysis.parts_with_ranges)
    {
        part.exact_ranges.clear();
        rows += part.getRowsCount();
        marks += part.getMarksCount();
        ranges += part.ranges.size();
    }
    analysis.selected_parts = analysis.parts_with_ranges.size();
    analysis.selected_ranges = ranges;
    analysis.selected_marks = marks;
    analysis.selected_rows = rows;
    analysis.has_exact_ranges = false;
    analysis.exact_ranges_excluded = false;
}

ActionsDAG makeInFilter(const TwoPassTemplate & templ, const FutureSetPtr & set)
{
    ActionsDAG dag;
    const auto & key = dag.addInput(templ.key_column, templ.key_type);
    ColumnConst::Ptr set_column = ColumnConst::create(ColumnSet::create(1, set), 0);
    const auto & set_node = dag.addColumn(std::move(set_column), std::make_shared<DataTypeSet>(), set_column_name);
    const auto & in = dag.addFunction(FunctionFactory::instance().get("in", templ.context), {&key, &set_node}, in_column_name);
    dag.getOutputs() = {&key, &in};
    return dag;
}

/// `ActionsDAG::mergeNodes` and the reader resolve nodes by name: the merged IN filter must be the new `in` over the key column
/// input and the set, and neither it nor the conjunction may share a name with another node or a column of the read.
/// Returns nullptr otherwise.
PrewhereInfoPtr addToPrewhere(const PrewhereInfoPtr & existing, ActionsDAG in_filter, const TwoPassTemplate & templ, const Block & read_header)
{
    auto result = std::make_shared<PrewhereInfo>();
    result->need_filter = true;
    result->remove_prewhere_column = true;

    auto is_new_in = [&](const ActionsDAG & dag, const ActionsDAG::Node * node)
    {
        return node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->getName() == "in"
            && node->children.size() == 2 && node->children[0]->type == ActionsDAG::ActionType::INPUT
            && node->children[0]->result_name == templ.key_column && node->children[0]->result_type->equals(*templ.key_type)
            && node->children[1]->type == ActionsDAG::ActionType::COLUMN && node->children[1]->result_name == set_column_name
            && !hasResultNameClash(dag, node) && !hasResultNameClash(dag, node->children[0]) && !hasResultNameClash(dag, node->children[1]);
    };

    if (!existing)
    {
        const auto * in_node = &in_filter.findInOutputs(in_column_name);
        if (!is_new_in(in_filter, in_node) || read_header.has(in_column_name))
            return nullptr;
        result->prewhere_actions = std::move(in_filter);
        result->prewhere_column_name = in_column_name;
        return result;
    }

    ActionsDAG combined = existing->prewhere_actions.clone();
    const auto * existing_filter = &combined.findInOutputs(existing->prewhere_column_name);
    ActionsDAG::NodeRawConstPtrs new_outputs;
    combined.mergeNodes(std::move(in_filter), &new_outputs);
    const ActionsDAG::Node * in_node = nullptr;
    for (const auto * node : new_outputs)
        if (node->result_name == in_column_name)
            in_node = node;
    if (!in_node || !is_new_in(combined, in_node))
        return nullptr;

    const auto * and_node = &combined.addFunction(FunctionFactory::instance().get("and", templ.context), {existing_filter, in_node}, {});
    if (hasResultNameClash(combined, and_node) || read_header.has(and_node->result_name))
        return nullptr;
    auto & outputs = combined.getOutputs();
    for (const auto * node : new_outputs)
        if (node != in_node && std::ranges::find(outputs, node) == outputs.end())
            outputs.push_back(node);
    if (existing->remove_prewhere_column)
        std::erase(outputs, existing_filter);
    outputs.push_back(and_node);

    result->prewhere_actions = std::move(combined);
    result->prewhere_column_name = and_node->result_name;
    return result;
}

FutureSetPtr makeSet(const TwoPassTemplate & templ, const std::vector<UInt64> & keys)
{
    auto column = templ.key_type->createColumn();
    column->reserve(keys.size());
    SipHash hash;
    hash.update(templ.key_type->getName());
    hash.update(keys.size());
    for (UInt64 key : keys)
    {
        column->insert(templ.key_type->isValueRepresentedByUnsignedInteger() ? Field(key) : Field(static_cast<Int64>(key)));
        hash.update(key);
    }
    UInt64 hash_low = 0;
    UInt64 hash_high = 0;
    hash.get128(hash_low, hash_high);
    ColumnsWithTypeAndName block{ColumnWithTypeAndName(std::move(column), templ.key_type, templ.key_column)};
    return std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash(hash_low, hash_high), nullptr, std::move(block), /*transform_null_in=*/ false, SizeLimits{});
}

QueryPlan makePilotPlan(const TwoPassTemplate & templ, bool groups)
{
    auto read = cloneTyped(*templ.read);
    auto sampled = std::make_shared<ReadFromMergeTree::AnalysisResult>(*templ.read->getAnalyzedResult());
    keepEveryNthMark(*sampled, sample_period);
    updateTotals(*sampled);
    read->setAnalyzedResult(std::move(sampled));
    read->adoptFiltersFrom(*templ.read);
    read->disableQueryConditionCache();

    QueryPlan plan;
    addReadAndFilters(plan, templ, std::move(read));
    const Names keys = groups ? templ.aggregating->getParams().keys : Names{templ.key_name};
    project(plan, keys);
    removeUnusedColumns(*plan.getRootNode(), RemoveUnusedColumnsMode::Global);
    auto params = countParams(templ, keys);
    const String & count_name = countName(templ);
    if (groups)
    {
        const size_t top = templ.top();
        params.bucket_top_k = top;
        params.bucket_top_k_ascending = false;
        params.bucket_top_k_rank_index = 0;
        plan.addStep(makeAggregatingStep(plan.getCurrentHeader(), std::move(params), templ.context));
        SortDescription description;
        description.emplace_back(count_name, -1, 1);
        plan.addStep(std::make_unique<SortingStep>(
            plan.getCurrentHeader(), std::move(description), top, SortingStep::Settings(templ.context->getSettingsRef())));
        plan.addStep(std::make_unique<LimitStep>(plan.getCurrentHeader(), top, 0));
    }
    else
        plan.addStep(makeAggregatingStep(plan.getCurrentHeader(), std::move(params), templ.context));
    project(plan, {count_name});
    return plan;
}

QueryPlan makeCountsPlan(const TwoPassTemplate & templ, std::optional<size_t> top_values, UInt64 min_count)
{
    auto read = cloneTyped(*templ.read);
    read->adoptFiltersFrom(*templ.read);
    QueryPlan plan;
    addReadAndFilters(plan, templ, std::move(read));
    project(plan, {templ.key_name});
    removeUnusedColumns(*plan.getRootNode(), RemoveUnusedColumnsMode::Global);
    auto params = countParams(templ, {templ.key_name});
    if (top_values && *top_values <= Aggregator::Params::max_bucket_top_k)
    {
        params.bucket_top_k = *top_values;
        params.bucket_top_k_ascending = false;
        params.bucket_top_k_rank_index = 0;
    }
    else if (!top_values)
    {
        params.having_prefilter_op = Aggregator::Params::HavingPrefilterOp::GreaterOrEqual;
        params.having_prefilter_threshold = min_count;
        params.having_prefilter_count_index = 0;
    }
    plan.addStep(makeAggregatingStep(plan.getCurrentHeader(), std::move(params), templ.context));

    const String & count_name = countName(templ);
    if (top_values)
    {
        SortDescription description;
        description.emplace_back(count_name, -1, 1);
        plan.addStep(std::make_unique<SortingStep>(
            plan.getCurrentHeader(), std::move(description), *top_values, SortingStep::Settings(templ.context->getSettingsRef())));
        plan.addStep(std::make_unique<LimitStep>(plan.getCurrentHeader(), *top_values, 0));
    }
    else
    {
        const auto & header = plan.getCurrentHeader();
        ActionsDAG dag(header->getColumnsWithTypeAndName());
        const auto & count = dag.findInOutputs(count_name);
        auto type = std::make_shared<DataTypeUInt64>();
        ColumnConst::Ptr bound_column = ColumnConst::create(ColumnUInt64::create(1, min_count), 1);
        const auto & bound = dag.addColumn(std::move(bound_column), type, "__two_pass_count_top_k_bound");
        const auto & filter = dag.addFunction(FunctionFactory::instance().get("greaterOrEquals", templ.context), {&count, &bound}, "__two_pass_count_top_k_having");
        dag.getOutputs().push_back(&filter);
        plan.addStep(std::make_unique<FilterStep>(header, std::move(dag), filter.result_name, /*remove_filter_column=*/ true));
    }
    project(plan, {templ.key_name, count_name});
    return plan;
}

QueryPlan makeRestrictedPlan(const TwoPassTemplate & templ, const std::vector<UInt64> & keys, size_t limit, size_t offset)
{
    auto set = makeSet(templ, keys);

    auto ranges = std::make_shared<ReadFromMergeTree::AnalysisResult>(*templ.read->getAnalyzedResult());
    {
        auto analysis_read = cloneTyped(*templ.read);
        analysis_read->disableQueryConditionCache();
        auto filter = makeInFilter(templ, set);
        analysis_read->addFilter(std::move(filter), in_column_name);
        analysis_read->SourceStepWithFilterBase::applyFilters();
        if (auto restriction = analysis_read->selectRangesToRead(); restriction && restriction->isUsable())
        {
            restrictRanges(*ranges, *restriction);
            updateTotals(*ranges);
        }
    }

    auto read = cloneTyped(*templ.read);
    read->disableQueryConditionCache();
    auto prewhere = addToPrewhere(read->getPrewhereInfo(), makeInFilter(templ, set), templ, *read->getOutputHeader());
    if (!prewhere)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The IN filter of the two-pass top-K does not merge into PREWHERE");
    read->updatePrewhereInfo(prewhere);
    read->setAnalyzedResult(std::move(ranges));
    read->adoptFiltersFrom(*templ.read);

    QueryPlan plan;
    addReadAndFilters(plan, templ, std::move(read));
    plan.addStep(makeAggregatingStep(plan.getCurrentHeader(), templ.aggregating->getParams(), templ.context));
    convertHeader(plan, templ.post_steps.empty() ? templ.sorting->getInputHeaders().front() : templ.post_steps.front()->getInputHeaders().front());
    for (const auto & step : templ.post_steps)
        plan.addStep(step->clone());
    plan.addStep(templ.sorting->clone());
    plan.addStep(std::make_unique<LimitStep>(plan.getCurrentHeader(), limit, offset));
    return plan;
}

/// Pilots over every 64th granule (key counts: reject a near-unique key or dominant values; group counts: lower bound B),
/// a count pass, then the query restricted to the most frequent values (exact if the n-th count is at least the count
/// of every other value), else over the values with at least max(B, n-th count) rows. Each pass is added to the running
/// pipeline; input 0 is the original plan, used when the pilots reject.
class TwoPassCountTopKTransform final : public IProcessor
{
public:
    TwoPassCountTopKTransform(const SharedHeader & header, TwoPassTemplatePtr templ_)
        : IProcessor({InputPort(*header)}, {OutputPort(*header)})
        , templ(std::move(templ_))
        , log(getLogger("TwoPassCountTopK"))
    {
    }

    String getName() const override { return "TwoPassCountTopKTransform"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit.swap(counter); }

    Status prepare() override
    {
        auto & output = outputs.front();
        if (output.isFinished())
        {
            closeInputs();
            return Status::Finished;
        }

        if (!new_processors.empty())
            return Status::UpdatePipeline;

        if (to_close)
        {
            to_close->close();
            to_close = nullptr;
        }
        if (close_original)
        {
            inputs.front().close();
            close_original = false;
        }

        switch (stage)
        {
            case Stage::Start:
            case Stage::Deciding:
                return Status::Ready;

            case Stage::Collecting:
            {
                if (pending_chunk)
                    return Status::Ready;
                if (current->isFinished())
                {
                    stage = Stage::Deciding;
                    return Status::Ready;
                }
                current->setNeeded();
                if (!current->hasData())
                    return Status::NeedData;
                pending_chunk = current->pull();
                return Status::Ready;
            }

            case Stage::Forwarding:
            {
                if (current->isFinished())
                {
                    if (pass == Pass::Final && current != &inputs.front() && final_rows_before_limit)
                        rows_before_limit->add(final_rows_before_limit->get());
                    output.finish();
                    closeInputs();
                    return Status::Finished;
                }
                if (!output.canPush())
                {
                    current->setNotNeeded();
                    return Status::PortFull;
                }
                current->setNeeded();
                if (!current->hasData())
                    return Status::NeedData;
                output.push(current->pull());
                return Status::PortFull;
            }

            case Stage::Emitting:
            {
                if (!output.canPush())
                    return Status::PortFull;
                if (next_to_emit == to_emit.size())
                {
                    output.finish();
                    closeInputs();
                    return Status::Finished;
                }
                output.push(std::move(to_emit[next_to_emit++]));
                return Status::PortFull;
            }
        }
        UNREACHABLE();
    }

    void work() override
    {
        switch (stage)
        {
            case Stage::Start:
                startPass(Pass::KeyPilot, makePilotPlan(*templ, /*groups=*/ false));
                break;
            case Stage::Collecting:
                consume(std::move(*pending_chunk));
                pending_chunk.reset();
                break;
            case Stage::Deciding:
                decide();
                break;
            case Stage::Forwarding:
            case Stage::Emitting:
                break;
        }
    }

    PipelineUpdate updatePipeline() override
    {
        inputs.emplace_back(new_output->getHeader(), this);
        connect(*new_output, inputs.back());
        current = &inputs.back();
        current->setNeeded();
        new_output = nullptr;
        /// The processors of a pass point to the steps of a plan that no longer exists.
        for (auto & processor : new_processors)
            processor->inheritQueryPlanStepFromParent(*this, getQueryPlanStepGroup());
        return PipelineUpdate{.to_add = std::move(new_processors), .to_remove = {}};
    }

private:
    enum class Stage : uint8_t
    {
        Start,
        Collecting,
        Deciding,
        Forwarding,
        Emitting,
    };

    enum class Pass : uint8_t
    {
        KeyPilot,
        GroupPilot,
        Counts,
        Restricted,
        Final,
    };

    TwoPassTemplatePtr templ;
    LoggerPtr log;
    RowsBeforeStepCounterPtr rows_before_limit;
    /// Rows before the LIMIT of the restricted and the final pass.
    RowsBeforeStepCounterPtr restricted_rows_before_limit;
    RowsBeforeStepCounterPtr final_rows_before_limit;

    Stage stage = Stage::Start;
    Pass pass = Pass::KeyPilot;

    InputPort * current = nullptr;
    InputPort * to_close = nullptr;
    bool close_original = false;
    std::optional<Chunk> pending_chunk;

    OutputPort * new_output = nullptr;
    Processors new_processors;
    std::vector<QueryPlanResourceHolder> resources;

    UInt64 sample_rows = 0;
    std::vector<UInt64> sample_key_counts;
    bool too_many_sampled_values = false;
    UInt64 sample_theta = 0;
    std::vector<UInt64> sample_group_counts;
    UInt64 bound = 0;

    std::vector<std::pair<UInt64, UInt64>> counted;
    std::optional<size_t> fetched;
    UInt64 complete_from = 0;
    bool too_many_values = false;

    size_t num_values = 0;
    UInt64 theta = 0;
    Chunks restricted_chunks;
    bool recounted = false;

    Chunks to_emit;
    size_t next_to_emit = 0;

    void closeInputs()
    {
        for (auto & input : inputs)
            input.close();
    }

    static size_t minValues(size_t top) { return std::max<size_t>(100, 10 * top); }

    void startPass(Pass next, QueryPlan plan)
    {
        auto builder = plan.buildQueryPipeline(
            QueryPlanOptimizationSettings(templ->context), BuildQueryPipelineSettings(templ->context), /*do_optimize=*/ false);
        QueryPlanResourceHolder pass_resources;
        auto pipe = QueryPipelineBuilder::getPipe(std::move(*builder), pass_resources);
        pipe.resize(1);
        new_output = pipe.getOutputPort(0);
        new_processors = Pipe::detachProcessors(std::move(pipe));
        resources.push_back(std::move(pass_resources));
        if (rows_before_limit && (next == Pass::Restricted || next == Pass::Final))
        {
            auto counter = std::make_shared<RowsBeforeStepCounter>();
            for (const auto & processor : new_processors)
                if (typeid_cast<PartialSortingTransform *>(processor.get()))
                    processor->setRowsBeforeLimitCounter(counter);
            (next == Pass::Restricted ? restricted_rows_before_limit : final_rows_before_limit) = std::move(counter);
        }
        if (current && current != &inputs.front())
            to_close = current;
        pass = next;
        stage = next == Pass::Final ? Stage::Forwarding : Stage::Collecting;
        ProfileEvents::increment(ProfileEvents::TwoPassCountTopKPasses);
    }

    void fallback(const char * reason)
    {
        LOG_DEBUG(log, "Running the original plan: {}", reason);
        ProfileEvents::increment(ProfileEvents::TwoPassCountTopKFallbacks);
        if (current && current != &inputs.front())
            to_close = current;
        current = &inputs.front();
        stage = Stage::Forwarding;
    }

    void emitRestricted()
    {
        if (restricted_rows_before_limit)
            rows_before_limit->add(restricted_rows_before_limit->get());
        close_original = true;
        UInt64 to_skip = templ->offset;
        UInt64 to_take = templ->limit;
        for (auto & chunk : restricted_chunks)
        {
            if (to_take == 0)
                break;
            const size_t rows = chunk.getNumRows();
            if (to_skip >= rows)
            {
                to_skip -= rows;
                continue;
            }
            const size_t begin = to_skip;
            const size_t length = std::min<UInt64>(rows - begin, to_take);
            to_skip = 0;
            to_take -= length;
            Columns columns;
            for (const auto & column : chunk.getColumns())
                columns.push_back(column->cut(begin, length));
            to_emit.emplace_back(std::move(columns), length);
        }
        stage = Stage::Emitting;
    }

    void consume(Chunk chunk)
    {
        const size_t rows = chunk.getNumRows();
        if (rows == 0)
            return;
        switch (pass)
        {
            case Pass::KeyPilot:
            {
                const auto & counts = *chunk.getColumns()[0];
                for (size_t row = 0; row < rows; ++row)
                {
                    const UInt64 count = counts.getUInt(row);
                    sample_rows += count;
                    if (sample_key_counts.size() < max_sampled_values)
                        sample_key_counts.push_back(count);
                    else
                        too_many_sampled_values = true;
                }
                break;
            }
            case Pass::GroupPilot:
            {
                const auto & counts = *chunk.getColumns()[0];
                for (size_t row = 0; row < rows; ++row)
                    sample_group_counts.push_back(counts.getUInt(row));
                break;
            }
            case Pass::Counts:
            {
                if (too_many_values)
                    break;
                const auto & keys = *chunk.getColumns()[0];
                const auto & counts = *chunk.getColumns()[1];
                for (size_t row = 0; row < rows; ++row)
                    counted.emplace_back(counts.getUInt(row), keys.get64(row));
                if (counted.size() > max_candidates)
                {
                    too_many_values = true;
                    to_close = current;
                    stage = Stage::Deciding;
                }
                break;
            }
            case Pass::Restricted:
                restricted_chunks.push_back(std::move(chunk));
                break;
            case Pass::Final:
                break;
        }
    }

    void decide()
    {
        switch (pass)
        {
            case Pass::KeyPilot:
                decideAfterKeyPilot();
                break;
            case Pass::GroupPilot:
                decideAfterGroupPilot();
                break;
            case Pass::Counts:
                if (recounted)
                    decideAfterRecount();
                else
                    decideAfterCounts();
                break;
            case Pass::Restricted:
                decideAfterRestricted();
                break;
            case Pass::Final:
                break;
        }
    }

    void decideAfterKeyPilot()
    {
        const size_t min_values = minValues(templ->top());
        const UInt64 min_sample_rows = templ->min_rows / min_sample_rows_divisor;
        const double rows = static_cast<double>(sample_rows);
        auto key_counts = std::move(sample_key_counts);
        sample_key_counts = {};

        UInt64 top_value_rows = 0;
        if (key_counts.size() > min_values)
            std::nth_element(key_counts.begin(), key_counts.begin() + (min_values - 1), key_counts.end(), std::greater<>());
        for (size_t i = 0; i < std::min(min_values, key_counts.size()); ++i)
            top_value_rows += key_counts[i];
        if (key_counts.size() >= min_values)
            sample_theta = *std::min_element(key_counts.begin(), key_counts.begin() + min_values);

        if (too_many_sampled_values || sample_rows < std::max<UInt64>(min_sample_rows, 1)
            || static_cast<double>(key_counts.size()) > max_unique_share * rows || static_cast<double>(top_value_rows) > max_top_share * rows)
        {
            LOG_DEBUG(log, "Pilot: {} sampled rows, {} values of {}, the {} most frequent hold {} rows",
                sample_rows, key_counts.size(), templ->key_name, min_values, top_value_rows);
            fallback("the key does not prune enough rows");
            return;
        }
        startPass(Pass::GroupPilot, makePilotPlan(*templ, /*groups=*/ true));
    }

    void decideAfterGroupPilot()
    {
        const size_t top = templ->top();
        const size_t min_values = minValues(top);
        if (sample_group_counts.size() >= top)
        {
            std::nth_element(sample_group_counts.begin(), sample_group_counts.begin() + (top - 1), sample_group_counts.end(), std::greater<>());
            bound = sample_group_counts[top - 1];
        }
        sample_group_counts = {};
        if (bound < min_lower_bound)
        {
            LOG_DEBUG(log, "Pilot: the sampled count of group {} is {} ({} sampled rows)", top, bound, sample_rows);
            fallback("the result groups are small");
            return;
        }

        if (bound >= sample_theta && templ->key_prunes_granules)
            fetched = 2 * min_values;
        startPass(Pass::Counts, makeCountsPlan(*templ, fetched, bound));
    }

    void sortCounted()
    {
        std::sort(counted.begin(), counted.end(), [](const auto & lhs, const auto & rhs)
        {
            return lhs.first > rhs.first || (lhs.first == rhs.first && lhs.second < rhs.second);
        });
    }

    std::vector<UInt64> firstKeys(size_t count) const
    {
        std::vector<UInt64> keys;
        keys.reserve(count);
        for (size_t i = 0; i < count; ++i)
            keys.push_back(counted[i].second);
        return keys;
    }

    void decideAfterCounts()
    {
        if (too_many_values || counted.empty())
        {
            fallback(counted.empty() ? "no values of the key" : "too many frequent values of the key");
            return;
        }
        sortCounted();
        if (fetched)
            complete_from = counted.size() < *fetched ? 0 : counted.back().first + 1;
        else
            complete_from = bound;

        num_values = std::min(minValues(templ->top()), counted.size());
        if (!templ->key_prunes_granules)
        {
            /// Every pass reads all granules, so a larger set costs little and avoids a second pass over the other columns.
            const UInt64 max_rows = templ->read->getAnalyzedResult()->selected_rows / unpruned_rows_divisor;
            UInt64 rows = 0;
            for (size_t i = 0; i < num_values; ++i)
                rows += counted[i].first;
            while (num_values < counted.size() && rows + counted[num_values].first <= max_rows)
                rows += counted[num_values++].first;
        }
        theta = complete_from;
        if (num_values < counted.size())
        {
            theta = counted[num_values - 1].first;
            while (num_values < counted.size() && counted[num_values].first == theta)
                ++num_values;
            if (num_values == counted.size())
                theta = complete_from;
        }
        startPass(Pass::Restricted, makeRestrictedPlan(*templ, firstKeys(num_values), templ->top(), 0));
    }

    UInt64 restrictedCount() const
    {
        const size_t top = templ->top();
        size_t seen = 0;
        for (const auto & chunk : restricted_chunks)
        {
            if (seen + chunk.getNumRows() >= top)
                return chunk.getColumns()[templ->count_position]->getUInt(top - seen - 1);
            seen += chunk.getNumRows();
        }
        return 0;
    }

    void decideAfterRestricted()
    {
        const UInt64 restricted_count = restrictedCount();
        LOG_DEBUG(log, "Restricted to {} values of {} (theta {}): count of row {} is {}; pilot bound {}, {} values counted (complete from {})",
            num_values, templ->key_name, theta, templ->top(), restricted_count, bound, counted.size(), complete_from);

        if (restricted_count >= theta)
        {
            emitRestricted();
            return;
        }

        const UInt64 lower = std::max(bound, restricted_count);
        if (lower < complete_from)
        {
            bound = lower;
            counted.clear();
            fetched.reset();
            recounted = true;
            startPass(Pass::Counts, makeCountsPlan(*templ, {}, lower));
            return;
        }
        finish(lower);
    }

    void decideAfterRecount()
    {
        if (too_many_values)
        {
            fallback("too many frequent values of the key");
            return;
        }
        sortCounted();
        complete_from = bound;
        finish(bound);
    }

    void finish(UInt64 lower)
    {
        size_t num_candidates = 0;
        while (num_candidates < counted.size() && counted[num_candidates].first >= lower)
            ++num_candidates;
        LOG_DEBUG(log, "{} values of {} with at least {} rows", num_candidates, templ->key_name, lower);
        if (num_candidates == 0 || num_candidates > max_candidates)
        {
            fallback("too many candidate values of the key");
            return;
        }
        if (num_candidates <= num_values)
        {
            emitRestricted();
            return;
        }
        close_original = true;
        startPass(Pass::Final, makeRestrictedPlan(*templ, firstKeys(num_candidates), templ->limit, templ->offset));
    }
};

class TwoPassCountTopKStep final : public ITransformingStep
{
public:
    TwoPassCountTopKStep(const SharedHeader & header, TwoPassTemplatePtr templ_)
        : ITransformingStep(
            header,
            header,
            {
                .data_stream_traits = {.returns_single_stream = true, .preserves_number_of_streams = false, .preserves_sorting = false},
                .transform_traits = {.preserves_number_of_rows = false},
            })
        , templ(std::move(templ_))
    {
    }

    String getName() const override { return "TwoPassCountTopK"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.resize(1);
        pipeline.addTransform(std::make_shared<TwoPassCountTopKTransform>(pipeline.getSharedHeader(), templ));
    }

    void describeActions(FormatSettings & settings) const override
    {
        const String & prefix = settings.detail_prefix;
        settings.out << prefix << "Bounding key: " << templ->key_name << '\n';
        settings.out << prefix << "Limit: " << templ->limit << '\n';
        settings.out << prefix << "Offset: " << templ->offset << '\n';
    }

    QueryPlanStepPtr clone() const override { return std::make_unique<TwoPassCountTopKStep>(input_headers.front(), templ); }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    TwoPassTemplatePtr templ;
};

const ActionsDAG & expressionOf(const IQueryPlanStep & step)
{
    if (const auto * expression = typeid_cast<const ExpressionStep *>(&step))
        return expression->getExpression();
    return typeid_cast<const FilterStep &>(step).getExpression();
}

bool isBoundingKeyType(const DataTypePtr & type)
{
    WhichDataType which(type);
    return which.isInt32() || which.isUInt32() || which.isInt64() || which.isUInt64();
}

bool isStringType(const DataTypePtr & type)
{
    WhichDataType which(removeNullable(removeLowCardinality(type)));
    return which.isString() || which.isFixedString();
}

std::optional<String> passedThrough(const ActionsDAG & dag, const String & name)
{
    const auto * node = dag.tryFindInOutputs(name);
    if (!node)
        return {};
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};
    return node->result_name;
}

/// Every pass evaluates the expressions again over other granules and blocks: they must give the same values.
bool isRepeatable(const ActionsDAG & dag)
{
    if (isSensitiveToEvaluationCount(dag) || dag.hasArrayJoin())
        return false;
    for (const auto & node : dag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
            continue;
        const IColumn * column = node.column.get();
        if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
            column = &column_const->getDataColumn();
        if (const auto * column_set = typeid_cast<const ColumnSet *>(column))
            if (!typeid_cast<const FutureSetFromTuple *>(column_set->getData().get()))
                return false;
    }
    return true;
}

bool readsNarrowColumns(const ActionsDAG & dag, const String & filter_column)
{
    const auto * filter = dag.tryFindInOutputs(filter_column);
    if (!filter)
        return false;
    auto sub_dag = ActionsDAG::cloneSubDAG({filter}, /*remove_aliases=*/ false);
    for (const auto * input : sub_dag.getInputs())
    {
        auto type = removeNullable(input->result_type);
        if (!type->haveMaximumSizeOfValue() || type->getMaximumSizeOfValueInMemory() > 16)
            return false;
    }
    return true;
}

/// A part written before ALTER ADD COLUMN lacks the column: the reader computes it from its DEFAULT or MATERIALIZED expression with
/// a DAG it builds again for every block of every pass (IMergeTreeReader::evaluateMissingDefaults), so even now() can change.
/// Build that DAG the same way for the columns some selected part lacks, and require it to be deterministic. The reader also applies
/// pending mutations on the fly.
bool readerExpressionsAreRepeatable(const ReadFromMergeTree & read, const RangesInDataParts & parts)
{
    if (const auto & mutations = read.getMutationsSnapshot(); mutations && mutations->hasDataMutations())
        return false;
    const auto & metadata = read.getStorageMetadata();
    auto columns = metadata->getColumns();
    for (const auto & column : metadata->virtuals)
        if (column.default_desc.expression)
            columns.add(column);
    if (!columns.hasDefaults())
        return true;

    auto computed = [&](const String & name)
    {
        if (!columns.getDefault(name))
            return false;
        auto type = columns.get(name).type;
        return std::ranges::any_of(parts, [&](const RangesInDataPart & part)
        {
            auto in_part = part.data_part->tryGetColumn(name);
            return !in_part || !in_part->type->equals(*type);
        });
    };
    Names names = read.getAllColumnNames();
    if (const auto & prewhere = read.getPrewhereInfo())
        for (const auto * input : prewhere->prewhere_actions.getInputs())
            names.push_back(input->result_name);
    if (const auto & row_level_filter = read.getRowLevelFilter())
        for (const auto * input : row_level_filter->actions.getInputs())
            names.push_back(input->result_name);
    NamesAndTypesList required;
    for (const auto & name : names)
    {
        auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);
        if (column && computed(column->getNameInStorage()) && !required.contains(column->getNameInStorage()))
            required.emplace_back(column->getNameInStorage(), column->getTypeInStorage());
    }
    if (required.empty())
        return true;

    /// The columns every selected part has are read; the defaults of the others are computed recursively, as for the reader.
    Block header;
    for (const auto & column : columns.getAll())
        if (!computed(column.name))
            header.insert({column.type->createColumn(), column.type, column.name});
    auto context = Context::createCopy(read.getContext());
    enableAllExperimentalSettings(context);
    try
    {
        auto dag = evaluateMissingDefaults(header, required, columns, context);
        return !dag || (isRepeatable(*dag) && !dag->hasNonDeterministic() && !dag->hasStatefulFunctions());
    }
    catch (const Exception &)
    {
        return false;
    }
}

bool hasReservedName(const ActionsDAG & dag)
{
    return std::ranges::any_of(dag.getNodes(), [](const auto & node) { return node.result_name.contains(reserved_name_prefix); });
}

/// The passes read more rows, and possibly more bytes, than the original plan: a limit or a throttle on the read volume, of
/// any level of the query, or a quota on it would act on a different amount than without the optimization.
bool hasReadVolumeLimits(const ReadFromMergeTree & read, const ContextPtr & context)
{
    if (const auto & storage_limits = read.getQueryInfo().storage_limits)
    {
        for (const auto & limits : *storage_limits)
        {
            const auto & local = limits.local_limits;
            const auto & speed = local.speed_limits;
            if (local.size_limits.hasLimits() || limits.leaf_limits.hasLimits() || local.timeout_overflow_mode == OverflowMode::BREAK
                || speed.min_execution_rps || speed.max_execution_rps || speed.min_execution_bps || speed.max_execution_bps
                || speed.max_estimated_execution_time.totalMicroseconds() != 0)
                return true;
        }
    }
    if (const auto quota = context->getQuota())
        for (const auto & usage : quota->getAllUsage())
            for (const auto & interval : usage.intervals)
                if (interval.max[static_cast<size_t>(QuotaType::READ_ROWS)] || interval.max[static_cast<size_t>(QuotaType::READ_BYTES)])
                    return true;
    return false;
}

bool tryApply(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * limit = typeid_cast<LimitStep *>(node.step.get());
    if (!limit || node.children.size() != 1 || limit->withTies() || limit->alwaysReadTillEnd() || limit->getLimit() == 0
        || limit->getLimit() > max_top || limit->getOffset() > max_top || limit->getLimit() + limit->getOffset() > max_top)
        return false;

    auto * sorting_node = node.children.front();
    auto * sorting = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting || sorting->getType() != SortingStep::Type::Full || sorting_node->children.size() != 1)
        return false;
    const auto & description = sorting->getSortDescription();
    if (description.empty() || description.front().direction != -1 || description.front().with_fill)
        return false;

    std::vector<QueryPlan::Node *> post_nodes;
    auto * current = sorting_node->children.front();
    while (typeid_cast<ExpressionStep *>(current->step.get()))
    {
        if (current->children.size() != 1)
            return false;
        post_nodes.push_back(current);
        current = current->children.front();
    }

    auto * aggregating_node = current;
    auto * aggregating = typeid_cast<AggregatingStep *>(aggregating_node->step.get());
    if (!aggregating || aggregating_node->children.size() != 1 || !aggregating->isFinal() || aggregating->isGroupingSets()
        || aggregating->inOrder() || aggregating->isMergingSkipped() || aggregating->explicitSortingRequired())
        return false;
    const auto & params = aggregating->getParams();
    if (params.keys.size() < 2 || params.overflow_row || params.max_rows_to_group_by || params.only_merge || params.top_k)
        return false;

    std::vector<QueryPlan::Node *> pre_nodes;
    current = aggregating_node->children.front();
    while (typeid_cast<ExpressionStep *>(current->step.get()) || typeid_cast<FilterStep *>(current->step.get()))
    {
        if (current->children.size() != 1)
            return false;
        pre_nodes.push_back(current);
        current = current->children.front();
    }

    auto * read = typeid_cast<ReadFromMergeTree *>(current->step.get());
    if (!read || !current->children.empty())
        return false;

    const auto & context = read->getContext();
    const auto & settings = context->getSettingsRef();

    if (settings[Setting::max_rows_to_group_by] || settings[Setting::max_rows_to_sort] || settings[Setting::max_bytes_to_sort]
        || settings[Setting::rows_before_aggregation]
        || settings[Setting::limit] != 0 || settings[Setting::offset] != 0 || context->canUseParallelReplicasOnInitiator()
        || hasReadVolumeLimits(*read, context))
        return false;
    if (settings[Setting::force_primary_key] || settings[Setting::force_index_by_date] || !settings[Setting::force_data_skipping_indexes].value.empty())
        return false;

    if (read->isQueryWithFinal() || read->isQueryWithSampling() || read->readsInOrder() || !read->getIndexReadTasks().empty()
        || read->isVectorColumnReplaced() || read->isSelectedForTopKFilterOptimization() || read->isParallelReplicasLocalPlanForInitiator()
        || read->isParallelReplicasLocalPlanForFollower() || read->getParallelReadingExtension())
        return false;
    /// A STREAM read selects its parts while reading, a masking policy hides the values the primary index holds, and the delete
    /// bitmap of a UNIQUE KEY is outside the parts the passes share.
    if (read->getQueryInfo().isStream() || read->getMergeTreeData().hasEnabledMaskingPolicies(context)
        || read->getStorageMetadata()->hasUniqueKey())
        return false;

    String count_name = description.front().column_name;
    for (auto * post : post_nodes)
    {
        const auto & dag = typeid_cast<ExpressionStep &>(*post->step).getExpression();
        if (!isRepeatable(dag))
            return false;
        auto input = passedThrough(dag, count_name);
        if (!input)
            return false;
        count_name = *input;
    }
    std::optional<size_t> count_aggregate;
    for (size_t i = 0; i < params.aggregates.size(); ++i)
    {
        const auto & aggregate = params.aggregates[i];
        if (aggregate.column_name == count_name && aggregate.function->getName() == "count" && aggregate.argument_names.empty()
            && aggregate.parameters.empty())
            count_aggregate = i;
    }
    if (!count_aggregate)
        return false;

    for (auto * pre : pre_nodes)
    {
        if (const auto * filter = typeid_cast<FilterStep *>(pre->step.get()))
        {
            if (!isRepeatable(filter->getExpression()) || !readsNarrowColumns(filter->getExpression(), filter->getFilterColumnName()))
                return false;
        }
        else if (!isRepeatable(typeid_cast<ExpressionStep &>(*pre->step).getExpression()))
            return false;
    }
    const auto & prewhere = read->getPrewhereInfo();
    if (prewhere && (!isRepeatable(prewhere->prewhere_actions) || !readsNarrowColumns(prewhere->prewhere_actions, prewhere->prewhere_column_name)))
        return false;
    const auto & row_level_filter = read->getRowLevelFilter();
    if (row_level_filter && (!isRepeatable(row_level_filter->actions) || !readsNarrowColumns(row_level_filter->actions, row_level_filter->column_name)))
        return false;

    auto is_reserved = [](const String & name) { return name.contains(reserved_name_prefix); };
    if (std::ranges::any_of(read->getOutputHeader()->getNames(), is_reserved) || std::ranges::any_of(params.keys, is_reserved)
        || std::ranges::any_of(params.aggregates, [&](const auto & aggregate) { return is_reserved(aggregate.column_name); })
        || (prewhere && hasReservedName(prewhere->prewhere_actions)) || (row_level_filter && hasReservedName(row_level_filter->actions))
        || std::ranges::any_of(pre_nodes, [](const auto * pre) { return hasReservedName(expressionOf(*pre->step)); })
        || std::ranges::any_of(post_nodes, [](const auto * post) { return hasReservedName(expressionOf(*post->step)); }))
        return false;

    const auto & aggregation_input = aggregating->getInputHeaders().front();
    String key_name;
    String key_column;
    DataTypePtr key_type;
    bool has_string_key = false;
    const auto & read_columns = read->getAllColumnNames();
    for (const auto & key : params.keys)
    {
        const auto * column = aggregation_input->findByName(key);
        if (!column)
            return false;
        if (isStringType(column->type))
            has_string_key = true;
        if (!key_name.empty() || !isBoundingKeyType(column->type))
            continue;

        std::optional<String> name = key;
        for (auto * pre : pre_nodes)
        {
            const auto & dag = expressionOf(*pre->step);
            name = passedThrough(dag, *name);
            if (!name)
                break;
        }
        if (!name || std::ranges::find(read_columns, *name) == read_columns.end())
            continue;
        if (prewhere)
            if (const auto * computed = prewhere->prewhere_actions.tryFindInOutputs(*name); computed && computed->type != ActionsDAG::ActionType::INPUT)
                continue;
        if (row_level_filter)
            if (const auto * computed = row_level_filter->actions.tryFindInOutputs(*name); computed && computed->type != ActionsDAG::ActionType::INPUT)
                continue;
        const auto * read_column = read->getOutputHeader()->findByName(*name);
        if (!read_column || !read_column->type->equals(*column->type))
            continue;
        key_name = key;
        key_column = *name;
        key_type = column->type;
    }
    if (key_name.empty() || !has_string_key)
        return false;

    auto analysis = read->getAnalyzedResult();
    if (!analysis)
    {
        analysis = read->selectRangesToRead();
        read->setAnalyzedResult(analysis);
    }
    const UInt64 min_rows = optimization_settings.two_pass_count_top_k_min_rows;
    if (!analysis || !analysis->isUsable() || analysis->readFromProjection() || analysis->selected_rows < min_rows
        || !readerExpressionsAreRepeatable(*read, analysis->parts_with_ranges))
        return false;

    const auto & output_header = limit->getOutputHeader();
    const auto * count_output = output_header->findByName(description.front().column_name);
    if (!count_output)
        return false;

    auto templ = std::make_shared<TwoPassTemplate>();
    templ->read = cloneTyped(*read);
    templ->read->adoptFiltersFrom(*read);
    for (auto it = pre_nodes.rbegin(); it != pre_nodes.rend(); ++it)
        templ->pre_steps.push_back((*it)->step->clone());
    templ->aggregating = cloneTyped(*aggregating);
    for (auto it = post_nodes.rbegin(); it != post_nodes.rend(); ++it)
        templ->post_steps.push_back((*it)->step->clone());
    templ->sorting = cloneTyped(*sorting);
    templ->limit = limit->getLimit();
    templ->offset = limit->getOffset();
    templ->key_name = key_name;
    templ->key_column = key_column;
    templ->key_type = key_type;
    const auto metadata = read->getStorageMetadata();
    for (const auto & names : {metadata->getColumnsRequiredForPrimaryKey(), metadata->getColumnsRequiredForPartitionKey()})
        templ->key_prunes_granules |= std::ranges::find(names, key_column) != names.end();
    templ->count_aggregate = *count_aggregate;
    templ->count_position = output_header->getPositionByName(description.front().column_name);
    templ->min_rows = min_rows;
    templ->context = context;

    if (!addToPrewhere(prewhere, makeInFilter(*templ, makeSet(*templ, {})), *templ, *read->getOutputHeader()))
        return false;

    auto & original = nodes.emplace_back(QueryPlan::Node{.step = std::move(node.step), .children = std::move(node.children)});
    node.step = std::make_unique<TwoPassCountTopKStep>(original.step->getOutputHeader(), std::move(templ));
    node.step->setStepDescription("ORDER BY count() DESC LIMIT in passes");
    node.children = {&original};
    return true;
}

}

/// Runs `ORDER BY count() DESC LIMIT n` over a multi-key GROUP BY in passes over the rows of the most frequent values of
/// an integer key: see `TwoPassCountTopKTransform`. The step keeps the optimized plan below the LIMIT as the fallback.
void optimizeTwoPassCountTopK(QueryPlan::Node & root, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (optimization_settings.make_distributed_plan || optimization_settings.serialize_query_plan)
        return;

    Stack stack;
    stack.push_back({.node = &root});
    while (!stack.empty())
    {
        auto & frame = stack.back();
        if (frame.next_child == 0)
        {
            const bool below_two_pass = stack.size() >= 2 && typeid_cast<TwoPassCountTopKStep *>(stack[stack.size() - 2].node->step.get());
            if (!below_two_pass && !typeid_cast<TwoPassCountTopKStep *>(frame.node->step.get()) && tryApply(*frame.node, nodes, optimization_settings))
            {
                stack.pop_back();
                continue;
            }
        }
        if (frame.next_child < frame.node->children.size())
        {
            auto * child = frame.node->children[frame.next_child];
            ++frame.next_child;
            stack.push_back({.node = child});
            continue;
        }
        stack.pop_back();
    }
}

}

bool isTwoPassCountTopKTransform(const IProcessor & processor)
{
    return typeid_cast<const QueryPlanOptimizations::TwoPassCountTopKTransform *>(&processor) != nullptr;
}

}
