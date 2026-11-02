#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopK.h>

#include <Access/Common/QuotaDefs.h>
#include <Access/EnabledQuota.h>
#include <Access/QuotaUsage.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/BoundingKey.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopKImpl.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Pilot.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB
{

namespace MultiPassTopKImpl
{

namespace
{

/// The fewest granules that the read must select. The pilot and the proof cost about as much on a small input as on a
/// large one, while the aggregation they save grows with the input: below about a thousand granules, 8M rows at the
/// default granule size, the pilot of most queries finds no clear top and only adds its cost.
constexpr size_t min_selected_granules = 1024;

/// The most times that the passes of a run read the input: the sketch pass and the two restricted passes of the
/// bounding mode read it in full at most, after a pilot that reads at most every 32nd granule.
constexpr size_t max_reads_of_input = 4;

/// A limit of bytes must exceed the decoded bytes of the passes this many times. The reads count the bytes of the
/// blocks they read, which the decoded bytes of the columns bound only roughly: every block counts the whole dictionary
/// of a `LowCardinality` column, for example.
constexpr size_t decoded_bytes_margin = 10;

/// Whether a limit of the size of the read holds for the passes as well, which read up to `max_reads_of_input` times
/// its `rows` and its `decoded_bytes`. A limit that breaks the read instead of throwing would cut a pass short.
bool sizeLimitHoldsForPasses(const SizeLimits & limits, size_t rows, std::optional<size_t> decoded_bytes)
{
    if (!limits.hasLimits())
        return true;
    if (limits.overflow_mode != OverflowMode::THROW)
        return false;
    if (limits.max_rows && rows > limits.max_rows / max_reads_of_input)
        return false;
    return !limits.max_bytes
        || (decoded_bytes && *decoded_bytes <= limits.max_bytes / (max_reads_of_input * decoded_bytes_margin));
}

/// Whether the limits of the query hold for the passes as well, which read the input `read` of an aggregation with
/// `params` several times. The passes count fewer groups than the original aggregation, so a limit of the groups must
/// never apply: a group holds at least one row, so the groups never reach a limit of at least the rows of the read. A
/// timeout that breaks the read would cut a pass short. A minimum speed keeps the strategy off as well: the passes read
/// at other speeds than the original read, which the minimum was chosen for. A maximum speed throttles the passes as it
/// throttles the original aggregation, and the estimated execution time takes the rows that every pass reports to read.
/// A quota of the reads keeps the strategy off, as what it leaves is not known in advance.
bool limitsHoldForPasses(const ReadFromMergeTree & read, const Aggregator::Params & params)
{
    const size_t rows = read.getAnalyzedResult()->selected_rows;
    if (params.max_rows_to_group_by && params.max_rows_to_group_by < rows)
        return false;
    if (const auto & storage_limits = read.getQueryInfo().storage_limits)
    {
        /// The estimate looks up the columns of every part, so only a limit of bytes pays for it.
        std::optional<size_t> decoded_bytes;
        if (std::ranges::any_of(*storage_limits, [](const auto & limits)
                { return limits.local_limits.size_limits.max_bytes || limits.leaf_limits.max_bytes; }))
            decoded_bytes = read.estimateBytesToRead(ReadBytesKind::Uncompressed);
        for (const auto & limits : *storage_limits)
        {
            const auto & local = limits.local_limits;
            if (!sizeLimitHoldsForPasses(local.size_limits, rows, decoded_bytes)
                || !sizeLimitHoldsForPasses(limits.leaf_limits, rows, decoded_bytes)
                || local.timeout_overflow_mode == OverflowMode::BREAK || local.speed_limits.min_execution_rps
                || local.speed_limits.min_execution_bps)
                return false;
        }
    }
    if (const auto quota = read.getContext()->getQuota())
        for (const auto & usage : quota->getAllUsage())
            for (const auto & interval : usage.intervals)
                if (interval.max[static_cast<size_t>(QuotaType::READ_ROWS)] || interval.max[static_cast<size_t>(QuotaType::READ_BYTES)])
                    return false;
    return true;
}

}

}

using namespace MultiPassTopKImpl;

const ActionsDAG * expressionOfInputStep(const IQueryPlanStep & step)
{
    if (const auto * expression = typeid_cast<const ExpressionStep *>(&step))
        return &expression->getExpression();
    if (const auto * filter = typeid_cast<const FilterStep *>(&step))
        return &filter->getExpression();
    return nullptr;
}

MultiPassTopKInputPtr makeMultiPassTopKInput(
    const AggregatingStep & aggregating,
    const ReadFromMergeTree & read,
    const std::vector<const IQueryPlanStep *> & input_steps,
    size_t top,
    size_t rank_index)
{
    /// The candidate pass proves the n-th best of its candidates, so they must hold the top.
    if (top > max_candidates || read.getAnalyzedResult()->selected_marks < min_selected_granules)
        return nullptr;
    if (!pilotCanIdentifyGranules(read) || !limitsHoldForPasses(read, aggregating.getParams()))
        return nullptr;

    auto input = std::make_shared<MultiPassTopKInput>();
    input->top = top;
    input->rank_index = rank_index;
    input->read = read.cloneWithContext(read.getContext());
    if (!blocksHaveEqualStructure(*input->read->getOutputHeader(), *read.getOutputHeader()))
        return nullptr;
    input->read->adoptFiltersFrom(read);
    for (const auto * step : input_steps)
        input->input_steps.push_back(step->clone());
    input->context = read.getContext();
    input->bounding_key = findBoundingKey(aggregating, *input);
    return input;
}

std::optional<String> boundingKeyOf(const MultiPassTopKInput & input)
{
    if (!input.bounding_key)
        return {};
    return input.bounding_key->name;
}

size_t rankIndexOf(const MultiPassTopKInput & input)
{
    return input.rank_index;
}

}
