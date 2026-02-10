#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/ReadFromSystemPrimesStep.h>
#include <Processors/QueryPlan/numbersLikeUtils.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromPrimes.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/System/StorageSystemPrimes.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

using Interval = std::pair<UInt64, UInt64>; /// inclusive [l..r]

void addNullSource(Pipe & pipe, const std::string & column_name)
{
    pipe.addSource(std::make_shared<NullSource>(PrimesSource::createHeader(column_name)));
}

std::vector<Range> intersectWithPrimesDomain(const Ranges & ranges)
{
    const Range primes_domain(FieldRef(UInt64(2)), true, FieldRef(std::numeric_limits<UInt64>::max()), true);

    std::vector<Range> intersections;
    intersections.reserve(ranges.size());

    for (const auto & range : ranges)
        if (auto intersection = primes_domain.intersectWith(range))
            intersections.push_back(*intersection);

    return intersections;
}

/// We can keep using `Range` however it is very slow, and inside `SourceFromPrimes`, we will doing
/// lots of ranges access. So, we convert to lightweight `Intervals`.
std::vector<Interval> rangesToIntervals(const std::vector<Range> & ranges)
{
    std::vector<Interval> intervals;
    intervals.reserve(ranges.size());

    for (const auto & range : ranges)
    {
        UInt64 l = range.left.safeGet<UInt64>();
        if (!range.left_included)
        {
            if (l == std::numeric_limits<UInt64>::max())
                continue;
            ++l;
        }

        UInt64 r = range.right.safeGet<UInt64>();
        if (!range.right_included)
        {
            if (r == 0)
                continue;
            --r;
        }

        l = std::max<UInt64>(l, 2);
        if (l > r)
            continue;

        intervals.emplace_back(l, r);
    }

    return intervals;
}

/// Maybe this part not needed if KeyCondition already simplifies the intervals.
/// But let's keep it regardless to be safe from future changes to other components.
void mergeIntervals(std::vector<Interval> & intervals)
{
    if (intervals.empty())
        return;

    std::sort(intervals.begin(), intervals.end());

    size_t merged_intervals_size = 0;
    for (size_t i = 0; i < intervals.size(); ++i)
    {
        if (merged_intervals_size == 0)
        {
            intervals[merged_intervals_size++] = intervals[i];
            continue;
        }

        auto & prev = intervals[merged_intervals_size - 1];
        const auto & cur = intervals[i];

        const UInt64 prev_r = prev.second;

        /// We need to be careful otherwise prev_r + 1 may overflow
        /// Additionally, if prev_r == max, everything overlaps with it
        const bool disjoint = (prev_r != std::numeric_limits<UInt64>::max()) && (cur.first > prev_r + 1);

        if (disjoint)
            intervals[merged_intervals_size++] = cur;
        else
            prev.second = std::max(prev.second, cur.second);
    }

    intervals.resize(merged_intervals_size);
}

}

ReadFromSystemPrimesStep::ReadFromSystemPrimesStep(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    StoragePtr storage_,
    size_t max_block_size_)
    : SourceStepWithFilter(
          std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names_)),
          column_names_,
          query_info_,
          storage_snapshot_,
          context_)
    , column_names{column_names_}
    , storage{std::move(storage_)}
    , key_expression{KeyDescription::parse(column_names[0], storage_snapshot->metadata->columns, context, false).expression}
    , max_block_size{max_block_size_}
    , storage_limits(query_info_.storage_limits)
{
    storage_snapshot->check(column_names);
    if (column_names.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SystemPrimes supports exactly one column");

    if (storage->as<StorageSystemPrimes>() == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ReadFromSystemPrimesStep expects StorageSystemPrimes");

    /// In case we are able to push the limit down, we store it now. We can only safely push the limit down in the following two cases:
    ///     - No filter
    ///     - Filter only contains plain ranges. Like, (1 < x < 10) and (5 <= x < 50) or (x < 200)
    limit = NumbersLikeUtils::getLimitFromQueryInfo(query_info_, context_);
}

void ReadFromSystemPrimesStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    if (pipe.empty())
    {
        chassert(output_header != nullptr);
        pipe = Pipe(std::make_shared<NullSource>(output_header));
    }

    /// Add storage limits
    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    /// Add to processors to get processor info through explain pipeline statement
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

QueryPlanStepPtr ReadFromSystemPrimesStep::clone() const
{
    return std::make_unique<ReadFromSystemPrimesStep>(
        column_names, getQueryInfo(), getStorageSnapshot(), getContext(), storage, max_block_size);
}

Pipe ReadFromSystemPrimesStep::makePipe()
{
    auto & primes_storage = storage->as<StorageSystemPrimes &>();
    chassert(primes_storage.step != 0);

    const bool has_filter = (filter_actions_dag != nullptr);

    std::optional<UInt64> effective_limit = primes_storage.limit;

    /// This pushes down the query limit
    auto apply_query_limit = [&]() -> void
    {
        if (!limit)
            return;

        const UInt64 query_limit = static_cast<UInt64>(*limit);
        effective_limit = std::min(effective_limit.value_or(query_limit), query_limit);
    };

    Pipe pipe;

    /// In this case we know which values to process (all the values from start); so, we can safely use query limit right away
    if (!has_filter)
        apply_query_limit();

    /// LIMIT 0 case
    if (effective_limit && *effective_limit == 0)
    {
        addNullSource(pipe, primes_storage.column_name);
        return pipe;
    }

    if (has_filter)
    {
        Ranges extracted_ranges;

        ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag ? filter_actions_dag->getOutputs().front() : nullptr, context);

        KeyCondition condition(inverted_dag, context, column_names, key_expression);

        /// This implies the entire filter is just composed of simple ranges like (a < x < b)
        if (condition.extractPlainRanges(extracted_ranges))
        {
            /// In this path, we exactly know which values to process; so, we can safely use query limit right away.
            apply_query_limit();

            if (effective_limit)
                NumbersLikeUtils::checkLimits(context->getSettingsRef(), static_cast<size_t>(*effective_limit));

            std::vector<Range> intersected = intersectWithPrimesDomain(extracted_ranges);

            if (intersected.empty())
            {
                addNullSource(pipe, primes_storage.column_name);
                return pipe;
            }

            /// It is cheaper to work with Intervals in SourceFromPrimes than Ranges
            /// Intervals are just pairs of UInt64
            std::vector<Interval> intervals = rangesToIntervals(intersected);
            if (intervals.empty())
            {
                addNullSource(pipe, primes_storage.column_name);
                return pipe;
            }

            mergeIntervals(intervals);

            /// Fast value-range sieve only simple ranges
            /// SELECT * FROM system.primes WHERE (prime BETWEEN 1000000000000 AND 1000000001000) OR (prime BETWEEN 2000000000000 AND 2000000001000)
            if (primes_storage.offset == 0 && primes_storage.step == 1)
            {
                /// SELECT * FROM primes(100) WHERE (prime BETWEEN 2 AND 10) OR (prime BETWEEN 20 AND 100)
                /// SELECT * FROM system.primes WHERE (prime BETWEEN 2 AND 10) OR (prime BETWEEN 20 AND 100)
                auto source = std::make_shared<PrimesSimpleRangedSource>(
                    static_cast<UInt64>(max_block_size), std::move(intervals), effective_limit, primes_storage.column_name);

                if (effective_limit)
                    source->addTotalRowsApprox(*effective_limit);

                pipe.addSource(std::move(source));
                return pipe;
            }

            /// This is the general ranged source with non-zero offset and step size
            /// SELECT * FROM primes(5, 100, 2) WHERE (prime BETWEEN 2 AND 10) OR (prime BETWEEN 20 AND 100)
            auto source = std::make_shared<PrimesRangedSource>(
                static_cast<UInt64>(max_block_size),
                std::move(intervals),
                primes_storage.offset,
                effective_limit,
                primes_storage.step,
                primes_storage.column_name);

            if (effective_limit)
                source->addTotalRowsApprox(*effective_limit);

            pipe.addSource(std::move(source));
            return pipe;
        }
    }

    if (effective_limit)
        NumbersLikeUtils::checkLimits(context->getSettingsRef(), static_cast<size_t>(*effective_limit));

    /// General case: ignore the first `primes_storage.offset` primes, then take every `primes_storage.step`-th prime until we have
    /// taken `effective_limit` primes.
    auto source = std::make_shared<PrimesSource>(
        static_cast<UInt64>(max_block_size), primes_storage.offset, effective_limit, primes_storage.step, primes_storage.column_name);

    if (effective_limit)
        source->addTotalRowsApprox(*effective_limit);

    pipe.addSource(std::move(source));
    return pipe;
}

}
