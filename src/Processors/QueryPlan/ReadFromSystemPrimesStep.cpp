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

    Pipe pipe;
    const auto header = PrimesSource::createHeader(primes_storage.column_name);

    auto add_null_source = [&] { NumbersLikeUtils::addNullSource(pipe, header); };

    /// Pushdown rationale:
    /// - Filter pushdown:
    ///   - Bounded sources (`primes(N)` / `primes(offset, length[, step])`) define a prime-index domain.
    ///     Value-space filtering is a later step; applying it during generation would change that domain and alter results.
    ///     Example: `SELECT prime FROM primes(100) WHERE prime > 1000`.
    ///     Correct behavior is to generate `primes(100)` first (the first 100 primes) and then filter, which is empty.
    ///     If we pushed `prime > 1000` into generation, we would switch to a value-domain scan and lose index-domain semantics.
    ///   - Unbounded sources (`primes()` / `system.primes`) have no bounded prime-index domain to preserve.
    ///     Therefore extracted value ranges/bounds can be applied during generation; if none are extractable, we scan normally.
    ///     Example: `SELECT prime FROM primes() WHERE prime BETWEEN 100 AND 130`.
    ///     This is equivalent to post-filtering because the source is unbounded in index space.
    /// - LIMIT/OFFSET pushdown:
    ///   - Safe only when generated rows already satisfy WHERE (no filter, or exact extracted ranges).
    ///     Otherwise pre-filter LIMIT can cut off rows that would match later.
    ///     Example: `SELECT prime FROM system.primes WHERE prime % 10 = 1 LIMIT 5`.
    ///     If LIMIT were pushed first, we would take `[2, 3, 5, 7, 11]`, filter to `[11]`, and return too few rows.
    ///     Correct output is `[11, 31, 41, 61, 71]`.
    ///     With conservative ranges (e.g. `prime % 3 = 1 AND prime < 100`), bounds only restrict generation domain;
    ///     they do not guarantee all generated rows satisfy WHERE, so LIMIT still cannot be pushed down.
    ///
    /// This is the row-limit we pass down to the source.
    /// It starts as the storage/table-function limit (`primes(N)`), and may be additionally capped
    /// by the query LIMIT/OFFSET when it is safe to push that down.
    std::optional<UInt64> effective_limit = primes_storage.limit;

    /// Storage-level LIMIT 0 (e.g. `primes(0)`) is an empty table regardless of the WHERE clause.
    if (effective_limit && *effective_limit == 0)
    {
        add_null_source();
        return pipe;
    }

    /// Cap `effective_limit` by the query LIMIT/OFFSET, but only in paths where doing so is correct.
    /// (The query LIMIT is applied after WHERE; pushing it into the source makes it effectively pre-filter,
    /// so it is only safe when this source already generates only rows that satisfy the filter.)
    auto apply_query_limit = [&] { NumbersLikeUtils::applyQueryLimit(effective_limit, limit); };

    auto intersect_ranges = [&](const Ranges & ranges) -> std::optional<std::vector<Interval>>
    {
        /// Intersect extracted conditions with the primes value domain [2, UInt64::max],
        /// and convert to sorted, non-overlapping inclusive intervals for `SourceFromPrimes` implementations.
        std::vector<Range> intersected = intersectWithPrimesDomain(ranges);
        if (intersected.empty())
            return std::nullopt;

        /// It is cheaper to work with Intervals in SourceFromPrimes than Ranges.
        /// Intervals are just pairs of UInt64.
        std::vector<Interval> intervals = rangesToIntervals(intersected);
        if (intervals.empty())
            return std::nullopt;

        mergeIntervals(intervals);
        return std::move(intervals);
    };

    /// No-filter path: output is exactly the generated prime sequence, so pushing down query LIMIT/OFFSET is safe.
    if (!filter_actions_dag)
    {
        /// Applying query LIMIT/OFFSET at the source doesn't change results (it is applied later anyway),
        /// but it can significantly reduce the amount of generated primes.
        apply_query_limit();

        if (effective_limit)
            NumbersLikeUtils::checkLimits(context->getSettingsRef(), static_cast<size_t>(*effective_limit));

        auto source = std::make_shared<PrimesSource>(
            static_cast<UInt64>(max_block_size), primes_storage.offset, effective_limit, primes_storage.step, primes_storage.column_name);

        if (effective_limit)
            source->addTotalRowsApprox(*effective_limit);

        pipe.addSource(std::move(source));
        return pipe;
    }

    /// Filtered path:
    /// Extract ranges/bounds implied by the WHERE clause.
    ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag->getOutputs().front(), context);
    KeyCondition condition(inverted_dag, context, column_names, key_expression);
    const auto extracted_ranges = NumbersLikeUtils::extractRanges(condition);

    /// `extractRanges()` returns either:
    ///  - exact disjoint ranges that fully represent the filter (safe to push down query LIMIT);
    ///  - conservative bounds/ranges that are safe to intersect with, but may still include values filtered out later.
    ///
    /// An empty `Ranges` means the condition is contradictory (always false).
    if (NumbersLikeUtils::isAlwaysFalse(extracted_ranges.ranges))
    {
        add_null_source();
        return pipe;
    }

    /// Exact disjoint ranges: the WHERE clause can be represented as a union of intervals in value space.
    /// We use this optimization only for unbounded sources (`system.primes` / `primes()`), where applying the value
    /// filter during generation preserves semantics and allows pushing query LIMIT down.
    if (!primes_storage.limit && extracted_ranges.kind == NumbersLikeUtils::ExtractedRanges::Kind::ExactRanges)
    {
        apply_query_limit();

        if (effective_limit)
            NumbersLikeUtils::checkLimits(context->getSettingsRef(), static_cast<size_t>(*effective_limit));

        auto maybe_intervals = intersect_ranges(extracted_ranges.ranges);
        if (!maybe_intervals)
        {
            add_null_source();
            return pipe;
        }

        auto intervals = std::move(*maybe_intervals);

        /// Fast path: offset=0/step=1 means we can use a value-range prime sieve directly.
        if (primes_storage.offset == 0 && primes_storage.step == 1)
        {
            auto source = std::make_shared<PrimesSimpleRangedSource>(
                static_cast<UInt64>(max_block_size), std::move(intervals), effective_limit, primes_storage.column_name);

            if (effective_limit)
                source->addTotalRowsApprox(*effective_limit);

            pipe.addSource(std::move(source));
            return pipe;
        }

        /// General path: apply the prime-index selection (offset/step) and then the value-interval filter.
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

    /// Conservative bounds: when exact extraction fails, we may still get safe value bounds (e.g. from `prime < 100`).
    /// This is useful only for unbounded sources (`system.primes` / `primes()`): without a bound we might generate primes
    /// indefinitely even if the full filter is bounded.
    ///
    /// Query LIMIT is NOT pushed down here: bounds are conservative, so we cannot know how many generated values will
    /// survive the full filter.
    if (!primes_storage.limit && !NumbersLikeUtils::isUniverse(extracted_ranges.ranges))
    {
        auto maybe_intervals = intersect_ranges(extracted_ranges.ranges);
        if (!maybe_intervals)
        {
            add_null_source();
            return pipe;
        }

        auto intervals = std::move(*maybe_intervals);

        if (primes_storage.offset == 0 && primes_storage.step == 1)
        {
            pipe.addSource(
                std::make_shared<PrimesSimpleRangedSource>(
                    static_cast<UInt64>(max_block_size), std::move(intervals), std::nullopt, primes_storage.column_name));
            return pipe;
        }

        pipe.addSource(
            std::make_shared<PrimesRangedSource>(
                static_cast<UInt64>(max_block_size),
                std::move(intervals),
                primes_storage.offset,
                std::nullopt,
                primes_storage.step,
                primes_storage.column_name));
        return pipe;
    }

    if (effective_limit)
        NumbersLikeUtils::checkLimits(context->getSettingsRef(), static_cast<size_t>(*effective_limit));

    /// Fallback: generate primes in prime-index space as defined by storage params and apply the full filter later.
    ///
    /// This path is taken when we cannot safely apply the WHERE predicate during generation, for example:
    ///  - bounded sources (`primes(N)` / `primes(offset, length[, step])`): storage arguments define the table in prime-index space,
    ///    so pushing down value-space predicates would change semantics;
    ///  - unbounded sources where we cannot extract any useful value restriction from the predicate.
    ///
    /// Examples:
    ///  - `SELECT prime FROM primes(10) WHERE prime > 10;` (bounded source + value predicate)
    ///  - `SELECT prime FROM primes(100) WHERE prime % 10 = 1;` (bounded source + non-interval predicate)
    ///  - `SELECT prime FROM system.primes WHERE prime % 10 = 1 LIMIT 1;` (unbounded source + non-interval predicate)
    ///  - `SELECT prime FROM system.primes WHERE bitAnd(prime, prime + 1) = 0 LIMIT 7;` (unbounded source + non-interval predicate)
    auto source = std::make_shared<PrimesSource>(
        static_cast<UInt64>(max_block_size), primes_storage.offset, effective_limit, primes_storage.step, primes_storage.column_name);

    if (effective_limit)
        source->addTotalRowsApprox(*effective_limit);

    pipe.addSource(std::move(source));
    return pipe;
}

}
