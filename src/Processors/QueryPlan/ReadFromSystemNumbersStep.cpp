#include <memory>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Processors/QueryPlan/numbersLikeUtils.h>

#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/LimitTransform.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <base/types.h>
#include <fmt/format.h>
#include <Common/iota.h>
#include <Common/typeid_cast.h>
#include <Core/Types.h>


namespace DB
{
namespace
{

template <iota_supported_types T>
inline void iotaWithStepOptimized(T * begin, size_t count, T first_value, T step)
{
    if (step == 1)
        iota(begin, count, first_value);
    else
        iotaWithStep(begin, count, first_value, step);
}

/// Unbounded numbers generator, used for `system.numbers(_mt)` and `numbers(_mt)()`
/// when we cannot extract useful WHERE bounds and cannot safely push down query LIMIT/OFFSET.
class NumbersSource : public ISource
{
public:
    NumbersSource(
        UInt64 block_size_,
        UInt64 start_,
        const std::string & column_name,
        UInt64 step_between_chunks_)
        : ISource(createHeader(column_name))
        , block_size(block_size_)
        , next(start_)
        , step_between_chunks(step_between_chunks_)
    {
    }
    String getName() const override { return "Numbers"; }

    static SharedHeader createHeader(const std::string & column_name)
    {
        return std::make_shared<const Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), column_name)});
    }

protected:
    Chunk generate() override
    {
        auto column = ColumnUInt64::create(block_size);
        ColumnUInt64::Container & vec = column->getData();

        UInt64 curr = next; /// The local variable for some reason works faster (>20%) than member of class.
        UInt64 * pos = vec.data(); /// This also accelerates the code.

        iota(pos, static_cast<size_t>(block_size), curr);

        next += step_between_chunks;

        progress(column->size(), column->byteSize());
        return {Columns{std::move(column)}, block_size};
    }

private:
    UInt64 block_size;
    UInt64 next;
    UInt64 step_between_chunks;
};

struct RangeWithStep
{
    UInt64 left;    /// first value in the range
    UInt64 step;    /// step between values
    UInt128 size;   /// how many values in this range
};

using RangesWithStep = std::vector<RangeWithStep>;

std::optional<RangeWithStep> steppedRangeFromRange(const Range & r, UInt64 step, UInt64 remainder)
{
    if ((r.right.safeGet<UInt64>() == 0) && (!r.right_included))
        return std::nullopt;
    UInt64 begin = (r.left.safeGet<UInt64>() / step) * step;
    if (begin > std::numeric_limits<UInt64>::max() - remainder)
        return std::nullopt;
    begin += remainder;

    while ((r.left_included <= r.left.safeGet<UInt64>()) && (begin <= r.left.safeGet<UInt64>() - r.left_included))
    {
        if (std::numeric_limits<UInt64>::max() - step < begin)
            return std::nullopt;
        begin += step;
    }

    if ((begin >= r.right_included) && (begin - r.right_included >= r.right.safeGet<UInt64>()))
        return std::nullopt;
    UInt64 right_edge_included = r.right.safeGet<UInt64>() - (1 - r.right_included);
    return std::optional{RangeWithStep{begin, step, static_cast<UInt128>(right_edge_included - begin) / step + 1}};
}

auto sizeOfRanges(const RangesWithStep & rs)
{
    UInt128 total_size{};
    for (const RangeWithStep & r : rs)
    {
        /// total_size will never overflow
        total_size += r.size;
    }
    return total_size;
};

/// Generate numbers according to ranges.
/// Numbers generated is ordered in one stream.
/// Notice that we will not generate additional numbers out of ranges.
class NumbersRangedSource : public ISource
{
public:
    /// Represent a position in `Ranges` list.
    struct RangesPos
    {
        size_t offset_in_ranges; /// which range in the vector
        UInt128 offset_in_range; /// how many values already consumed inside that range
    };

    struct RangesState
    {
        RangesPos pos;
        mutable std::mutex mutex;
    };

    using RangesStatePtr = std::shared_ptr<RangesState>;

    NumbersRangedSource(
        const RangesWithStep & ranges_,
        RangesStatePtr & ranges_state_,
        UInt64 base_block_size_,
        UInt64 step_,
        const std::string & column_name)
        : ISource(NumbersSource::createHeader(column_name))
        , ranges(ranges_)
        , ranges_state(ranges_state_)
        , base_block_size(base_block_size_)
        , step(step_)
    {
    }

    String getName() const override { return "NumbersRange"; }

private:
    /// Find the data range in ranges and return (start position, how many items found).
    /// If no data left in ranges return 0 items found
    std::pair<RangesPos, UInt64> findRanges()
    {
        std::lock_guard lock(ranges_state->mutex);


        UInt64 maximum_can_take = base_block_size;

        /// `start` is the current global position.
        RangesPos start = ranges_state->pos;
        RangesPos end = start;

        /// Find end
        while (maximum_can_take != 0)
        {
            if (end.offset_in_ranges == ranges.size())
                break; /// No ranges left at all


            auto & current_range = ranges[end.offset_in_ranges];

            UInt128 remaining_in_current_range = current_range.size - end.offset_in_range;

            chassert(remaining_in_current_range > 0);

            UInt128 take128 = std::min<UInt128>(remaining_in_current_range, maximum_can_take);

            UInt64 take = static_cast<UInt64>(take128);

            /// Advance 'end' by 'take' elements inside this range.
            chassert(end.offset_in_range + take <= current_range.size);

            end.offset_in_range += take;

            /// If we've exactly exhausted this range, move to the next one.
            if (end.offset_in_range == current_range.size)
            {
                ++end.offset_in_ranges;
                end.offset_in_range = 0;
            }

            maximum_can_take -= take;
        }

        /// Publish new global position.
        ranges_state->pos = end;

        UInt64 block_size = base_block_size - maximum_can_take;

        return {start, block_size};
    }

protected:
    Chunk generate() override
    {
        if (ranges.empty())
            return {};

        /// Find the data range.
        /// If data left is small, shrink block size.
        auto [start, block_size] = findRanges();

        chassert(block_size <= base_block_size);

        /// No data left.
        if (!block_size)
            return {};

        chassert(start.offset_in_ranges < ranges.size());

        auto column = ColumnUInt64::create(block_size);
        ColumnUInt64::Container & vec = column->getData();

        UInt64 * pos = vec.data();

        UInt64 provided = 0;
        RangesPos cursor = start;

        while (provided < block_size)
        {
            chassert(cursor.offset_in_ranges < ranges.size());

            auto & range = ranges[cursor.offset_in_ranges];

            UInt64 need = block_size - provided;
            UInt128 remaining_in_current_range = range.size - cursor.offset_in_range;

            /// How many items from the current range should belong to this block starting at cursor.
            UInt128 can_provide = std::min<UInt128>(remaining_in_current_range, need);
            chassert(can_provide > 0);

            UInt64 take = static_cast<UInt64>(can_provide);

            /// Compute logical start value (in 128-bit to avoid overflow), then narrow to UInt64 for the column.
            UInt128 start_value_128 = static_cast<UInt128>(range.left) + cursor.offset_in_range * step;

            chassert(start_value_128 <= std::numeric_limits<UInt64>::max());
            UInt64 start_value = static_cast<UInt64>(start_value_128);

            iotaWithStepOptimized<UInt64>(pos, take, start_value, step);
            pos += take;
            provided += take;

            cursor.offset_in_range += take;
            if (cursor.offset_in_range == range.size)
            {
                ++cursor.offset_in_ranges;
                cursor.offset_in_range = 0;
            }
        }

        chassert(provided == block_size);
        progress(column->size(), column->byteSize());

        return {Columns{std::move(column)}, block_size};
    }

private:
    /// The ranges is shared between all streams.
    RangesWithStep ranges;

    /// Ranges state shared between all streams, actually is the start of the ranges.
    RangesStatePtr ranges_state;

    /// Base block size, will shrink when data left is not enough.
    UInt64 base_block_size;

    UInt64 step;
};

}

namespace
{
/// Shrink ranges to size.
///     For example: ranges: [1, 5], [8, 100]; size: 7, we will get [1, 5], [8, 9]
void shrinkRanges(RangesWithStep & ranges, size_t size)
{
    if (size == 0)
    {
        ranges.clear();
        return;
    }

    chassert(!ranges.empty());

#ifndef NDEBUG
    {
        UInt128 total_size{};
        for (const auto & r : ranges)
        {
            chassert(r.size > 0);
            total_size += r.size;
        }
        chassert(static_cast<UInt128>(size) <= total_size);
    }
#endif

    size_t last_range_idx = 0;
    [[maybe_unused]] bool found = false; /// for invariant checking only
    for (size_t i = 0; i < ranges.size(); i++)
    {
        auto range_size = ranges[i].size;
        if (range_size < size)
        {
            size -= static_cast<UInt64>(range_size);
            continue;
        }
        if (range_size == size)
        {
            last_range_idx = i;
            found = true;
            break;
        }

        auto & range = ranges[i];
        range.size = static_cast<UInt128>(size);
        last_range_idx = i;
        found = true;
        break;
    }

    /// With the preconditions above, we must have found a truncation point.
    chassert(found);
    chassert(last_range_idx < ranges.size());

    /// delete the additional ranges
    ranges.erase(ranges.begin() + (last_range_idx + 1), ranges.end());
}

}

ReadFromSystemNumbersStep::ReadFromSystemNumbersStep(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    StoragePtr storage_,
    size_t max_block_size_,
    size_t num_streams_)
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
    , num_streams{num_streams_}
    , storage_limits(query_info.storage_limits)
{
    storage_snapshot->check(column_names);
    chassert(column_names.size() == 1);
    chassert(storage->as<StorageSystemNumbers>() != nullptr);

    limit = NumbersLikeUtils::getLimitFromQueryInfo(query_info_, context);
}


void ReadFromSystemNumbersStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    if (pipe.empty())
    {
        chassert(output_header != nullptr);
        pipe = Pipe(std::make_shared<NullSource>(output_header));
    }

    /// Add storage limits.
    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    /// Add to processors to get processor info through explain pipeline statement.
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

QueryPlanStepPtr ReadFromSystemNumbersStep::clone() const
{
    return std::make_unique<ReadFromSystemNumbersStep>(column_names, getQueryInfo(), getStorageSnapshot(), getContext(), storage, max_block_size, num_streams);
}

Pipe ReadFromSystemNumbersStep::makePipe()
{
    auto & numbers_storage = storage->as<StorageSystemNumbers &>();

    /// For non-multithreaded variants (`system.numbers` / `numbers(...)`), keep a single stream to preserve ordering.
    if (!numbers_storage.multithreaded)
        num_streams = 1;

    Pipe pipe;
    const auto header = NumbersSource::createHeader(numbers_storage.column_name);

    auto add_null_source = [&] { NumbersLikeUtils::addNullSource(pipe, header); };

    /// Pushdown rationale:
    /// - Filter pushdown:
    ///   - `numbers` is a value-domain source. For bounded sources (`numbers(limit)` / `numbers(offset, limit[, step])`),
    ///     storage arguments define the table domain (with step and possible wrap), and extracted ranges are intersected
    ///     with that domain to preserve semantics while reducing generation.
    ///     Example: `SELECT number FROM numbers(10, 10) WHERE number >= 15` should read only `[15, 19]`.
    ///   - For unbounded sources (`system.numbers(_mt)` / `numbers(_mt)()`), if no useful bounds are extracted
    ///     and query LIMIT/OFFSET cannot be safely pushed down, we keep the fast unbounded generator.
    ///     Otherwise we use ranged generation from extracted ranges/bounds (or from pushed query LIMIT/OFFSET).
    ///     Example: `SELECT number FROM system.numbers WHERE number BETWEEN 100 AND 130`.
    /// - LIMIT/OFFSET pushdown:
    ///   - Safe only when extracted ranges are exact. With conservative bounds, pre-filter LIMIT may stop too early.
    ///     Example: `SELECT number FROM system.numbers WHERE number % 3 = 1 AND number < 100 LIMIT 5`.
    ///     Here `number < 100` gives only a conservative bound, so pushing LIMIT first could return too few rows
    ///     (e.g. from `[0, 1, 2, 3, 4]` we keep only `[1, 4]` instead of `[1, 4, 7, 10, 13]`).
    ///
    /// Storage-level LIMIT 0 (`numbers(..., 0, ...)`) is an empty table regardless of the WHERE clause.
    if (numbers_storage.limit.has_value() && (numbers_storage.limit.value() == 0))
    {
        add_null_source();
        return pipe;
    }

    chassert(numbers_storage.step != UInt64{0});

    /// Extract ranges/bounds implied by the WHERE clause.
    ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag ? filter_actions_dag->getOutputs().front() : nullptr, context);
    KeyCondition condition(inverted_dag, context, column_names, key_expression);

    const auto extracted_ranges = NumbersLikeUtils::extractRanges(condition);
    const bool exact_ranges = (extracted_ranges.kind == NumbersLikeUtils::ExtractedRanges::Kind::ExactRanges);

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

    /// Fast path: for unbounded tables (`system.numbers(_mt)`, `numbers(_mt)()`) with no usable bounds in WHERE,
    /// keep the ordinary unbounded scan (NumbersSource).
    ///
    /// When we can safely push down query LIMIT/OFFSET (no filter / exact plain ranges), it can be more efficient to use
    /// `NumbersRangedSource` and shrink the universe to the requested amount. Otherwise, `NumbersSource` is the most
    /// efficient generator and we let downstream transforms (filter/limit) stop the pipeline.
    const bool can_pushdown_query_limit = exact_ranges && limit.has_value();
    if (!numbers_storage.limit.has_value() && NumbersLikeUtils::isUniverse(extracted_ranges.ranges) && !can_pushdown_query_limit)
    {
        chassert(numbers_storage.step == 1);

        /// Distance between the starts of consecutive chunks in a single source.
        /// For multiple streams, each source jumps by `num_streams * block_range` to avoid overlap.
        const UInt64 step_between_chunks = num_streams * max_block_size;
        for (size_t i = 0; i < num_streams; ++i)
        {
            /// Offset between starting points of adjacent streams equals `max_block_size`,
            /// so streams generate disjoint chunks.
            const auto source_offset = i * max_block_size;
            const auto source_start = numbers_storage.offset + source_offset;

            auto source = std::make_shared<NumbersSource>(
                max_block_size, source_start, numbers_storage.column_name, step_between_chunks);

            pipe.addSource(std::move(source));
        }

        return pipe;
    }

    /// Otherwise use `NumbersRangedSource`:
    /// - bounded tables (`numbers(limit)`, `numbers(offset, limit[, step])`) to respect the table domain;
    /// - unbounded tables with some extracted bounds/ranges to reduce scanning.

    const UInt64 step = numbers_storage.step;
    const UInt64 remainder = numbers_storage.offset % step;

    RangesWithStep intersected_ranges;

    auto append_stepped_intersections = [&](const Range & domain_range, UInt64 domain_remainder)
    {
        /// Intersect extracted ranges with the table domain and convert each intersection to a stepped range
        /// that matches the sequence `x ≡ domain_remainder (mod step)`.
        for (const auto & r : extracted_ranges.ranges)
        {
            auto maybe_intersected_range = domain_range.intersectWith(r);
            if (!maybe_intersected_range)
                continue;

            auto maybe_range_with_step = steppedRangeFromRange(*maybe_intersected_range, step, domain_remainder);
            if (maybe_range_with_step)
                intersected_ranges.push_back(*maybe_range_with_step);
        }
    };

    if (numbers_storage.limit.has_value())
    {
        const UInt64 storage_limit = *numbers_storage.limit;

        /// Domain of `numbers(offset, limit[, step])` is [offset, offset + limit), potentially wrapping at 2^64.
        if (std::numeric_limits<UInt64>::max() - numbers_storage.offset >= storage_limit)
        {
            append_stepped_intersections(
                Range(FieldRef(numbers_storage.offset), true, FieldRef(numbers_storage.offset + storage_limit), false), remainder);
        }
        /// Wrap-around case, for example: numbers(18446744073709551614, 5) produces:
        /// [18446744073709551614, 18446744073709551615] then [0, 2].
        else
        {
            /// Tail segment: [offset, UInt64::max]
            append_stepped_intersections(
                Range(FieldRef(numbers_storage.offset), true, FieldRef(std::numeric_limits<UInt64>::max()), true), remainder);

            auto overflow_end = UInt128(numbers_storage.offset) + UInt128(storage_limit);
            UInt64 wrapped_end = UInt64(overflow_end - std::numeric_limits<UInt64>::max() - 1);

            /// Wrapped segment starts at 0 and ends at `wrapped_end` (exclusive).

            /// 2^64 % step, computed safely in 128-bit
            UInt64 wrap_mod = static_cast<UInt64>((static_cast<UInt128>(std::numeric_limits<UInt64>::max()) + 1) % step);

            /// remainder for the wrapped segment: (offset - 2^64) % step
            UInt128 tmp = static_cast<UInt128>(remainder) + step - wrap_mod;
            UInt64 remainder_overflow = static_cast<UInt64>(tmp % step);

            append_stepped_intersections(Range(FieldRef(UInt64(0)), true, FieldRef(wrapped_end), false), remainder_overflow);
        }
    }
    else
    {
        /// Unbounded table (`system.numbers(_mt)`, `numbers(_mt)()`): clamp extracted ranges to the UInt64 domain.
        append_stepped_intersections(Range(FieldRef(UInt64(0)), true, FieldRef(std::numeric_limits<UInt64>::max()), true), UInt64(0));
    }

    /// No intersection between the table domain and extracted ranges.
    if (intersected_ranges.empty())
    {
        add_null_source();
        return pipe;
    }

    UInt128 total_size = sizeOfRanges(intersected_ranges);

    /// Only push down query LIMIT/OFFSET when the extracted ranges are exact.
    /// For conservative bounds, pushing down LIMIT could produce too few rows after applying the full WHERE filter.
    if (exact_ranges && limit && *limit < total_size)
    {
        total_size = *limit;
        /// We should shrink intersected_ranges for case:
        ///     intersected_ranges: [1, 4], [7, 100]; query_limit: 2
        shrinkRanges(intersected_ranges, static_cast<size_t>(total_size));
    }

    NumbersLikeUtils::checkLimits(context->getSettingsRef(), size_t(total_size));

    /// Don't start more streams than can be meaningfully filled with at least one block.
    if (total_size / max_block_size < num_streams)
        num_streams = static_cast<size_t>(total_size / max_block_size);

    if (num_streams == 0)
        num_streams = 1;

    /// All streams share a single cursor into `intersected_ranges` to split work without overlaps.
    auto ranges_state = std::make_shared<NumbersRangedSource::RangesState>();
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<NumbersRangedSource>(
            intersected_ranges, ranges_state, max_block_size, numbers_storage.step, numbers_storage.column_name);

        if (i == 0)
            source->addTotalRowsApprox(static_cast<size_t>(total_size));

        pipe.addSource(std::move(source));
    }

    return pipe;
}

}
