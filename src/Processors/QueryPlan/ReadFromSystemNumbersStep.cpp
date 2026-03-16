#include <memory>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>

#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/LimitTransform.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <base/types.h>
#include <fmt/format.h>
#include <Common/iota.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Core/Types.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsOverflowMode read_overflow_mode_leaf;
}

namespace ErrorCodes
{
extern const int TOO_MANY_ROWS;
}

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

/// The range is defined as [start, end)
UInt64 itemCountInRange(UInt64 start, UInt64 end, UInt64 step)
{
    const auto range_count = end - start;
    if (step == 1)
        return range_count;

    return (range_count - 1) / step + 1;
}

class NumbersSource : public ISource
{
public:
    NumbersSource(
        UInt64 block_size_,
        UInt64 offset_,
        std::optional<UInt64> end_,
        const std::string & column_name,
        UInt64 step_in_chunk_,
        UInt64 step_between_chunks_)
        : ISource(createHeader(column_name))
        , block_size(block_size_)
        , next(offset_)
        , end(end_)
        , step_in_chunk(step_in_chunk_)
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
        UInt64 real_block_size = block_size;
        if (end.has_value())
        {
            if (end.value() <= next)
                return {};

            auto max_items_to_generate = itemCountInRange(next, *end, step_in_chunk);

            real_block_size = std::min(block_size, max_items_to_generate);
        }
        auto column = ColumnUInt64::create(real_block_size);
        ColumnUInt64::Container & vec = column->getData();

        UInt64 curr = next; /// The local variable for some reason works faster (>20%) than member of class.
        UInt64 * pos = vec.data(); /// This also accelerates the code.

        UInt64 * current_end = &vec[real_block_size];

        iotaWithStepOptimized(pos, static_cast<size_t>(current_end - pos), curr, step_in_chunk);

        next += step_between_chunks;

        progress(column->size(), column->byteSize());
        return {Columns{std::move(column)}, real_block_size};
    }

private:
    UInt64 block_size;
    UInt64 next;
    std::optional<UInt64> end; /// not included
    UInt64 step_in_chunk;
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
/// Whether we should push limit down to scan.
bool shouldPushdownLimit(const SelectQueryInfo & query_info, const InterpreterSelectQuery::LimitInfo & lim_info)
{
    /// Reject negative, fractional, and zero limits for pushdown
    if (lim_info.is_limit_length_negative
        || lim_info.fractional_limit > 0
        || lim_info.fractional_offset > 0
        || lim_info.limit_length == 0)
        return false;

    chassert(query_info.query);

    const auto & query = query_info.query->as<ASTSelectQuery &>();

    /// Just ignore some minor cases, such as:
    ///     select * from system.numbers order by number asc limit 10
    return !query.distinct
        && !query.limitBy()
        && !query_info.has_order_by
        && !query_info.need_aggregate
        /// For new analyzer, window will be deleted from AST, so we should not use query.window()
        && !query_info.has_window
        && !query_info.additional_filter_ast
        && !query.limit_with_ties;
}

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

/// This is ideologically wrong. We should only get it from the query plan optimization.
std::optional<size_t> getLimitFromQueryInfo(const SelectQueryInfo & query_info, const ContextPtr & context)
{
    if (!query_info.query)
        return {};

    const auto lim_info = InterpreterSelectQuery::getLimitLengthAndOffset(query_info.query->as<ASTSelectQuery &>(), context);

    if (!shouldPushdownLimit(query_info, lim_info))
        return {};

    return lim_info.limit_length + lim_info.limit_offset;
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
    , query_info_limit(query_info.trivial_limit)
    , storage_limits(query_info.storage_limits)
{
    storage_snapshot->check(column_names);
    chassert(column_names.size() == 1);
    chassert(storage->as<StorageSystemNumbers>() != nullptr);

    limit = getLimitFromQueryInfo(query_info_, context);
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

    if (!numbers_storage.multithreaded)
        num_streams = 1;

    Pipe pipe;
    Ranges ranges;

    if (numbers_storage.limit.has_value() && (numbers_storage.limit.value() == 0))
    {
        pipe.addSource(std::make_shared<NullSource>(NumbersSource::createHeader(numbers_storage.column_name)));
        return pipe;
    }
    chassert(numbers_storage.step != UInt64{0});

    /// Build rpn of query filters
    ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag ? filter_actions_dag->getOutputs().front() : nullptr, context);
    KeyCondition condition(inverted_dag, context, column_names, key_expression);

    if (condition.extractPlainRanges(ranges))
    {
        /// Intersect ranges with table range
        std::optional<Range> table_range;
        std::optional<Range> overflowed_table_range;

        if (numbers_storage.limit.has_value())
        {
            if (std::numeric_limits<UInt64>::max() - numbers_storage.offset >= *(numbers_storage.limit))
            {
                table_range.emplace(
                    FieldRef(numbers_storage.offset), true, FieldRef(numbers_storage.offset + *(numbers_storage.limit)), false);
            }
            /// UInt64 overflow, for example: SELECT number FROM numbers(18446744073709551614, 5)
            else
            {
                table_range.emplace(FieldRef(numbers_storage.offset), true, std::numeric_limits<UInt64>::max(), true);
                auto overflow_end = UInt128(numbers_storage.offset) + UInt128(*numbers_storage.limit);
                overflowed_table_range.emplace(
                    FieldRef(UInt64(0)), true, FieldRef(UInt64(overflow_end - std::numeric_limits<UInt64>::max() - 1)), false);
            }
        }
        else
        {
            table_range.emplace(FieldRef(numbers_storage.offset), true, FieldRef(std::numeric_limits<UInt64>::max()), true);
        }

        RangesWithStep intersected_ranges;
        for (auto & r : ranges)
        {
            auto intersected_range = table_range->intersectWith(r);
            if (intersected_range.has_value())
            {
                auto range_with_step
                    = steppedRangeFromRange(intersected_range.value(), numbers_storage.step, numbers_storage.offset % numbers_storage.step);
                if (range_with_step.has_value())
                    intersected_ranges.push_back(*range_with_step);
            }
        }


        /// intersection with overflowed_table_range goes back.
        if (overflowed_table_range.has_value())
        {
            auto step = numbers_storage.step;

            UInt64 offset_mod = numbers_storage.offset % step;

            /// 2^64 % step, computed safely in 128-bit
            UInt64 wrap_mod = static_cast<UInt64>((static_cast<UInt128>(std::numeric_limits<UInt64>::max()) + 1) % step);

            /// remainder for the wrapped segment: (offset - 2^64) % step
            UInt128 tmp = static_cast<UInt128>(offset_mod) + step - wrap_mod;
            UInt64 remainder_overflow = static_cast<UInt64>(tmp % step);

            for (auto & r : ranges)
            {
                auto intersected_range = overflowed_table_range->intersectWith(r);
                if (intersected_range)
                {
                    auto range_with_step = steppedRangeFromRange(intersected_range.value(), step, remainder_overflow);
                    if (range_with_step)
                        intersected_ranges.push_back(*range_with_step);
                }
            }
        }

        /// ranges is blank, return a source who has no data
        if (intersected_ranges.empty())
        {
            pipe.addSource(std::make_shared<NullSource>(NumbersSource::createHeader(numbers_storage.column_name)));
            return pipe;
        }

        UInt128 total_size = sizeOfRanges(intersected_ranges);

        /// limit total_size by query_limit
        if (limit && *limit < total_size)
        {
            total_size = *limit;
            /// We should shrink intersected_ranges for case:
            ///     intersected_ranges: [1, 4], [7, 100]; query_limit: 2
            shrinkRanges(intersected_ranges, total_size);
        }

        checkLimits(size_t(total_size));

        if (total_size / max_block_size < num_streams)
            num_streams = static_cast<size_t>(total_size / max_block_size);

        if (num_streams == 0)
            num_streams = 1;

        /// Ranges state, all streams will share the state.
        auto ranges_state = std::make_shared<NumbersRangedSource::RangesState>();
        for (size_t i = 0; i < num_streams; ++i)
        {
            auto source = std::make_shared<NumbersRangedSource>(
                intersected_ranges, ranges_state, max_block_size, numbers_storage.step, numbers_storage.column_name);

            if (i == 0)
                source->addTotalRowsApprox(total_size);

            pipe.addSource(std::move(source));
        }
        return pipe;
    }

    const auto end = std::invoke(
        [&]() -> std::optional<UInt64>
        {
            if (numbers_storage.limit.has_value())
                return *(numbers_storage.limit) + numbers_storage.offset;
            return {};
        });

    /// Fall back to NumbersSource
    /// Range in a single block
    const auto block_range = max_block_size * numbers_storage.step;
    /// Step between chunks in a single source.
    /// It is bigger than block_range in case of multiple threads, because we have to account for other sources as well.
    const auto step_between_chunks = num_streams * block_range;
    for (size_t i = 0; i < num_streams; ++i)
    {
        const auto source_offset = i * block_range;
        if (numbers_storage.limit.has_value() && *numbers_storage.limit < source_offset)
            break;

        const auto source_start = numbers_storage.offset + source_offset;

        auto source = std::make_shared<NumbersSource>(
            max_block_size,
            source_start,
            end,
            numbers_storage.column_name,
            numbers_storage.step,
            step_between_chunks);

        if (end && i == 0)
        {
            UInt64 rows_approx = itemCountInRange(numbers_storage.offset, *end, numbers_storage.step);
            if (limit > 0 && limit < rows_approx)
                rows_approx = query_info_limit;
            source->addTotalRowsApprox(rows_approx);
        }

        pipe.addSource(std::move(source));
    }

    return pipe;
}

void ReadFromSystemNumbersStep::checkLimits(size_t rows)
{
    const auto & settings = context->getSettingsRef();

    if (settings[Setting::read_overflow_mode] == OverflowMode::THROW && settings[Setting::max_rows_to_read])
    {
        const auto limits = SizeLimits(settings[Setting::max_rows_to_read], 0, settings[Setting::read_overflow_mode]);
        limits.check(rows, 0, "rows (controlled by 'max_rows_to_read' setting)", ErrorCodes::TOO_MANY_ROWS);
    }

    if (settings[Setting::read_overflow_mode_leaf] == OverflowMode::THROW && settings[Setting::max_rows_to_read_leaf])
    {
        const auto leaf_limits = SizeLimits(settings[Setting::max_rows_to_read_leaf], 0, settings[Setting::read_overflow_mode_leaf]);
        leaf_limits.check(rows, 0, "rows (controlled by 'max_rows_to_read_leaf' setting)", ErrorCodes::TOO_MANY_ROWS);
    }
}

}
