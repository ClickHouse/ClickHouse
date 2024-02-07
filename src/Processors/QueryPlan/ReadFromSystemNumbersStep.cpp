#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>

#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/LimitTransform.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Common/iota.h>
#include <Common/typeid_cast.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_MANY_ROWS;
}

namespace
{

class NumbersSource : public ISource
{
public:
    NumbersSource(UInt64 block_size_, UInt64 offset_, UInt64 step_, const std::string & column_name, UInt64 inner_step_)
        : ISource(createHeader(column_name))
        , block_size(block_size_)
        , next(offset_)
        , step(step_)
        , inner_step(inner_step_)
        , inner_remainder(offset_ % inner_step_)
    {
    }
    String getName() const override { return "Numbers"; }

    static Block createHeader(const std::string & column_name)
    {
        return {ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), column_name)};
    }

protected:
    Chunk generate() override
    {
        UInt64 curr = next; /// The local variable for some reason works faster (>20%) than member of class.
        UInt64 first_element = (curr / inner_step) * inner_step + inner_remainder;
        if (first_element < curr)
            first_element += inner_step;
        UInt64 filtered_block_size = 0;
        if (first_element - curr >= block_size)
        {
            auto column = ColumnUInt64::create(0);
            return {Columns{std::move(column)}, filtered_block_size};
        }
        if (first_element - curr < block_size)
            filtered_block_size = (block_size - (first_element - curr) - 1) / inner_step + 1;

        auto column = ColumnUInt64::create(filtered_block_size);
        ColumnUInt64::Container & vec = column->getData();
        UInt64 * pos = vec.data(); /// This also accelerates the code.
        UInt64 * end = &vec[filtered_block_size];
        iota_with_step(pos, static_cast<size_t>(end - pos), first_element, inner_step);

        next += step;

        progress(column->size(), column->byteSize());

        return {Columns{std::move(column)}, filtered_block_size};
    }

private:
    UInt64 block_size;
    UInt64 next;
    UInt64 step;
    UInt64 inner_step;
    UInt64 inner_remainder;
};

struct RangeWithStep
{
    Range range;
    UInt64 step;
};

using RangesWithStep = std::vector<RangeWithStep>;

std::optional<RangeWithStep> stepped_range_from_range(const Range & r, UInt64 step, UInt64 remainder)
{
    if ((r.right.get<UInt64>() == 0) && (!r.right_included))
        return std::nullopt;
    UInt64 begin = (r.left.get<UInt64>() / step) * step;
    if (begin > std::numeric_limits<UInt64>::max() - remainder)
        return std::nullopt;
    begin += remainder;

    while ((r.left_included <= r.left.get<UInt64>()) && (begin <= r.left.get<UInt64>() - r.left_included))
    {
        if (std::numeric_limits<UInt64>::max() - step < begin)
            return std::nullopt;
        begin += step;
    }

    if ((begin >= r.right_included) && (begin - r.right_included >= r.right.get<UInt64>()))
        return std::nullopt;
    UInt64 right_edge_included = r.right.get<UInt64>() - (1 - r.right_included);
    return std::optional{RangeWithStep{Range(begin, true, right_edge_included, true), step}};
}

[[maybe_unused]] UInt128 sizeOfRange(const RangeWithStep & r)
{
    if (r.range.right.isPositiveInfinity())
        return static_cast<UInt128>(std::numeric_limits<UInt64>::max() - r.range.left.get<UInt64>()) / r.step + r.range.left_included;

    return static_cast<UInt128>(r.range.right.get<UInt64>() - r.range.left.get<UInt64>()) / r.step + 1;
};

[[maybe_unused]] auto sizeOfRanges(const RangesWithStep & rs)
{
    UInt128 total_size{};
    for (const RangeWithStep & r : rs)
    {
        /// total_size will never overflow
        total_size += sizeOfRange(r);
    }
    return total_size;
};

/// Generate numbers according to ranges.
/// Numbers generated is ordered in one stream.
/// Notice that we will not generate additional numbers out of ranges.
class [[maybe_unused]] NumbersRangedSource : public ISource
{
public:
    /// Represent a position in Ranges list.
    struct RangesPos
    {
        size_t offset_in_ranges;
        UInt128 offset_in_range;
    };

    struct RangesState
    {
        RangesPos pos;
        mutable std::mutex mutex;
    };

    using RangesStatePtr = std::shared_ptr<RangesState>;

    [[maybe_unused]] NumbersRangedSource(
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

protected:
    /// Find the data range in ranges and return how many item found.
    /// If no data left in ranges return 0.
    UInt64 findRanges(RangesPos & start, RangesPos & end, UInt64 base_block_size_)
    {
        std::lock_guard lock(ranges_state->mutex);


        UInt64 need = base_block_size_;
        UInt64 size = 0; /// how many item found.

        /// find start
        start = ranges_state->pos;
        end = start;

        /// find end
        while (need != 0)
        {
            UInt128 can_provide = end.offset_in_ranges == ranges.size() ? static_cast<UInt128>(0)
                                                                        : sizeOfRange(ranges[end.offset_in_ranges]) - end.offset_in_range;
            if (can_provide == 0)
                break;

            if (can_provide > need)
            {
                end.offset_in_range += need;
                size += need;
                need = 0;
            }
            else if (can_provide == need)
            {
                end.offset_in_ranges++;
                end.offset_in_range = 0;
                size += need;
                need = 0;
            }
            else
            {
                end.offset_in_ranges++;
                end.offset_in_range = 0;
                size += static_cast<UInt64>(can_provide);
                need -= static_cast<UInt64>(can_provide);
            }
        }

        ranges_state->pos = end;

        return size;
    }

    Chunk generate() override
    {
        if (ranges.empty())
            return {};

        auto first_value = [](const RangeWithStep & r) { return r.range.left.get<UInt64>() + (r.range.left_included ? 0 : 1); };

        auto last_value = [](const RangeWithStep & r) { return r.range.right.get<UInt64>() - (r.range.right_included ? 0 : 1); };

        /// Find the data range.
        /// If data left is small, shrink block size.
        RangesPos start, end;
        auto block_size = findRanges(start, end, base_block_size);

        if (!block_size)
            return {};

        auto column = ColumnUInt64::create(block_size);
        ColumnUInt64::Container & vec = column->getData();

        /// This will accelerates the code.
        UInt64 * pos = vec.data();

        UInt64 provided = 0;
        RangesPos cursor = start;

        while (block_size - provided != 0)
        {
            UInt64 need = block_size - provided;
            auto & range = ranges[cursor.offset_in_ranges];

            UInt128 can_provide = cursor.offset_in_ranges == end.offset_in_ranges
                ? end.offset_in_range - cursor.offset_in_range
                : static_cast<UInt128>(last_value(range) - first_value(range)) / range.step + 1 - cursor.offset_in_range;

            /// set value to block
            auto set_value = [&pos, this](UInt128 & start_value, UInt128 & end_value)
            {
                if (end_value > std::numeric_limits<UInt64>::max())
                {
                    while (start_value < end_value)
                    {
                        *(pos++) = start_value;
                        start_value += this->step;
                    }
                }
                else
                {
                    auto start_value_64 = static_cast<UInt64>(start_value);
                    auto end_value_64 = static_cast<UInt64>(end_value);
                    auto size = (end_value_64 - start_value_64) / this->step;
                    iota_with_step(pos, static_cast<size_t>(size), start_value_64, step);
                    pos += size;
                }
            };

            if (can_provide > need)
            {
                UInt64 start_value = first_value(range) + cursor.offset_in_range * step;
                /// end_value will never overflow
                iota_with_step(pos, static_cast<size_t>(need), start_value, step);
                pos += need;

                provided += need;
                cursor.offset_in_range += need;
            }
            else if (can_provide == need)
            {
                /// to avoid UInt64 overflow
                UInt128 start_value = static_cast<UInt128>(first_value(range)) + cursor.offset_in_range * step;
                UInt128 end_value = start_value + need * step;
                set_value(start_value, end_value);

                provided += need;
                cursor.offset_in_ranges++;
                cursor.offset_in_range = 0;
            }
            else
            {
                /// to avoid UInt64 overflow
                UInt128 start_value = static_cast<UInt128>(first_value(range)) + cursor.offset_in_range * step;
                UInt128 end_value = start_value + can_provide * step;
                set_value(start_value, end_value);

                provided += static_cast<UInt64>(can_provide);
                cursor.offset_in_ranges++;
                cursor.offset_in_range = 0;
            }
        }

        chassert(block_size == UInt64(pos - vec.begin()));
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
[[maybe_unused]] bool shouldPushdownLimit(SelectQueryInfo & query_info, UInt64 limit_length)
{
    const auto & query = query_info.query->as<ASTSelectQuery &>();
    /// Just ignore some minor cases, such as:
    ///     select * from system.numbers order by number asc limit 10
    return !query.distinct && !query.limitBy() && !query_info.has_order_by
        && !query_info.need_aggregate
        /// For new analyzer, window will be delete from AST, so we should not use query.window()
        && !query_info.has_window && !query_info.additional_filter_ast && (limit_length > 0 && !query.limit_with_ties);
}

/// Shrink ranges to size.
///     For example: ranges: [1, 5], [8, 100]; size: 7, we will get [1, 5], [8, 9]
[[maybe_unused]] void shrinkRanges(RangesWithStep & ranges, size_t size)
{
    size_t last_range_idx = 0;
    for (size_t i = 0; i < ranges.size(); i++)
    {
        auto range_size = sizeOfRange(ranges[i]);
        if (range_size < size)
        {
            size -= static_cast<UInt64>(range_size);
            continue;
        }
        else if (range_size == size)
        {
            last_range_idx = i;
            break;
        }
        else
        {
            auto & range = ranges[i];
            UInt64 right = range.range.left.get<UInt64>() + static_cast<UInt64>(size);
            range.range.right = Field(right);
            range.range.right_included = !range.range.left_included;
            last_range_idx = i;
            break;
        }
    }

    /// delete the additional ranges
    ranges.erase(ranges.begin() + (last_range_idx + 1), ranges.end());
}

}

ReadFromSystemNumbersStep::ReadFromSystemNumbersStep(
    const Names & column_names_,
    StoragePtr storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    size_t max_block_size_,
    size_t num_streams_)
    : SourceStepWithFilter{DataStream{.header = storage_snapshot_->getSampleBlockForColumns(column_names_)}}
    , column_names{column_names_}
    , storage{std::move(storage_)}
    , storage_snapshot{storage_snapshot_}
    , context{std::move(context_)}
    , key_expression{KeyDescription::parse(column_names[0], storage_snapshot->metadata->columns, context).expression}
    , max_block_size{max_block_size_}
    , num_streams{num_streams_}
    , limit_length_and_offset(InterpreterSelectQuery::getLimitLengthAndOffset(query_info.query->as<ASTSelectQuery &>(), context))
    , should_pushdown_limit(shouldPushdownLimit(query_info, limit_length_and_offset.first))
    , limit(query_info.limit)
    , storage_limits(query_info.storage_limits)
{
    storage_snapshot->check(column_names);
    chassert(column_names.size() == 1);
    chassert(storage->as<StorageSystemNumbers>() != nullptr);
}


void ReadFromSystemNumbersStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    if (pipe.empty())
    {
        assert(output_stream != std::nullopt);
        pipe = Pipe(std::make_shared<NullSource>(output_stream->header));
    }

    /// Add storage limits.
    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    /// Add to processors to get processor info through explain pipeline statement.
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
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

    /// Build rpn of query filters
    KeyCondition condition(buildFilterDAG(), context, column_names, key_expression);


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
        }


        /// intersection with overflowed_table_range goes back.
        if (overflowed_table_range.has_value())
        {
            for (auto & r : ranges)
            {
                auto intersected_range = overflowed_table_range->intersectWith(r);
                if (intersected_range)
                {
                    auto range_with_step = stepped_range_from_range(
                        intersected_range.value(),
                        numbers_storage.step,
                        static_cast<UInt64>(
                            (static_cast<UInt128>(numbers_storage.offset) + std::numeric_limits<UInt64>::max() + 1)
                            % numbers_storage.step));
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
        const auto & limit_length = limit_length_and_offset.first;
        const auto & limit_offset = limit_length_and_offset.second;

        /// If intersected ranges is limited or we can pushdown limit.
        if (!intersected_ranges.rbegin()->range.right.isPositiveInfinity() || should_pushdown_limit)
        {
            UInt128 total_size = sizeOfRanges(intersected_ranges);
            UInt128 query_limit = limit_length + limit_offset;

            /// limit total_size by query_limit
            if (should_pushdown_limit && query_limit < total_size)
            {
                total_size = query_limit;
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
    }

    /// Fall back to NumbersSource
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<NumbersSource>(
            max_block_size,
            numbers_storage.offset + i * max_block_size,
            num_streams * max_block_size,
            numbers_storage.column_name,
            numbers_storage.step);

        if (numbers_storage.limit && i == 0)
        {
            auto rows_appr = (*numbers_storage.limit - 1) / numbers_storage.step + 1;
            if (limit > 0 && limit < rows_appr)
                rows_appr = limit;
            source->addTotalRowsApprox(rows_appr);
        }

        pipe.addSource(std::move(source));
    }

    if (numbers_storage.limit)
    {
        size_t i = 0;
        auto storage_limit = (*numbers_storage.limit - 1) / numbers_storage.step + 1;
        /// This formula is how to split 'limit' elements to 'num_streams' chunks almost uniformly.
        pipe.addSimpleTransform(
            [&](const Block & header)
            {
                ++i;
                return std::make_shared<LimitTransform>(header, storage_limit * i / num_streams - storage_limit * (i - 1) / num_streams, 0);
            });
    }

    return pipe;
}

ActionsDAGPtr ReadFromSystemNumbersStep::buildFilterDAG()
{
    std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_node_column;
    return ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes, node_name_to_input_node_column, context);
}

void ReadFromSystemNumbersStep::checkLimits(size_t rows)
{
    const auto & settings = context->getSettingsRef();

    if (settings.read_overflow_mode == OverflowMode::THROW && settings.max_rows_to_read)
    {
        const auto limits = SizeLimits(settings.max_rows_to_read, 0, settings.read_overflow_mode);
        limits.check(rows, 0, "rows (controlled by 'max_rows_to_read' setting)", ErrorCodes::TOO_MANY_ROWS);
    }

    if (settings.read_overflow_mode_leaf == OverflowMode::THROW && settings.max_rows_to_read_leaf)
    {
        const auto leaf_limits = SizeLimits(settings.max_rows_to_read_leaf, 0, settings.read_overflow_mode_leaf);
        leaf_limits.check(rows, 0, "rows (controlled by 'max_rows_to_read_leaf' setting)", ErrorCodes::TOO_MANY_ROWS);
    }
}

}
