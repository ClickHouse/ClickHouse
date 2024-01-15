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
    NumbersSource(UInt64 block_size_, UInt64 offset_, UInt64 step_)
        : ISource(createHeader()), block_size(block_size_), next(offset_), step(step_)
    {
    }

    String getName() const override { return "Numbers"; }

    static Block createHeader() { return {ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number")}; }

protected:
    Chunk generate() override
    {
        auto column = ColumnUInt64::create(block_size);
        ColumnUInt64::Container & vec = column->getData();

        UInt64 curr = next; /// The local variable for some reason works faster (>20%) than member of class.
        UInt64 * pos = vec.data(); /// This also accelerates the code.
        UInt64 * end = &vec[block_size];
        iota(pos, static_cast<size_t>(end - pos), curr);

        next += step;

        progress(column->size(), column->byteSize());

        return {Columns{std::move(column)}, block_size};
    }

private:
    UInt64 block_size;
    UInt64 next;
    UInt64 step;
};


UInt128 sizeOfRange(const Range & r)
{
    UInt128 size;
    if (r.right.isPositiveInfinity())
        return static_cast<UInt128>(std::numeric_limits<uint64_t>::max()) - r.left.get<UInt64>() + r.left_included;

    size = static_cast<UInt128>(r.right.get<UInt64>()) - r.left.get<UInt64>() + 1;

    if (!r.left_included)
        size--;

    if (!r.right_included)
        size--;
    assert(size >= 0);
    return size;
};

auto sizeOfRanges(const Ranges & rs)
{
    UInt128 total_size{};
    for (const Range & r : rs)
    {
        /// total_size will never overflow
        total_size += sizeOfRange(r);
    }
    return total_size;
};

/// Generate numbers according to ranges.
/// Numbers generated is ordered in one stream.
/// Notice that we will not generate additional numbers out of ranges.
class NumbersRangedSource : public ISource
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

    NumbersRangedSource(const Ranges & ranges_, RangesStatePtr & ranges_state_, UInt64 base_block_size_)
        : ISource(NumbersSource::createHeader()), ranges(ranges_), ranges_state(ranges_state_), base_block_size(base_block_size_)
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

        auto first_value = [](const Range & r) { return r.left.get<UInt64>() + (r.left_included ? 0 : 1); };

        auto last_value = [](const Range & r) { return r.right.get<UInt64>() - (r.right_included ? 0 : 1); };

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
                : static_cast<UInt128>(last_value(range)) - first_value(range) + 1 - cursor.offset_in_range;

            /// set value to block
            auto set_value = [&pos](UInt128 & start_value, UInt128 & end_value)
            {
                if (end_value > std::numeric_limits<UInt64>::max())
                {
                    while (start_value < end_value)
                        *(pos++) = start_value++;
                }
                else
                {
                    auto start_value_64 = static_cast<UInt64>(start_value);
                    auto end_value_64 = static_cast<UInt64>(end_value);
                    auto size = end_value_64 - start_value_64;
                    iota(pos, static_cast<size_t>(size), start_value_64);
                    pos += size;
                }
            };

            if (can_provide > need)
            {
                UInt64 start_value = first_value(range) + cursor.offset_in_range;
                /// end_value will never overflow
                iota(pos, static_cast<size_t>(need), start_value);
                pos += need;

                provided += need;
                cursor.offset_in_range += need;
            }
            else if (can_provide == need)
            {
                /// to avoid UInt64 overflow
                UInt128 start_value = static_cast<UInt128>(first_value(range)) + cursor.offset_in_range;
                UInt128 end_value = start_value + need;
                set_value(start_value, end_value);

                provided += need;
                cursor.offset_in_ranges++;
                cursor.offset_in_range = 0;
            }
            else
            {
                /// to avoid UInt64 overflow
                UInt128 start_value = static_cast<UInt128>(first_value(range)) + cursor.offset_in_range;
                UInt128 end_value = start_value + can_provide;
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
    Ranges ranges;

    /// Ranges state shared between all streams, actually is the start of the ranges.
    RangesStatePtr ranges_state;

    /// Base block size, will shrink when data left is not enough.
    UInt64 base_block_size;
};

}

namespace
{
/// Whether we should push limit down to scan.
bool shouldPushdownLimit(SelectQueryInfo & query_info, UInt64 limit_length)
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
void shrinkRanges(Ranges & ranges, size_t size)
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
            UInt64 right = range.left.get<UInt64>() + static_cast<UInt64>(size);
            range.right = Field(right);
            range.right_included = !range.left_included;
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
    , limit_length_and_offset(InterpreterSelectQuery::getLimitLengthAndOffset(query_info.query->as<ASTSelectQuery&>(), context))
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

    /// Build rpn of query filters
    KeyCondition condition(buildFilterDAG(), context, column_names, key_expression);

    Pipe pipe;
    Ranges ranges;

    if (condition.extractPlainRanges(ranges))
    {
        /// Intersect ranges with table range
        std::optional<Range> table_range;
        std::optional<Range> overflowed_table_range;

        if (numbers_storage.limit.has_value())
        {
            if (std::numeric_limits<UInt64>::max() - numbers_storage.offset >= *(numbers_storage.limit))
            {
                table_range.emplace(FieldRef(numbers_storage.offset), true, FieldRef(numbers_storage.offset + *(numbers_storage.limit)), false);
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

        Ranges intersected_ranges;
        for (auto & r : ranges)
        {
            auto intersected_range = table_range->intersectWith(r);
            if (intersected_range)
                intersected_ranges.push_back(*intersected_range);
        }
        /// intersection with overflowed_table_range goes back.
        if (overflowed_table_range.has_value())
        {
            for (auto & r : ranges)
            {
                auto intersected_range = overflowed_table_range->intersectWith(r);
                if (intersected_range)
                    intersected_ranges.push_back(*overflowed_table_range);
            }
        }

        /// ranges is blank, return a source who has no data
        if (intersected_ranges.empty())
        {
            pipe.addSource(std::make_shared<NullSource>(NumbersSource::createHeader()));
            return pipe;
        }
        const auto & limit_length = limit_length_and_offset.first;
        const auto & limit_offset = limit_length_and_offset.second;

        /// If intersected ranges is limited or we can pushdown limit.
        if (!intersected_ranges.rbegin()->right.isPositiveInfinity() || should_pushdown_limit)
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
                auto source = std::make_shared<NumbersRangedSource>(intersected_ranges, ranges_state, max_block_size);

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
        auto source
            = std::make_shared<NumbersSource>(max_block_size, numbers_storage.offset + i * max_block_size, num_streams * max_block_size);

        if (numbers_storage.limit && i == 0)
        {
            auto rows_appr = *(numbers_storage.limit);
            if (limit > 0 && limit < rows_appr)
                rows_appr = limit;
            source->addTotalRowsApprox(rows_appr);
        }

        pipe.addSource(std::move(source));
    }

    if (numbers_storage.limit)
    {
        size_t i = 0;
        auto storage_limit = *(numbers_storage.limit);
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
