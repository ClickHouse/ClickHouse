#include <mutex>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/ISource.h>
#include <Processors/LimitTransform.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StorageSystemNumbers.h>


namespace DB
{

namespace
{

class NumbersSource : public ISource
{
public:
    NumbersSource(UInt64 block_size_, UInt64 offset_, UInt64 step_)
        : ISource(createHeader()), block_size(block_size_), next(offset_), step(step_) {}

    String getName() const override { return "Numbers"; }

    static Block createHeader()
    {
        return { ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number") };
    }

protected:
    Chunk generate() override
    {
        auto column = ColumnUInt64::create(block_size);
        ColumnUInt64::Container & vec = column->getData();

        size_t curr = next;     /// The local variable for some reason works faster (>20%) than member of class.
        UInt64 * pos = vec.data(); /// This also accelerates the code.
        UInt64 * end = &vec[block_size];
        while (pos < end)
            *pos++ = curr++;

        next += step;

        progress(column->size(), column->byteSize());

        return { Columns {std::move(column)}, block_size };
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
    assert (size > 0);

    if (!r.right_included)
        size--;
    assert (size > 0);
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
        : ISource(NumbersSource::createHeader())
        , ranges(ranges_)
        , ranges_state(ranges_state_)
        , base_block_size(base_block_size_) {}

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

        auto first_value = [](const Range & r)
        {
            return r.left.get<UInt64>() + (r.left_included ? 0 : 1);
        };

        auto last_value = [](const Range & r)
        {
            return r.right.get<UInt64>() - (r.right_included ? 0 : 1);
        };

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
            auto& range = ranges[cursor.offset_in_ranges];

            UInt128 can_provide = cursor.offset_in_ranges == end.offset_in_ranges
                ? end.offset_in_range - cursor.offset_in_range
                : static_cast<UInt128>(last_value(range)) - first_value(range) + 1 - cursor.offset_in_range;

            UInt64 start_value = first_value(range) + cursor.offset_in_range;
            if (can_provide > need)
            {
                auto end_value = start_value + need;
                while (start_value < end_value)
                    *(pos++) = start_value++;

                provided += need;
                cursor.offset_in_range += need;
            }
            else if (can_provide == need)
            {
                auto end_value = start_value + need;
                while (start_value < end_value)
                    *(pos++) = start_value++;

                provided += need;
                cursor.offset_in_ranges++;
                cursor.offset_in_range = 0;
            }
            else
            {
                auto end_value = start_value + static_cast<UInt64>(can_provide);
                while (start_value < end_value)
                    *(pos++) = start_value++;

                provided += static_cast<UInt64>(can_provide);
                cursor.offset_in_ranges++;
                cursor.offset_in_range = 0;
            }
        }

        progress(column->size(), column->byteSize());

        return { Columns {std::move(column)}, block_size };
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
    return !query.distinct
        && !query.limitBy()
        && !query_info.has_order_by
        && !query_info.need_aggregate
        /// For new analyzer, window will be delete from AST, so we should not use query.window()
        && !query_info.has_window
        && !query_info.additional_filter_ast
        && (limit_length > 0 && !query.limit_with_ties);
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
            size = 0;
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


StorageSystemNumbers::StorageSystemNumbers(const StorageID & table_id, bool multithreaded_, std::optional<UInt64> limit_, UInt64 offset_)
    : IStorage(table_id), multithreaded(multithreaded_), limit(limit_), offset(offset_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({{"number", std::make_shared<DataTypeUInt64>()}}));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemNumbers::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    if (!multithreaded)
        num_streams = 1;

    assert(column_names.size() == 1);

    /// Build rpn of query filters
    auto col_desc = KeyDescription::parse(column_names[0], storage_snapshot->getMetadataForQuery()->columns, context);
    KeyCondition condition(query_info, context, column_names, col_desc.expression, {});

    Pipe pipe;
    Ranges ranges;

    if (condition.extractPlainRanges(ranges))
    {
        /// Intersect ranges with table range
        std::optional<Range> table_range;
        if (limit.has_value() && std::numeric_limits<UInt64>::max() - offset >= *limit)
            table_range.emplace(FieldRef(offset), true, FieldRef(offset + *limit), false);
        else
            table_range.emplace(FieldRef(offset), true, std::numeric_limits<UInt64>::max(), true);

        Ranges intersected_ranges;
        for (auto & r : ranges)
        {
            auto intersected_range = table_range->intersectWith(r);
            if (intersected_range)
                intersected_ranges.push_back(*intersected_range);
        }

        /// ranges is blank, return a source who has no data
        if (intersected_ranges.empty())
        {
            pipe.addSource(std::make_shared<NullSource>(NumbersSource::createHeader()));
            return pipe;
        }

        auto & query = query_info.query->as<ASTSelectQuery &>();
        auto [limit_length, limit_offset] = InterpreterSelectQuery::getLimitLengthAndOffset(query, context);

        bool should_pushdown_limit = shouldPushdownLimit(query_info, limit_length);

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
        auto source = std::make_shared<NumbersSource>(max_block_size, offset + i * max_block_size, num_streams * max_block_size);

        if (limit && i == 0)
        {
            auto rows_appr = *limit;
            if (query_info.limit > 0 && query_info.limit < rows_appr)
                rows_appr = query_info.limit;
            source->addTotalRowsApprox(rows_appr);
        }

        pipe.addSource(std::move(source));
    }

    if (limit)
    {
        size_t i = 0;
        /// This formula is how to split 'limit' elements to 'num_streams' chunks almost uniformly.
        pipe.addSimpleTransform([&](const Block & header)
        {
            ++i;
            return std::make_shared<LimitTransform>(
                header, *limit * i / num_streams - *limit * (i - 1) / num_streams, 0);
        });
    }

    return pipe;
}

}
