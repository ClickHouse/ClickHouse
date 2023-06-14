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


class NumbersRangedSource : public ISource
{
public:
    /// Represent a position in Ranges
    struct RangesPos
    {
        /// offset in Ranges
        size_t x;
        /// offset in Range
        size_t y;
    };

    NumbersRangedSource(const Ranges & ranges_, UInt64 base_block_size_, RangesPos start_, RangesPos end_, size_t size_)
        : ISource(NumbersSource::createHeader())
        , ranges(ranges_)
        , base_block_size(base_block_size_)
        , cursor(start_)
        , end(end_)
        , size(size_) {}

    String getName() const override { return "NumbersRange"; }

protected:
    Chunk generate() override
    {
        if (ranges.empty())
            return {};

        auto first_value = [](const Range & r)
        {
            return r.left.get<UInt64>() - (r.left_included ? 0 : 1);
        };

        auto last_value = [](const Range & r)
        {
            return r.right.get<UInt64>() - (r.right_included ? 0 : 1);
        };

        auto block_size = std::min(base_block_size, size);

        if (!block_size)
            return {};

        auto column = ColumnUInt64::create(block_size);
        ColumnUInt64::Container & vec = column->getData();
        UInt64 * pos = vec.data(); /// This will accelerates the code.

        size_t provided = 0;
        for (; size != 0 && block_size - provided != 0;)
        {
            size_t need = block_size - provided;
            auto& range = ranges[cursor.x];

            size_t can_provide = cursor.x == end.x ? end.y - cursor.y : last_value(range) - first_value(range) + 1 - cursor.y;

            uint64_t start_value = first_value(range) + cursor.y;
            if (can_provide > need)
            {
                for (size_t i=0; i < need; i++)
                    *(pos++) = start_value + i;

                provided += need;
                cursor.y += need;
                size -= need;
            }
            else if (can_provide == need)
            {
                for (size_t i=0; i < need; i++)
                    *(pos++) = start_value + i;

                provided += need;
                cursor.x++;
                cursor.y = 0;
                size -= need;
            }
            else
            {
                for (size_t i=0; i < can_provide; i++)
                    *(pos++) = start_value + i;

                provided += can_provide;
                cursor.x++;
                cursor.y = 0;
                size -= can_provide;
            }
        }

        progress(column->size(), column->byteSize());

        return { Columns {std::move(column)}, block_size };
    }

private:
    Ranges ranges;
    /// Base block size, will change if there is no enough numbers
    UInt64 base_block_size;

    RangesPos cursor;
    RangesPos end; /// not included

    /// how many numbers left
    size_t size;
};

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

    Pipe pipe;

    assert(column_names.size() == 1);

    /// build query filter rpn
    auto col_desc = KeyDescription::parse(column_names[0], storage_snapshot->getMetadataForQuery()->columns, context);
    KeyCondition condition(query_info, context, column_names, col_desc.expression, {});

    Ranges ranges;
    if (condition.extractPlainRanges(ranges))
    {
        /// Intersect ranges with table range
        Range table_range(
            FieldRef(offset), true, limit.has_value() ? FieldRef(offset + *limit) : POSITIVE_INFINITY, limit.has_value());

        Ranges intersected_ranges;
        for (auto & r : ranges)
        {
            auto intersected_range = table_range.intersectWith(r);
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
        size_t query_limit = limit_length + limit_offset;

        /// If intersected ranges is unlimited, use NumbersRangedSource
        if (!intersected_ranges.rbegin()->right.isPositiveInfinity() || query_limit > 0)
        {
            auto size_of_range = [] (const Range & r) -> size_t
            {
                size_t size;
                uint64_t right = r.right.isPositiveInfinity() ? std::numeric_limits<uint64_t>::max() : r.right.get<UInt64>();

                size = right - r.left.get<UInt64>() + 1;
                if (!r.left_included)
                    size--;
                assert (size > 0);

                if (!r.right_included)
                    size--;
                assert (size > 0);
                return size;
            };

            auto size_of_ranges = [&size_of_range](const Ranges & rs) -> size_t
            {
                size_t total_size{};
                for (const Range & r : rs)
                {
                    /// total_size will never overflow
                    total_size += size_of_range(r);
                }
                return total_size;
            };

            size_t total_size = size_of_ranges(intersected_ranges);

            /// limit total_size by query_limit
            if (query_limit > 0 && query_limit < total_size)
                total_size = query_limit;

            num_streams = std::min(num_streams, total_size / max_block_size);

            if (num_streams == 0)
                num_streams = 1;

            /// Split ranges evenly, every sub ranges will have approximately same amount of numbers.
            NumbersRangedSource::RangesPos start({0, 0});
            for (size_t i = 0; i < num_streams; ++i)
            {
                auto size = total_size * (i + 1) / num_streams - total_size * i / num_streams;
                size_t need = size;

                /// find end
                NumbersRangedSource::RangesPos end(start);
                while (need != 0)
                {
                    size_t can_provide = size_of_range(intersected_ranges[end.x]) - end.y;
                    if (can_provide > need)
                    {
                        end.y += need;
                        need = 0;
                    }
                    else if (can_provide == need)
                    {
                        end.x++;
                        end.y = 0;
                        need = 0;
                    }
                    else
                    {
                        end.x++;
                        end.y = 0;
                        need -= can_provide;
                    }
                }

                auto source = std::make_shared<NumbersRangedSource>(intersected_ranges, max_block_size, start, end, size);
                start = end;

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
