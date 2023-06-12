#include <Common/Exception.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Storages/SelectQueryInfo.h>

#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/LimitTransform.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/KeyDescription.h>


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

    static Block createHeader()
    {
        return { ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number") };
    }
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

    NumbersRangedSource(const Ranges & ranges_, UInt64 block_size_, RangesPos start_, RangesPos end_)
        : ISource(createHeader())
        , ranges(ranges_)
        , block_size(block_size_)
        , cursor(start_)
        , end(end_) {}

    String getName() const override { return "NumbersRange"; }

protected:
    Chunk generate() override
    {
        if (block_size == 0)
            return {};

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

        auto column = ColumnUInt64::create(block_size);
        ColumnUInt64::Container & vec = column->getData();

        for (; cursor.x <= end.x;)
        {
            size_t need = block_size - vec.size();

            auto& range = ranges[cursor.x];
            UInt64 * pos = vec.data(); /// This also accelerates the code.

            size_t can_provide = cursor.x == end.x ? end.y - cursor.y : last_value(range) - first_value(range) + 1 - cursor.y;

            uint64_t start_value = first_value(range) + cursor.y;
            if (can_provide > need)
            {
                for (size_t i=0; i < need; i++)
                    *pos++ = (start_value + i);

                cursor.y += need;
                break;
            }
            else if (can_provide == need)
            {
                for (size_t i=0; i < need; i++)
                    *pos++ = (start_value + i);

                cursor.x++;
                cursor.y = 0;
                break;
            }
            else
            {
                for (size_t i=0; i < can_provide; i++)
                    *pos++ = (start_value + i);

                cursor.x++;
                cursor.y = 0;
            }
        }

        progress(column->size(), column->byteSize());

        return { Columns {std::move(column)}, block_size };
    }

private:
    Ranges ranges;
    UInt64 block_size;

    RangesPos cursor;
    RangesPos end; /// not included

    static Block createHeader()
    {
        return { ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number") };
    }
};

}


StorageSystemNumbers::StorageSystemNumbers(const StorageID & table_id, bool multithreaded_, std::optional<UInt64> limit_, UInt64 offset_, bool even_distribution_)
    : IStorage(table_id), multithreaded(multithreaded_), even_distribution(even_distribution_), limit(limit_), offset(offset_)
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

    if (limit && *limit < max_block_size)
    {
        max_block_size = static_cast<size_t>(*limit);
        multithreaded = false;
    }

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
        std::cout << "Ranges:\n";
        for (auto & r : ranges)
        {
            std::cout << r.toString() << std::endl;
        }

        /// Intersect ranges with table range
        Range table_range(
            FieldRef(offset), true, limit.has_value() ? FieldRef(offset + *limit) : NEGATIVE_INFINITY, limit.has_value());
        Ranges intersected_ranges;

        for (auto & r : ranges)
        {
            auto intersected_range = table_range.intersectWith(r);
            if (intersected_range)
            intersected_ranges.push_back(*intersected_range);
        }

        std::cout << "Intersected Ranges:\n";
        for (auto & r : intersected_ranges)
        {
            std::cout << r.toString() << std::endl;
        }

        /// 1. If intersected ranges is limited, use NumbersRangedSource
        if (intersected_ranges.empty() && !intersected_ranges.rbegin()->right.isPositiveInfinity())
        {
            auto size_of_range = [] (const Range & r) -> size_t
            {
                size_t size;
                size = r.right.get<UInt64>() - r.left.get<UInt64>() + 1;
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
                    total_size += size_of_range(r);
                }
                return total_size;
            };

            size_t total_size = size_of_ranges(intersected_ranges);
            num_streams = std::min(num_streams, total_size / max_block_size);

            if (num_streams == 0)
                num_streams = 1;

            /// Split ranges evenly
            NumbersRangedSource::RangesPos start({0, 0});
            for (size_t i = 0; i < num_streams; ++i)
            {
                auto need = total_size * (i + 1) / num_streams - total_size * i / num_streams;

                /// find end
                NumbersRangedSource::RangesPos end(start);
                while (need != 0)
                {
                    size_t can_provide = size_of_range(ranges[end.x]) - end.y;
                    if (can_provide > need)
                    {
                        end.y += need;
                        break;
                    }
                    else if (can_provide == need)
                    {
                        end.x++;
                        end.y = 0;
                        break;
                    }
                    else
                    {
                        end.x++;
                        end.y = 0;
                        need -= can_provide;
                    }
                }

                auto source = std::make_shared<NumbersRangedSource>(intersected_ranges, max_block_size, start, end);
                start = end;

                if (i == 0)
                {
                    auto rows_appr = total_size;
                    if (query_info.limit > 0 && query_info.limit < rows_appr)
                        rows_appr = query_info.limit;
                    source->addTotalRowsApprox(rows_appr);
                }

                pipe.addSource(std::move(source));
            }
            return pipe;
        }
    }

    /// 2. Or fall back to NumbersSource
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
