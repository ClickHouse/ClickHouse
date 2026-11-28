#pragma once

#include <atomic>
#include <array>
#include <Common/ZooKeeper/IKeeper.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>


namespace Coordination
{

class ErrorCounter
{
private:
    static constexpr int32_t MIN_ERROR_VALUE = static_cast<int32_t>(Error::MIN);
    static constexpr int32_t MAX_ERROR_VALUE = static_cast<int32_t>(Error::MAX);

    static constexpr size_t ARRAY_SIZE = MAX_ERROR_VALUE - MIN_ERROR_VALUE + 1;
    std::array<std::atomic<uint32_t>, ARRAY_SIZE> error_counts{};

    static size_t errorToIndex(Coordination::Error error)
    {
        const int32_t code = static_cast<int32_t>(error);
        chassert(code >= MIN_ERROR_VALUE && code <= MAX_ERROR_VALUE);
        return static_cast<size_t>(code - MIN_ERROR_VALUE);
    }

    static Coordination::Error indexToError(size_t index)
    {
        return static_cast<Coordination::Error>(static_cast<int32_t>(index) + MIN_ERROR_VALUE);
    }

public:
    void increment(Coordination::Error error)
    {
        error_counts[errorToIndex(error)].fetch_add(1, std::memory_order_relaxed);
    }

    void dumpToMapColumn(DB::ColumnMap * column) const
    {
        auto & offsets = column->getNestedColumn().getOffsets();
        auto & tuple_column = column->getNestedData();
        auto & key_column = tuple_column.getColumn(0);
        auto & value_column = tuple_column.getColumn(1);

        size_t count = 0;
        for (size_t i = 0; i < ARRAY_SIZE; ++i)
        {
            if (const uint32_t value = error_counts[i].load(std::memory_order_relaxed))
            {
                key_column.insert(indexToError(i));
                value_column.insert(value);
                ++count;
            }
        }

        offsets.push_back(offsets.back() + count);
    }
};

}
