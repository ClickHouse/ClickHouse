#pragma once

#include <mutex>
#include <Common/ZooKeeper/IKeeper.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>


namespace Coordination
{

class ErrorCounter
{
private:
    mutable std::mutex mutex;
    std::unordered_map<Coordination::Error, UInt32> errors TSA_GUARDED_BY(mutex);

public:
    void increment(Coordination::Error error)
    {
        std::lock_guard lock(mutex);
        ++errors[error];
    }

    void dumpToMapColumn(DB::ColumnMap * column) const
    {
        auto & offsets = column->getNestedColumn().getOffsets();
        auto & tuple_column = column->getNestedData();
        auto & key_column = tuple_column.getColumn(0);
        auto & value_column = tuple_column.getColumn(1);

        std::lock_guard lock(mutex);

        for (const auto & [error, count] : errors)
        {
            key_column.insert(error);
            value_column.insert(count);
        }

        offsets.push_back(offsets.back() + errors.size());
    }
};

}
