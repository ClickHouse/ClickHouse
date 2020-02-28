#pragma once

#include <algorithm>
#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>


namespace DB
{

/// Allows you refer to the row in the block and hold the block ownership,
///  and thus avoid creating a temporary row object.
/// Do not use std::shared_ptr, since there is no need for a place for `weak_count` and `deleter`;
///  does not use Poco::SharedPtr, since you need to allocate a block and `refcount` in one piece;
///  does not use Poco::AutoPtr, since it does not have a `move` constructor and there are extra checks for nullptr;
/// The reference counter is not atomic, since it is used from one thread.
namespace detail
{
struct SharedChunk : Chunk
{
    int refcount = 0;

    ColumnRawPtrs all_columns;
    ColumnRawPtrs sort_columns;

    SharedChunk(Chunk && chunk) : Chunk(std::move(chunk)) {}


    bool sortColumnsEqualAt(size_t row_num, size_t other_row_num, const detail::SharedChunk & other) const
    {
        size_t size = sort_columns.size();
        for (size_t i = 0; i < size; ++i)
            if (0 != sort_columns[i]->compareAt(row_num, other_row_num, *other.sort_columns[i], 1))
                return false;
        return true;
    }
};

}

using SharedChunkPtr = std::shared_ptr<detail::SharedChunk>;


}
