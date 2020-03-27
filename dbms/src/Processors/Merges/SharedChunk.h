#pragma once

#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <Core/SortCursor.h>


namespace DB::detail
{

/// Allows you refer to the row in the block and hold the block ownership,
///  and thus avoid creating a temporary row object.
/// Do not use std::shared_ptr, since there is no need for a place for `weak_count` and `deleter`;
///  does not use Poco::SharedPtr, since you need to allocate a block and `refcount` in one piece;
///  does not use Poco::AutoPtr, since it does not have a `move` constructor and there are extra checks for nullptr;
/// The reference counter is not atomic, since it is used from one thread.
struct SharedChunk : Chunk
{
    int refcount = 0;

    ColumnRawPtrs all_columns;
    ColumnRawPtrs sort_columns;

    explicit SharedChunk(Chunk && chunk) : Chunk(std::move(chunk)) {}
};

inline void intrusive_ptr_add_ref(detail::SharedChunk * ptr)
{
    ++ptr->refcount;
}

inline void intrusive_ptr_release(detail::SharedChunk * ptr)
{
    if (0 == --ptr->refcount)
        delete ptr;
}

using SharedChunkPtr = boost::intrusive_ptr<detail::SharedChunk>;

struct RowRef
{
    detail::SharedChunkPtr owned_chunk;

    ColumnRawPtrs * all_columns = nullptr;
    ColumnRawPtrs * sort_columns = nullptr;
    UInt64 row_num = 0;

    void swap(RowRef & other)
    {
        owned_chunk.swap(other.owned_chunk);
        std::swap(all_columns, other.all_columns);
        std::swap(sort_columns, other.sort_columns);
        std::swap(row_num, other.row_num);
    }

    bool empty() const { return all_columns == nullptr; }

    void set(SortCursor & cursor, SharedChunkPtr chunk)
    {
        owned_chunk = std::move(chunk);
        row_num = cursor.impl->pos;
        all_columns = &owned_chunk->all_columns;
        sort_columns = &owned_chunk->sort_columns;
    }

    bool hasEqualSortColumnsWith(const RowRef & other)
    {
        auto size = sort_columns->size();
        for (size_t col_number = 0; col_number < size; ++col_number)
        {
            auto & cur_column = (*sort_columns)[col_number];
            auto & other_column = (*other.sort_columns)[col_number];

            if (0 != cur_column->compareAt(row_num, other.row_num, *other_column, 1))
                return false;
        }

        return true;
    }
};

}
