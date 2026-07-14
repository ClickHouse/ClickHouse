#include <Processors/Merges/Algorithms/RowRef.h>

#include <Columns/IColumn.h>
#include <Common/logger_useful.h>
#include <Core/SortCursor.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace detail
{

void RowRef::set(SortCursor & cursor)
{
    sort_columns = cursor.impl->sort_columns.data();
    num_columns = cursor.impl->sort_columns.size();
    row_num = cursor.impl->getRow();
    source_stream_index = cursor.impl->order;
}

bool RowRef::checkEquals(size_t size, const IColumn ** lhs, size_t lhs_row, const IColumn ** rhs, size_t rhs_row)
{
    for (size_t col_number = 0; col_number < size; ++col_number)
    {
        auto & cur_column = lhs[col_number];
        auto & other_column = rhs[col_number];

        if (0 != cur_column->compareAt(lhs_row, rhs_row, *other_column, 1))
            return false;
    }

    return true;
}

size_t RowRef::checkEqualsFirstNonEqual(size_t size, size_t offset, const IColumn ** lhs, size_t lhs_row, const IColumn ** rhs, size_t rhs_row)
{
    if (size > 0 && 0 != lhs[offset]->compareAt(lhs_row, rhs_row, *rhs[offset], 1))
        return offset;

    for (size_t col_number = 0; col_number < size; ++col_number)
    {
        if (col_number == offset)
            continue;

        auto & cur_column = lhs[col_number];
        auto & other_column = rhs[col_number];

        if (0 != cur_column->compareAt(lhs_row, rhs_row, *other_column, 1))
            return col_number;
    }

    return size;
}

SharedChunkPtr SharedChunkAllocator::alloc(Chunk & chunk)
{
    if (free_chunks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not enough space in SharedChunkAllocator. Chunks allocated: {}",
                        chunks.size());

    auto pos = free_chunks.back();
    free_chunks.pop_back();

    chunks[pos].swap(chunk);
    chunks[pos].position = pos;
    chunks[pos].allocator = this;

    return SharedChunkPtr(&chunks[pos]);
}

SharedChunkAllocator::~SharedChunkAllocator()
{
    if (free_chunks.size() != chunks.size())
    {
        LOG_ERROR(getLogger("SharedChunkAllocator"), "SharedChunkAllocator was destroyed before RowRef was released. StackTrace: {}", StackTrace().toString());
    }
}

void SharedChunkAllocator::release(SharedChunk * ptr) noexcept
{
    if (chunks.empty())
    {
        /// This may happen if allocator was removed before chunks.
        /// Log message and exit, because we don't want to throw exception in destructor.

        LOG_ERROR(getLogger("SharedChunkAllocator"), "SharedChunkAllocator was destroyed before RowRef was released. StackTrace: {}", StackTrace().toString());

        return;
    }

    free_chunks.push_back(ptr->position);
}

void RowRefWithOwnedChunk::set(SortCursor & cursor, SharedChunkPtr chunk)
{
    owned_chunk = std::move(chunk);
    row_num = cursor.impl->getRow();
    all_columns = &owned_chunk->all_columns;
    sort_columns = &owned_chunk->sort_columns;
    current_cursor = cursor.impl;
    source_stream_index = cursor.impl->order;
}

}

}
