#pragma once

#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
#include <Core/SortCursor.h>
#include <Common/StackTrace.h>
#include <common/logger_useful.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::detail
{

class SharedChunkAllocator;

/// Allows you refer to the row in the block and hold the block ownership,
///  and thus avoid creating a temporary row object.
/// Do not use std::shared_ptr, since there is no need for a place for `weak_count` and `deleter`;
///  does not use Poco::SharedPtr, since you need to allocate a block and `refcount` in one piece;
///  does not use Poco::AutoPtr, since it does not have a `move` constructor and there are extra checks for nullptr;
/// The reference counter is not atomic, since it is used from one thread.
struct SharedChunk : Chunk
{
    ColumnRawPtrs all_columns;
    ColumnRawPtrs sort_columns;

    using Chunk::Chunk;
    using Chunk::operator=;

private:
    int refcount = 0;
    size_t position = 0;
    SharedChunkAllocator * allocator = nullptr;

    friend class SharedChunkAllocator;
    friend void intrusive_ptr_add_ref(SharedChunk * ptr);
    friend void intrusive_ptr_release(SharedChunk * ptr);
};

using SharedChunkPtr = boost::intrusive_ptr<detail::SharedChunk>;

/// Custom allocator for shared chunk.
/// It helps to avoid explicit new/delete calls if we know maximum required capacity.
/// Thanks to that, SharedChunk does not own any memory.
/// It improves leaks detection, because memory is allocated only once in constructor.
class SharedChunkAllocator
{
public:
    explicit SharedChunkAllocator(size_t max_chunks)
    {
        if (max_chunks == 0)
            max_chunks = 1;

        chunks.resize(max_chunks);
        free_chunks.reserve(max_chunks);

        for (size_t i = 0; i < max_chunks; ++i)
            free_chunks.push_back(i);
    }

    SharedChunkPtr alloc(Chunk & chunk)
    {
        if (free_chunks.empty())
            throw Exception("Not enough space in SharedChunkAllocator. "
                            "Chunks allocated: " + std::to_string(chunks.size()), ErrorCodes::LOGICAL_ERROR);

        auto pos = free_chunks.back();
        free_chunks.pop_back();

        chunks[pos].swap(chunk);
        chunks[pos].position = pos;
        chunks[pos].allocator = this;

        return SharedChunkPtr(&chunks[pos]);
    }

    ~SharedChunkAllocator()
    {
        if (free_chunks.size() != chunks.size())
        {
            LOG_ERROR(&Poco::Logger::get("SharedChunkAllocator"), "SharedChunkAllocator was destroyed before RowRef was released. StackTrace: {}", StackTrace().toString());

            return;
        }
    }

private:
    std::vector<SharedChunk> chunks;
    std::vector<size_t> free_chunks;

    void release(SharedChunk * ptr) noexcept
    {
        if (chunks.empty())
        {
            /// This may happen if allocator was removed before chunks.
            /// Log message and exit, because we don't want to throw exception in destructor.

            LOG_ERROR(&Poco::Logger::get("SharedChunkAllocator"), "SharedChunkAllocator was destroyed before RowRef was released. StackTrace: {}", StackTrace().toString());

            return;
        }

        free_chunks.push_back(ptr->position);
    }

    friend void intrusive_ptr_release(SharedChunk * ptr);
};

inline void intrusive_ptr_add_ref(SharedChunk * ptr)
{
    ++ptr->refcount;
}

inline void intrusive_ptr_release(SharedChunk * ptr)
{
    if (0 == --ptr->refcount)
        ptr->allocator->release(ptr);
}

/// This class represents a row in a chunk.
struct RowRef
{
    const IColumn ** sort_columns = nullptr; /// Point to sort_columns from SortCursor or last_chunk_sort_columns.
    size_t num_columns = 0;
    UInt64 row_num = 0;

    bool empty() const { return sort_columns == nullptr; }
    void reset() { sort_columns = nullptr; }

    void set(SortCursor & cursor)
    {
        sort_columns = cursor.impl->sort_columns.data();
        num_columns = cursor.impl->sort_columns.size();
        row_num = cursor.impl->getRow();
    }

    static bool checkEquals(size_t size, const IColumn ** lhs, size_t lhs_row, const IColumn ** rhs, size_t rhs_row)
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

    bool hasEqualSortColumnsWith(const RowRef & other)
    {
        return checkEquals(num_columns, sort_columns, row_num, other.sort_columns, other.row_num);
    }
};

/// This class also represents a row in a chunk.
/// RowRefWithOwnedChunk hold shared pointer to this chunk, possibly extending its life time.
/// It is needed, for example, in CollapsingTransform, where we need to store first negative row for current sort key.
/// We do not copy data itself, because it may be potentially changed for each row. Performance for `set` is important.
struct RowRefWithOwnedChunk
{
    detail::SharedChunkPtr owned_chunk = nullptr;

    ColumnRawPtrs * all_columns = nullptr;
    ColumnRawPtrs * sort_columns = nullptr;
    UInt64 row_num = 0;

    void swap(RowRefWithOwnedChunk & other)
    {
        owned_chunk.swap(other.owned_chunk);
        std::swap(all_columns, other.all_columns);
        std::swap(sort_columns, other.sort_columns);
        std::swap(row_num, other.row_num);
    }

    bool empty() const { return owned_chunk == nullptr; }

    void clear()
    {
        owned_chunk.reset();
        all_columns = nullptr;
        sort_columns = nullptr;
        row_num = 0;
    }

    void set(SortCursor & cursor, SharedChunkPtr chunk)
    {
        owned_chunk = std::move(chunk);
        row_num = cursor.impl->getRow();
        all_columns = &owned_chunk->all_columns;
        sort_columns = &owned_chunk->sort_columns;
    }

    bool hasEqualSortColumnsWith(const RowRefWithOwnedChunk & other)
    {
        return RowRef::checkEquals(sort_columns->size(), sort_columns->data(), row_num,
                                   other.sort_columns->data(), other.row_num);
    }
};

}
