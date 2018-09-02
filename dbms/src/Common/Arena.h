#pragma once

#include <string.h>
#include <memory>
#include <vector>
#include <boost/noncopyable.hpp>
#include <common/likely.h>
#include <Core/Defines.h>
#include <Common/memcpySmall.h>
#include <Common/ProfileEvents.h>
#include <Common/Allocator.h>


namespace ProfileEvents
{
    extern const Event ArenaAllocChunks;
    extern const Event ArenaAllocBytes;
}

namespace DB
{


/** Memory pool to append something. For example, short strings.
  * Usage scenario:
  * - put lot of strings inside pool, keep their addresses;
  * - addresses remain valid during lifetime of pool;
  * - at destruction of pool, all memory is freed;
  * - memory is allocated and freed by large chunks;
  * - freeing parts of data is not possible (but look at ArenaWithFreeLists if you need);
  */
class Arena : private boost::noncopyable
{
private:
    /// Padding allows to use 'memcpySmallAllowReadWriteOverflow15' instead of 'memcpy'.
    static constexpr size_t pad_right = 15;

    /// Contiguous chunk of memory and pointer to free space inside it. Member of single-linked list.
    struct Chunk : private Allocator<false>    /// empty base optimization
    {
        char * begin;
        char * pos;
        char * end; /// does not include padding.

        Chunk * prev;

        Chunk(size_t size_, Chunk * prev_)
        {
            ProfileEvents::increment(ProfileEvents::ArenaAllocChunks);
            ProfileEvents::increment(ProfileEvents::ArenaAllocBytes, size_);

            begin = reinterpret_cast<char *>(Allocator::alloc(size_));
            pos = begin;
            end = begin + size_ - pad_right;
            prev = prev_;
        }

        ~Chunk()
        {
            Allocator::free(begin, size());

            if (prev)
                delete prev;
        }

        size_t size() const { return end + pad_right - begin; }
        size_t remaining() const { return end - pos; }
    };

    size_t growth_factor;
    size_t linear_growth_threshold;

    /// Last contiguous chunk of memory.
    Chunk * head;
    size_t size_in_bytes;

    static size_t roundUpToPageSize(size_t s)
    {
        return (s + 4096 - 1) / 4096 * 4096;
    }

    /// If chunks size is less than 'linear_growth_threshold', then use exponential growth, otherwise - linear growth
    ///  (to not allocate too much excessive memory).
    size_t nextSize(size_t min_next_size) const
    {
        size_t size_after_grow = 0;

        if (head->size() < linear_growth_threshold)
            size_after_grow = head->size() * growth_factor;
        else
            size_after_grow = linear_growth_threshold;

        if (size_after_grow < min_next_size)
            size_after_grow = min_next_size;

        return roundUpToPageSize(size_after_grow);
    }

    /// Add next contiguous chunk of memory with size not less than specified.
    void NO_INLINE addChunk(size_t min_size)
    {
        head = new Chunk(nextSize(min_size + pad_right), head);
        size_in_bytes += head->size();
    }

    friend class ArenaAllocator;
    template <size_t> friend class AlignedArenaAllocator;

public:
    Arena(size_t initial_size_ = 4096, size_t growth_factor_ = 2, size_t linear_growth_threshold_ = 128 * 1024 * 1024)
        : growth_factor(growth_factor_), linear_growth_threshold(linear_growth_threshold_),
        head(new Chunk(initial_size_, nullptr)), size_in_bytes(head->size())
    {
    }

    ~Arena()
    {
        delete head;
    }

    /// Get piece of memory, without alignment.
    char * alloc(size_t size)
    {
        if (unlikely(head->pos + size > head->end))
            addChunk(size);

        char * res = head->pos;
        head->pos += size;
        return res;
    }

    /// Get peice of memory with alignment
    char * alignedAlloc(size_t size, size_t alignment)
    {
        do
        {
            void * head_pos = head->pos;
            size_t space = head->end - head->pos;

            auto res = static_cast<char *>(std::align(alignment, size, head_pos, space));
            if (res)
            {
                head->pos = static_cast<char *>(head_pos);
                head->pos += size;
                return res;
            }

            addChunk(size + alignment);
        } while (true);
    }

    /** Rollback just performed allocation.
      * Must pass size not more that was just allocated.
      */
    void rollback(size_t size)
    {
        head->pos -= size;
    }

    /** Begin or expand allocation of contiguous piece of memory without alignment.
      * 'begin' - current begin of piece of memory, if it need to be expanded, or nullptr, if it need to be started.
      * If there is no space in chunk to expand current piece of memory - then copy all piece to new chunk and change value of 'begin'.
      * NOTE This method is usable only for latest allocation. For earlier allocations, see 'realloc' method.
      */
    char * allocContinue(size_t size, char const *& begin)
    {
        while (unlikely(head->pos + size > head->end))
        {
            char * prev_end = head->pos;
            addChunk(size);

            if (begin)
                begin = insert(begin, prev_end - begin);
            else
                break;
        }

        char * res = head->pos;
        head->pos += size;

        if (!begin)
            begin = res;

        return res;
    }

    char * alignedAllocContinue(size_t size, char const *& begin, size_t alignment)
    {
        char * res;

        do
        {
            void * head_pos = head->pos;
            size_t space = head->end - head->pos;

            res = static_cast<char *>(std::align(alignment, size, head_pos, space));
            if (res)
            {
                head->pos = static_cast<char *>(head_pos);
                head->pos += size;
                break;
            }

            char * prev_end = head->pos;
            addChunk(size + alignment);

            if (begin)
                begin = alignedInsert(begin, prev_end - begin, alignment);
            else
                break;
        } while (true);

        if (!begin)
            begin = res;
        return res;
    }

    /// NOTE Old memory region is wasted.
    char * realloc(const char * old_data, size_t old_size, size_t new_size)
    {
        char * res = alloc(new_size);
        if (old_data)
            memcpySmallAllowReadWriteOverflow15(res, old_data, old_size);
        return res;
    }

    char * alignedRealloc(const char * old_data, size_t old_size, size_t new_size, size_t alignment)
    {
        char * res = alignedAlloc(new_size, alignment);
        if (old_data)
            memcpySmallAllowReadWriteOverflow15(res, old_data, old_size);
        return res;
    }

    /// Insert string without alignment.
    const char * insert(const char * data, size_t size)
    {
        char * res = alloc(size);
        memcpySmallAllowReadWriteOverflow15(res, data, size);
        return res;
    }

    const char * alignedInsert(const char * data, size_t size, size_t alignment)
    {
        char * res = alignedAlloc(size, alignment);
        memcpySmallAllowReadWriteOverflow15(res, data, size);
        return res;
    }

    /// Size of chunks in bytes.
    size_t size() const
    {
        return size_in_bytes;
    }

    size_t remainingSpaceInCurrentChunk() const
    {
        return head->remaining();
    }
};

using ArenaPtr = std::shared_ptr<Arena>;
using Arenas = std::vector<ArenaPtr>;


}
