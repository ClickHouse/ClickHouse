#pragma once

#include <string.h>
#include <memory>
#include <vector>
#include <boost/noncopyable.hpp>
#include <Core/Defines.h>
#if __has_include(<sanitizer/asan_interface.h>) && defined(ADDRESS_SANITIZER)
#   include <sanitizer/asan_interface.h>
#endif
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
    struct alignas(16) Chunk : private Allocator<false>    /// empty base optimization
    {
        char * begin;
        char * pos;
        char * end; /// does not include padding.

        Chunk * prev;

        Chunk(size_t size_, Chunk * prev_)
        {
            ProfileEvents::increment(ProfileEvents::ArenaAllocChunks);
            ProfileEvents::increment(ProfileEvents::ArenaAllocBytes, size_);

            begin = reinterpret_cast<char *>(Allocator<false>::alloc(size_));
            pos = begin;
            end = begin + size_ - pad_right;
            prev = prev_;

            ASAN_POISON_MEMORY_REGION(begin, size_);
        }

        ~Chunk()
        {
            /// We must unpoison the memory before returning to the allocator,
            /// because the allocator might not have asan integration, and the
            /// memory would stay poisoned forever. If the allocator supports
            /// asan, it will correctly poison the memory by itself.
            ASAN_UNPOISON_MEMORY_REGION(begin, size());

            Allocator<false>::free(begin, size());

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
        {
            size_after_grow = std::max(min_next_size, head->size() * growth_factor);
        }
        else
        {
            // allocContinue() combined with linear growth results in quadratic
            // behavior: we append the data by small amounts, and when it
            // doesn't fit, we create a new chunk and copy all the previous data
            // into it. The number of times we do this is directly proportional
            // to the total size of data that is going to be serialized. To make
            // the copying happen less often, round the next size up to the
            // linear_growth_threshold.
            size_after_grow = ((min_next_size + linear_growth_threshold - 1)
                    / linear_growth_threshold) * linear_growth_threshold;
        }

        assert(size_after_grow >= min_next_size);
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
        ASAN_UNPOISON_MEMORY_REGION(res, size + pad_right);
        return res;
    }

    /// Get piece of memory with alignment
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
                ASAN_UNPOISON_MEMORY_REGION(res, size + pad_right);
                return res;
            }

            addChunk(size + alignment);
        } while (true);
    }

    template <typename T>
    T * alloc()
    {
        return reinterpret_cast<T *>(alignedAlloc(sizeof(T), alignof(T)));
    }

    /** Rollback just performed allocation.
      * Must pass size not more that was just allocated.
      * Return the resulting head pointer, so that the caller can assert that
      * the allocation it intended to roll back was indeed the last one.
      */
    void * rollback(size_t size)
    {
        head->pos -= size;
        ASAN_POISON_MEMORY_REGION(head->pos, size + pad_right);
        return head->pos;
    }

    /** Begin or expand a contiguous range of memory.
      * 'range_start' is the start of range. If nullptr, a new range is
      * allocated.
      * If there is no space in the current chunk to expand the range,
      * the entire range is copied to a new, bigger memory chunk, and the value
      * of 'range_start' is updated.
      * If the optional 'start_alignment' is specified, the start of range is
      * kept aligned to this value.
      *
      * NOTE This method is usable only for the last allocation made on this
      * Arena. For earlier allocations, see 'realloc' method.
      */
    char * allocContinue(size_t additional_bytes, char const *& range_start,
                         size_t start_alignment = 0)
    {
        /*
         * Allocating zero bytes doesn't make much sense. Also, a zero-sized
         * range might break the invariant that the range begins at least before
         * the current chunk end.
         */
        assert(additional_bytes > 0);

        if (!range_start)
        {
            // Start a new memory range.
            char * result = start_alignment
                ? alignedAlloc(additional_bytes, start_alignment)
                : alloc(additional_bytes);

            range_start = result;
            return result;
        }

        // Extend an existing memory range with 'additional_bytes'.

        // This method only works for extending the last allocation. For lack of
        // original size, check a weaker condition: that 'begin' is at least in
        // the current Chunk.
        assert(range_start >= head->begin);
        assert(range_start < head->end);

        if (head->pos + additional_bytes <= head->end)
        {
            // The new size fits into the last chunk, so just alloc the
            // additional size. We can alloc without alignment here, because it
            // only applies to the start of the range, and we don't change it.
            return alloc(additional_bytes);
        }

        // New range doesn't fit into this chunk, will copy to a new one.
        //
        // Note: among other things, this method is used to provide a hack-ish
        // implementation of realloc over Arenas in ArenaAllocators. It wastes a
        // lot of memory -- quadratically so when we reach the linear allocation
        // threshold. This deficiency is intentionally left as is, and should be
        // solved not by complicating this method, but by rethinking the
        // approach to memory management for aggregate function states, so that
        // we can provide a proper realloc().
        const size_t existing_bytes = head->pos - range_start;
        const size_t new_bytes = existing_bytes + additional_bytes;
        const char * old_range = range_start;

        char * new_range = start_alignment
            ? alignedAlloc(new_bytes, start_alignment)
            : alloc(new_bytes);

        memcpy(new_range, old_range, existing_bytes);

        range_start = new_range;
        return new_range + existing_bytes;
    }

    /// NOTE Old memory region is wasted.
    char * realloc(const char * old_data, size_t old_size, size_t new_size)
    {
        char * res = alloc(new_size);
        if (old_data)
        {
            memcpy(res, old_data, old_size);
            ASAN_POISON_MEMORY_REGION(old_data, old_size);
        }
        return res;
    }

    char * alignedRealloc(const char * old_data, size_t old_size, size_t new_size, size_t alignment)
    {
        char * res = alignedAlloc(new_size, alignment);
        if (old_data)
        {
            memcpy(res, old_data, old_size);
            ASAN_POISON_MEMORY_REGION(old_data, old_size);
        }
        return res;
    }

    /// Insert string without alignment.
    const char * insert(const char * data, size_t size)
    {
        char * res = alloc(size);
        memcpy(res, data, size);
        return res;
    }

    const char * alignedInsert(const char * data, size_t size, size_t alignment)
    {
        char * res = alignedAlloc(size, alignment);
        memcpy(res, data, size);
        return res;
    }

    /// Size of chunks in bytes.
    size_t size() const
    {
        return size_in_bytes;
    }

    /// Bad method, don't use it -- the chunks are not your business, the entire
    /// purpose of the arena code is to manage them for you, so if you find
    /// yourself having to use this method, probably you're doing something wrong.
    size_t remainingSpaceInCurrentChunk() const
    {
        return head->remaining();
    }
};

using ArenaPtr = std::shared_ptr<Arena>;
using Arenas = std::vector<ArenaPtr>;


}
