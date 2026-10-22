#pragma once

#include <cstring>
#include <limits>
#include <memory>
#include <vector>
#include <Core/Defines.h>
#include <boost/noncopyable.hpp>
#include <Common/Allocator.h>
#include <Common/memcpySmall.h>
#include <base/getPageSize.h>
#include <base/arithmeticOverflow.h>

#if __has_include(<sanitizer/asan_interface.h>) && defined(ADDRESS_SANITIZER)
#   include <sanitizer/asan_interface.h>
#endif


namespace DB
{


/** Memory pool to append something. For example, short strings.
  * Usage scenario:
  * - put lot of strings inside pool, keep their addresses;
  * - addresses remain valid during lifetime of pool;
  * - at destruction of pool, all memory is freed;
  * - memory is allocated and freed by large MemoryChunks;
  * - freeing parts of data is not possible (but look at ArenaWithFreeLists if you need);
  */
class Arena : private boost::noncopyable
{
private:
    static constexpr size_t pad_right = PADDING_FOR_SIMD - 1;

    /// Contiguous MemoryChunk of memory and pointer to free space inside it. Member of single-linked list.
    struct alignas(16) MemoryChunk : private Allocator<false>    /// empty base optimization
    {
        char * begin = nullptr;
        char * pos = nullptr;
        char * end = nullptr; /// does not include padding.

        std::unique_ptr<MemoryChunk> prev;

        MemoryChunk() = default;

        void swap(MemoryChunk & other) noexcept
        {
            std::swap(begin, other.begin);
            std::swap(pos, other.pos);
            std::swap(end, other.end);
            prev.swap(other.prev);
        }

        MemoryChunk(MemoryChunk && other) noexcept
        {
            *this = std::move(other);
        }

        MemoryChunk & operator=(MemoryChunk && other) noexcept
        {
            swap(other);
            return *this;
        }

        explicit MemoryChunk(size_t size_);

        ~MemoryChunk()
        {
            if (empty())
                return;

            /// We must unpoison the memory before returning to the allocator,
            /// because the allocator might not have asan integration, and the
            /// memory would stay poisoned forever. If the allocator supports
            /// asan, it will correctly poison the memory by itself.
            ASAN_UNPOISON_MEMORY_REGION(begin, size());

            Allocator<false>::free(begin, size());
        }

        bool empty() const { return begin == end;}
        size_t size() const { return end + pad_right - begin; }
        size_t remaining() const { return end - pos; }
    };

    size_t initial_size;
    size_t growth_factor;
    size_t linear_growth_threshold;

    /// Last contiguous MemoryChunk of memory.
    MemoryChunk head;
    size_t allocated_bytes = 0;
    size_t used_bytes = 0;
    size_t page_size;

    template <typename Size>
    static Size roundUpToPageSize(Size s, size_t page_size)
    {
        return (s + page_size - 1) / page_size * page_size;
    }

    /// Uses exponential buffer growth below `linear_growth_threshold` and linear growth above it
    /// to limit unused capacity.
    template <typename Size>
    Size nextSize(Size min_next_size) const
    {
        Size size_after_grow = 0;

        if (head.empty())
        {
            size_after_grow = std::max<Size>(min_next_size, initial_size);
        }
        else if (head.size() < linear_growth_threshold)
        {
            size_after_grow = std::max(min_next_size, Size(head.size()) * growth_factor);
        }
        else
        {
            /// With `allocContinue`, appending small amounts can repeatedly copy the accumulated data
            /// into a new `MemoryChunk`. Growing by similarly small amounts would make the number of
            /// copies proportional to the serialized size, resulting in quadratic work. Rounding the
            /// next size up to `linear_growth_threshold` makes these copies less frequent.
            size_after_grow = ((min_next_size + linear_growth_threshold - 1)
                    / linear_growth_threshold) * linear_growth_threshold;
        }

        chassert(size_after_grow >= min_next_size);
        return roundUpToPageSize(size_after_grow, page_size);
    }

    /// The size of an allocation can come from the data, so it is rejected instead of wrapping around.
    [[noreturn]] static void throwTooLargeAllocation(size_t size);

    /// Add next contiguous MemoryChunk of memory with size not less than specified.
    void NO_INLINE addMemoryChunk(size_t min_size, size_t alignment = 0)
    {
        /// The alignment and the padding added here, and the rounding inside `nextSize`, would wrap
        /// around for a size close to the maximum of `size_t`, and then a chunk smaller than the
        /// requested size would be allocated. Sizes that the allocator refuses outright are cut off
        /// here as well: the size of an allocation can come from the data, so it is a data error
        /// rather than the logical error the allocator would report.
        if (min_size > MAX_ALLOCATION_SIZE - alignment - pad_right - linear_growth_threshold - page_size)
            throwTooLargeAllocation(min_size);

        size_t next_size = nextSize(min_size + alignment + pad_right);
        if (head.empty())
        {
            head = MemoryChunk(next_size);
        }
        else
        {
            auto chunk = std::make_unique<MemoryChunk>(next_size);
            head.swap(*chunk);
            head.prev = std::move(chunk);
        }
        allocated_bytes += head.size();
    }

    friend class ArenaAllocator;
    template <size_t> friend class AlignedArenaAllocator;

public:
    explicit Arena(size_t initial_size_ = 4096, size_t growth_factor_ = 2, size_t linear_growth_threshold_ = 128 * 1024 * 1024)
        : initial_size(initial_size_)
        , growth_factor(growth_factor_)
        , linear_growth_threshold(linear_growth_threshold_)
        , page_size(static_cast<size_t>(::getPageSize()))
    {
    }

    /// Bounds additional memory for `num_allocations` calls to `alloc` requesting `total_bytes` in total.
    /// Includes buffers and metadata without inspecting individual allocation sizes or changing the arena.
    /// Saturates at the maximum of `size_t` when the bound is not representable.
    size_t estimateGrowthMemory(size_t num_allocations, size_t total_bytes) const noexcept
    {
        if (num_allocations == 0 || (!head.empty() && total_bytes <= head.remaining()))
            return 0;

        /// The total covers multiple allocations, so its hypothetical buffer can exceed the allocation
        /// limit. Wider arithmetic preserves the sizing calculation until the estimate is saturated.
        constexpr size_t max_size = std::numeric_limits<size_t>::max();
        const UInt128 next_chunk_size = nextSize(UInt128(total_bytes) + pad_right);
        if (next_chunk_size > max_size)
            return max_size;

        const size_t max_next_chunk_size = static_cast<size_t>(next_chunk_size);
        /// If the smallest possible next buffer can hold the entire payload, no second buffer is needed.
        if (num_allocations == 1 || total_bytes <= nextSize(UInt128(pad_right)) - pad_right)
            return max_next_chunk_size + (head.empty() ? 0 : sizeof(MemoryChunk));

        /// Each abandoned tail is smaller than the allocation that did not fit. Therefore the usable
        /// capacity of all new buffers except the last is less than twice the requested bytes.
        /// Each buffer has at least one page, which also bounds the padding and metadata overhead.
        size_t twice_total_bytes = 0;
        if (common::mulOverflow(total_bytes, size_t(2), twice_total_bytes))
            return max_size;
        const size_t max_chunks = std::min(num_allocations, twice_total_bytes / (page_size - pad_right) + 1);

        /// A new buffer able to hold the entire payload must be the last. Its predecessor is therefore
        /// smaller than `total_bytes + pad_right`. Geometric growth can multiply that by `growth_factor`;
        /// linear rounding can at most double it. `max_next_chunk_size` also covers the first allocation.
        size_t buffer_bytes = 0;
        size_t overhead_bytes = 0;
        if (common::mulOverflow(std::max(growth_factor, size_t(2)), max_next_chunk_size, buffer_bytes)
            || common::mulOverflow(max_chunks, pad_right + sizeof(MemoryChunk), overhead_bytes)
            || common::addOverflow(buffer_bytes, twice_total_bytes, buffer_bytes)
            || common::addOverflow(buffer_bytes, overhead_bytes, buffer_bytes))
            return max_size;
        return buffer_bytes;
    }

    /// Get piece of memory, without alignment.
    /// Note: we expect it will return a non-nullptr even if the size is zero.
    char * alloc(size_t size)
    {
        used_bytes += size;
        if (unlikely(head.empty() || size > head.remaining()))
            addMemoryChunk(size);

        char * res = head.pos;
        head.pos += size;
        ASAN_UNPOISON_MEMORY_REGION(res, size + pad_right);
        return res;
    }

    /// Get piece of memory with alignment
    char * alignedAlloc(size_t size, size_t alignment)
    {
        used_bytes += size;
        if (unlikely(head.empty() || size > head.remaining()))
            addMemoryChunk(size, alignment);

        do
        {
            void * head_pos = head.pos;
            size_t space = head.end - head.pos;

            auto * res = static_cast<char *>(std::align(alignment, size, head_pos, space));
            if (res)
            {
                head.pos = static_cast<char *>(head_pos);
                head.pos += size;
                ASAN_UNPOISON_MEMORY_REGION(res, size + pad_right);
                return res;
            }

            addMemoryChunk(size, alignment);
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
        used_bytes -= size;
        head.pos -= size;
        ASAN_POISON_MEMORY_REGION(head.pos, size + pad_right);
        return head.pos;
    }

    /** Begin or expand a contiguous range of memory.
      * 'range_start' is the start of range. If nullptr, a new range is
      * allocated.
      * If there is no space in the current MemoryChunk to expand the range,
      * the entire range is copied to a new, bigger memory MemoryChunk, and the value
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
        /** Allocating zero bytes doesn't make much sense. Also, a zero-sized
          * range might break the invariant that the range begins at least before
          * the current MemoryChunk end.
          */
        chassert(additional_bytes > 0);

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
        // the current MemoryChunk.
        chassert(range_start >= head.begin);
        chassert(range_start < head.end);

        if (head.pos + additional_bytes <= head.end)
        {
            // The new size fits into the last MemoryChunk, so just alloc the
            // additional size. We can alloc without alignment here, because it
            // only applies to the start of the range, and we don't change it.
            return alloc(additional_bytes);
        }

        // New range doesn't fit into this MemoryChunk, will copy to a new one.
        //
        // Note: among other things, this method is used to provide a hack-ish
        // implementation of realloc over Arenas in ArenaAllocators. It wastes a
        // lot of memory -- quadratically so when we reach the linear allocation
        // threshold. This deficiency is intentionally left as is, and should be
        // solved not by complicating this method, but by rethinking the
        // approach to memory management for aggregate function states, so that
        // we can provide a proper realloc().
        const size_t existing_bytes = head.pos - range_start;
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

    /// Size of all MemoryChunks in bytes.
    size_t allocatedBytes() const { return allocated_bytes; }

    /// Total space actually used (not counting padding or space unused by caller allocations) in all MemoryChunks in bytes.
    size_t usedBytes() const { return used_bytes; }

    /// Bad method, don't use it -- the MemoryChunks are not your business, the entire
    /// purpose of the arena code is to manage them for you, so if you find
    /// yourself having to use this method, probably you're doing something wrong.
    size_t remainingSpaceInCurrentMemoryChunk() const
    {
        return head.remaining();
    }
};

using ArenaPtr = std::shared_ptr<Arena>;
using Arenas = std::vector<ArenaPtr>;

}
