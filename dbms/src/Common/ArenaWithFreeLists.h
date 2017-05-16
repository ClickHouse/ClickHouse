#pragma once

#include <Common/Arena.h>
#include <Common/BitHelpers.h>


namespace DB
{


/** Unlike Arena, allows you to release (for later re-use)
  *  previously allocated (not necessarily just recently) chunks of memory.
  * For this, the requested size is rounded up to the power of two
  *  (or up to 8, if less, or using memory allocation outside Arena if the size is greater than 65536).
  * When freeing memory, for each size (14 options in all: 8, 16 ... 65536),
  *  a single-linked list of free blocks is kept track.
  * When allocating, we take the head of the list of free blocks,
  *  or, if the list is empty - allocate a new block using Arena.
  */
class ArenaWithFreeLists : private Allocator<false>, private boost::noncopyable
{
private:
    /// If the block is free, then the pointer to the next free block is stored at its beginning, or nullptr, if there are no more free blocks.
    /// If the block is used, then some data is stored in it.
    union Block
    {
        Block * next;
        char data[0];
    };

    /// The maximum size of a piece of memory that is allocated with Arena. Otherwise, we use Allocator directly.
    static constexpr size_t max_fixed_block_size = 65536;

    /// Get the index in the freelist array for the specified size.
    static size_t findFreeListIndex(const size_t size)
    {
        return size <= 8 ? 2 : bitScanReverse(size - 1);
    }

    /// Arena is used to allocate blocks that are not too large.
    Arena pool;

    /// Lists of free blocks. Each element points to the head of the corresponding list, or is nullptr.
    /// The first two elements are not used, but are intended to simplify arithmetic.
    Block * free_lists[16] {};

public:
    ArenaWithFreeLists(
        const size_t initial_size = 4096, const size_t growth_factor = 2,
        const size_t linear_growth_threshold = 128 * 1024 * 1024)
        : pool{initial_size, growth_factor, linear_growth_threshold}
    {
    }

    char * alloc(const size_t size)
    {
        if (size > max_fixed_block_size)
            return static_cast<char *>(Allocator::alloc(size));

        /// find list of required size
        const auto list_idx = findFreeListIndex(size);

        /// If there is a free block.
        if (auto & free_block_ptr = free_lists[list_idx])
        {
            /// Let's take it. And change the head of the list to the next item in the list.
            const auto res = free_block_ptr->data;
            free_block_ptr = free_block_ptr->next;
            return res;
        }

        /// no block of corresponding size, allocate a new one
        return pool.alloc(1 << (list_idx + 1));
    }

    void free(char * ptr, const size_t size)
    {
        if (size > max_fixed_block_size)
            return Allocator::free(ptr, size);

        /// find list of required size
        const auto list_idx = findFreeListIndex(size);

        /// Insert the released block into the head of the list.
        auto & free_block_ptr = free_lists[list_idx];
        const auto old_head = free_block_ptr;
        free_block_ptr = reinterpret_cast<Block *>(ptr);
        free_block_ptr->next = old_head;
    }

    /// Size of the allocated pool in bytes
    size_t size() const
    {
        return pool.size();
    }
};


}
