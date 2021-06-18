#pragma once

#include <Common/Arena.h>
#include <ext/range.h>
#include <ext/size.h>
#include <ext/bit_cast.h>
#include <cstdlib>
#include <memory>


namespace DB
{


/** Can allocate memory objects of fixed size with deletion support.
 *    For small `object_size`s allocated no less than getMinAllocationSize() bytes. */
class SmallObjectPool
{
private:
    struct Block { Block * next; };
    static constexpr auto getMinAllocationSize() { return sizeof(Block); }

    const size_t object_size;
    Arena pool;
    Block * free_list{};

public:
    SmallObjectPool(
        const size_t object_size_, const size_t initial_size = 4096, const size_t growth_factor = 2,
        const size_t linear_growth_threshold = 128 * 1024 * 1024)
        : object_size{std::max(object_size_, getMinAllocationSize())},
          pool{initial_size, growth_factor, linear_growth_threshold}
    {
        if (pool.size() < object_size)
            return;

        const auto num_objects = pool.size() / object_size;
        auto head = free_list = ext::bit_cast<Block *>(pool.alloc(num_objects * object_size));

        for (const auto i : ext::range(0, num_objects - 1))
        {
            (void) i;
            head->next = ext::bit_cast<Block *>(ext::bit_cast<char *>(head) + object_size);
            head = head->next;
        }

        head->next = nullptr;
    }

    char * alloc()
    {
        if (free_list)
        {
            const auto res = reinterpret_cast<char *>(free_list);
            free_list = free_list->next;
            return res;
        }

        return pool.alloc(object_size);
    }

    void free(const void * ptr)
    {
        union
        {
            const void * p_v;
            Block * block;
        };

        p_v = ptr;
        block->next = free_list;

        free_list = block;
    }

    /// The size of the allocated pool in bytes
    size_t size() const
    {
        return pool.size();
    }

};


}
