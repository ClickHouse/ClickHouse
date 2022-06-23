#pragma once

#include <Common/Arena.h>
#include <common/unaligned.h>


namespace DB
{

/** Can allocate memory objects of fixed size with deletion support.
  * For small `object_size`s allocated no less than pointer size.
  */
class SmallObjectPool
{
private:
    const size_t object_size;
    Arena pool;
    char * free_list = nullptr;

public:
    SmallObjectPool(size_t object_size_)
        : object_size{std::max(object_size_, sizeof(char *))}
    {
    }

    char * alloc()
    {
        if (free_list)
        {
            char * res = free_list;
            free_list = unalignedLoad<char *>(free_list);
            return res;
        }

        return pool.alloc(object_size);
    }

    void free(char * ptr)
    {
        unalignedStore<char *>(ptr, free_list);
        free_list = ptr;
    }

    /// The size of the allocated pool in bytes
    size_t size() const
    {
        return pool.size();
    }

};

}
