#include <iostream>
#include <Common/Allocator.h>

int main()
{
    Allocator<true> alloc;

    if (1)
    {
        size_t size = 50000000;
        auto p = alloc.alloc(size);
        size_t old_size = size;
        for (; size < 1000000000; size += 50000000)
        {
            p = alloc.realloc(p, old_size, size);
            old_size = size;
        }
        alloc.free(p, old_size);
        std::cerr << "50mb+50mb+.. ok.\n";
    }


    {
        size_t size = 1000000000;
        auto p = alloc.alloc(size);
        size_t old_size = size;
        //try
        //{
        // Now possible jump 65mb->63mb
            for (; size > 1000; size /= 2)
            {
                p = alloc.realloc(p, old_size, size);
                old_size = size;
            }
        /* }
        catch (...)
        {
            size = old_size;
            std::cerr << "ok. impossible catch.\n";
        } */
        alloc.free(p, old_size);
        std::cerr << "1gb,512mb,128mb,.. ok.\n";
    }


    if (1)
    {
        size_t size = 1;
        auto p = alloc.alloc(size);
        size_t old_size = size;
        for (; size < 1000000000; size *= 2)
        {
            p = alloc.realloc(p, old_size, size);
            old_size = size;
        }
        alloc.free(p, old_size);
        std::cerr << "1,2,4,8,..,1G  ok.\n";
    }

    std::cerr << "ok.\n";
}
