#include <iostream>
#include <Common/Allocators/IGrabberAllocator.h>

using namespace DB;

int main() noexcept
{
    IGrabberAllocator<int, int> cache(MMAP_THRESHOLD);

     for (int i = 0; i < 5; ++i)
     {
       auto pair = cache.getOrSet(i,
                    [] {return 100; },
                    [](void *) { return 1; });
       std::cout << pair.first << " " << pair.second << "\n";
     }

    cache.getStats().print(std::cout);

    cache.shrinkToFit();

    cache.getStats().print(std::cout);

    for (int i = 0; i < 5; ++i)
        std::cout << cache.get(i).get() << "\n";
}

