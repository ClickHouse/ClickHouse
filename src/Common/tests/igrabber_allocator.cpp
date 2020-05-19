#include <Common/Allocators/IGrabberAllocator.h>
#include <cstdio>

using namespace DB;

int main() noexcept
{
    IGrabberAllocator<int, int> cache(MMAP_THRESHOLD);

    printf("Zero cache\n");

     {
         auto pair = cache.getOrSet(0, [] {return 100; }, [](void *) { return 1; });
         auto stats = cache.getStats();

         printf("All regions: %lu, expected 1\n Used regions: %lu, expected 1\n Unused regions: %lu, expected 0\n",
                 stats.regions, stats.used_regions, stats.unused_regions);

         stats.print(std::cout);
     }

     {
         auto pair = cache.getOrSet(1, [] {return 100; }, [](void *) { return 1; });
         auto stats = cache.getStats();

         printf("All regions: %lu, expected 1\n Used regions: %lu, expected 1\n Unused regions: %lu, expected 0\n",
                 stats.regions, stats.used_regions, stats.unused_regions);

         stats.print(std::cout);
     }


     auto stats = cache.getStats();

     printf("All regions: %lu, expected 1\n Used regions: %lu, expected 0\n Unused regions: %lu, expected 1\n",
             stats.regions, stats.used_regions, stats.unused_regions);

     stats.print(std::cout);

    cache.shrinkToFit();

    cache.getStats().print(std::cout);

    std::cout << "Is nullptr:" << (cache.get(0).get() == nullptr) << "\n";
    std::cout << "Is nullptr:" << (cache.get(1).get() == nullptr) << "\n";
}

