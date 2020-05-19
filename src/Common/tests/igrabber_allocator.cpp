#include <iostream>
#include <Common/Allocators/IGrabberAllocator.h>

using namespace DB;

int main() noexcept
{
    IGrabberAllocator<int, int> cache(MMAP_THRESHOLD);

    cache.getStats().print(std::cout);

    std::cout << (cache.get(0) == nullptr) << "\n";

    ga::Stats stats;

    {
        auto&& [ptr, produced] = cache.getOrSet(0,
                []{ return 200; },
                [](void *) {return 100;});

        std::cout << produced << "\n"; //true

        stats = cache.getStats();

        std::cout << stats.misses << " " << 1 << "\n";
        std::cout << stats.used_regions_count << " " << 1 << "\n";
        std::cout << stats.all_regions_count << " " << 1 << "\n";

        auto ptr2 = cache.get(0);

        stats = cache.getStats();

        std::cout << ptr.get() << " " << ptr2.get() << "\n";
        std::cout << stats.misses << " " << 1 << "\n";
        std::cout << stats.hits << " " << 1 << "\n";
        std::cout << stats.used_regions_count << " " << 1 << "\n";
        std::cout << stats.all_regions_count << " " << 1 << "\n";
    }

    stats = cache.getStats();

    std::cout << stats.keyed_regions_count << " " << 0 << "\n";
    std::cout << stats.all_regions_count << " " << 1 << "\n";

    cache.getStats().print(std::cout);
}

