#include <Common/Allocators/IGrabberAllocator.h>
#include <cstdio>

using namespace DB;

int main() noexcept
{
    IGrabberAllocator<int, int> cache(MMAP_THRESHOLD);

    const auto size = [] {return 100; };
    const auto init = [](void *) {return 100; };

    auto&& [ptr, produced] = cache.getOrSet(0, size, init);

    {
        cache.getOrSet(1, size, init);
    }

    cache.getStats().print(std::cout);

    cache.shrinkToFit();

    cache.getStats().print(std::cout);

    std::cout << ptr.get() << " " << cache.get(0).get() << "\n";
    std::cout << nullptr << " " << cache.get(1).get() << "\n";
}

