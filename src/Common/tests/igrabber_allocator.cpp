#include <Common/Allocators/IGrabberAllocator.h>
#include <cstdio>

using namespace DB;

int main() noexcept
{
    IGrabberAllocator<int, int> cache(MMAP_THRESHOLD);

    const auto size = [] {return 100; };
    const auto init = [](void *) {return 100; };

    auto ptr = cache.getOrSet(0, size, init).first;
    cache.shrinkToFit();

    auto ptr2 = cache.get(0);

    std::cout << ptr.get() << " " << ptr2.get() << "\n";
}

