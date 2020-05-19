#include <Common/Allocators/IGrabberAllocator.h>

int main() noexcept
{
    DB::IGrabberAllocator<int, int> cache(MMAP_THRESHOLD);

    const auto init = [](void *) {return 100; };

    auto ptr = cache.getOrSet(0, [] {return 100; }, init).first;
    cache.shrinkToFit();

    auto ptr2 = cache.get(0);

    std::cout << ptr.get() << " " << ptr2.get() << "\n";
}
