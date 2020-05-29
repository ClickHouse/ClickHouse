#include <Common/Allocators/IGrabberAllocator.h>
#include <thread>

using namespace DB;

using Alloc = IGrabberAllocator<int, int>;

struct Holder
{
    Holder(Alloc& a, int key)
    {
        ptr = a.getOrSet(key,
                []{ return sizeof(int); },
                [](void *) {return 42;}).first;
    }

    std::shared_ptr<int> ptr;
};

int main() noexcept
{
   Alloc cache(MMAP_THRESHOLD);

   std::vector<std::thread> thread_pool;

   for (size_t k = 0; k < 8; k++)
       thread_pool.emplace_back([&cache] {
           for (int i = 1; i < 10000; ++i) {
               Holder inc(cache, i);
               Holder last(cache, i - 1);
               Holder next(cache, i + 1);
           }});

   for (auto& t : thread_pool) t.join();
}

