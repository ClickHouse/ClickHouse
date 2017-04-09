#include <iostream>
#include <Common/ArrayCache.h>


int main(int argc, char ** argv)
{
    using Cache = ArrayCache<int, int>;
    Cache cache(1024 * 1024 * 1024);

    Cache::HolderPtr holder = cache.getOrSet(1, []{ return 100; }, [](void * ptr, int & payload){}, nullptr);

    return 0;
}
