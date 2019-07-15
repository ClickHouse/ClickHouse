#include <map>
#include <vector>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <iostream>

#include <Common/config.h>

//#undef USE_MIMALLOC
//#define USE_MIMALLOC 0

#if USE_MIMALLOC

#include <mimalloc.h>
#define malloc mi_malloc
#define free mi_free

#else

#include <stdlib.h>

#endif


size_t total_size{0};

struct Allocation
{
    void * ptr = nullptr;
    size_t size = 0;

    Allocation() {}

    Allocation(size_t size)
        : size(size)
    {
        ptr = malloc(size);
        if (!ptr)
            throw std::runtime_error("Cannot allocate memory");
        total_size += size;
    }

    ~Allocation()
    {
        if (ptr)
        {
            free(ptr);
            total_size -= size;
        }
        ptr = nullptr;
    }

    Allocation(const Allocation &) = delete;

    Allocation(Allocation && rhs)
    {
        ptr = rhs.ptr;
        size = rhs.size;
        rhs.ptr = nullptr;
        rhs.size = 0;
    }
};


int main(int, char **)
{
    std::vector<Allocation> allocations;

    constexpr size_t limit = 100000000;
    constexpr size_t min_alloc_size = 65536;
    constexpr size_t max_alloc_size = 10000000;

    std::mt19937 rng;
    auto distribution = std::uniform_int_distribution(min_alloc_size, max_alloc_size);

    size_t total_allocations = 0;

    while (true)
    {
        size_t size = distribution(rng);

        while (total_size + size > limit)
            allocations.pop_back();

        allocations.emplace_back(size);

        ++total_allocations;
        if (total_allocations % (1ULL << 20) == 0)
            std::cerr << "Total allocations: " << total_allocations << "\n";
    }
}
