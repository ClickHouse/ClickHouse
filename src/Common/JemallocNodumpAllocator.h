#pragma once

#include <cstddef>
#include <cstdio>
#include <limits>
#include <new>

#include <jemalloc/jemalloc.h>
#include <sys/mman.h>
#include <Poco/Format.h>


namespace DB
{

class JemallocNodumpAllocatorImpl
{
public:
    static JemallocNodumpAllocatorImpl & instance();

    JemallocNodumpAllocatorImpl(const JemallocNodumpAllocatorImpl &) = delete;
    JemallocNodumpAllocatorImpl & operator =(const JemallocNodumpAllocatorImpl &) = delete;

    void * allocate(size_t size) const;

    void * reallocate(void * p, size_t size) const;

    void deallocate(void * p) const;

    static void deallocate(void * p, void * userData);

private:
    inline static extent_hooks_t extent_hooks_{};
    inline static extent_alloc_t * original_alloc_ = nullptr;

    /// Used to allocate extents by jemalloc.
    static void * alloc(
        extent_hooks_t * extent, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned arena_ind);

    explicit JemallocNodumpAllocatorImpl();

    void setupArena();

    unsigned arena_index{0};
    int flags{0};
};

template<typename T>
class JemallocNodumpAllocator
{
public:
    using value_type = T;

    JemallocNodumpAllocator() noexcept = default;

    template<typename U>
    explicit JemallocNodumpAllocator(const JemallocNodumpAllocator<U> &) noexcept
    {
    }

    T* allocate(std::size_t n)
    {
        static constexpr size_t element_size = sizeof(T);

        if (n > std::numeric_limits<std::size_t>::max() / element_size)
        {
            throw std::bad_alloc();
        }

        const std::size_t bytes = n * element_size;
        void * ptr = JemallocNodumpAllocatorImpl::instance().allocate(bytes);
        if (!ptr)
        {
            throw std::bad_alloc();
        }
        return static_cast<T*>(ptr);
    }

    void deallocate(T* p, std::size_t /*n*/) noexcept
    {
        JemallocNodumpAllocatorImpl::instance().deallocate(p);
    }

    template<typename U>
    struct rebind { // NOLINT(readability-identifier-naming)
        using other = JemallocNodumpAllocator<U>;
    };
};

template<typename T, typename U>
bool operator ==(const JemallocNodumpAllocator<T> &, const JemallocNodumpAllocator<U> &) noexcept
{
    return true;
}

template<typename T, typename U>
bool operator !=(const JemallocNodumpAllocator<T> & a, const JemallocNodumpAllocator<U> & b) noexcept
{
    return !(a == b);
}

}
