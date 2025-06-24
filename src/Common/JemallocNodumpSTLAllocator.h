#pragma once

#ifdef USE_JEMALLOC

#include <limits>
#include <new>

#include <jemalloc/jemalloc.h>
#include <sys/mman.h>
#include <Poco/Format.h>

#include <Common/JemallocNodumpAllocatorImpl.h>


namespace DB
{

template<typename T>
class JemallocNodumpSTLAllocator
{
public:
    using value_type = T;

    JemallocNodumpSTLAllocator() noexcept = default;

    template<typename U>
    explicit JemallocNodumpSTLAllocator(const JemallocNodumpSTLAllocator<U> &) noexcept
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
};

template<typename T, typename U>
bool operator ==(const JemallocNodumpSTLAllocator<T> &, const JemallocNodumpSTLAllocator<U> &) noexcept
{
    return true;
}

template<typename T, typename U>
bool operator !=(const JemallocNodumpSTLAllocator<T> & a, const JemallocNodumpSTLAllocator<U> & b) noexcept
{
    return !(a == b);
}

}
#endif
