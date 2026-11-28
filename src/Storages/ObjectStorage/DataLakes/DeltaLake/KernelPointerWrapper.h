#pragma once
#include <Core/Types.h>
#include <boost/noncopyable.hpp>

namespace DeltaLake
{

/**
 * DeltaKernel exposes many of its internal structures via raw pointers
 * and deletion functions for each of them.
 * This class represents a RAII wrapper to manage such objects.
 */
template <typename KernelType, void (*free_function)(KernelType *)>
struct KernelPointerWrapper : boost::noncopyable
{
    KernelPointerWrapper() : ptr(nullptr) {}

    explicit KernelPointerWrapper(KernelType * ptr_) : ptr(ptr_) {}

    KernelPointerWrapper(KernelPointerWrapper && other) noexcept
        : ptr(other.ptr)
    {
        other.ptr = nullptr;
    }

    ~KernelPointerWrapper() { free(); }

    KernelPointerWrapper & operator=(KernelPointerWrapper && other) noexcept
    {
        std::swap(ptr, other.ptr);
        return *this;
    }

    KernelPointerWrapper & operator=(KernelType * ptr_) noexcept
    {
        free();
        ptr = ptr_;
        return *this;
    }

    [[nodiscard]] KernelType * get() const { return ptr; }

    KernelType * release()
    {
        auto res = ptr;
        ptr = nullptr;
        return res;
    }

private:
    KernelType * ptr;

    void free()
    {
        if (ptr)
            free_function(ptr);
    }
};

}
