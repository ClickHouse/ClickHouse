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
    KernelPointerWrapper() : ptr(nullptr), free_func(nullptr) {}

    explicit KernelPointerWrapper(KernelType * ptr_)
        : ptr(ptr_)
        , free_func(free_function)
    {
    }

    KernelPointerWrapper(KernelPointerWrapper && other) noexcept
        : ptr(other.ptr)
        , free_func(other.free_func)
    {
        other.ptr = nullptr;
    }

    ~KernelPointerWrapper() { free(); }

    KernelPointerWrapper & operator=(KernelPointerWrapper && other) noexcept
    {
        std::swap(ptr, other.ptr);
        std::swap(free_func, other.free_func);
        return *this;
    }

    KernelPointerWrapper & operator=(KernelType * ptr_) noexcept
    {
        free();
        ptr = ptr_;
        free_func = free_function;
        return *this;
    }

    KernelType * get() const { return ptr; }

private:
    KernelType * ptr;
    void (*free_func)(KernelType *) = nullptr;

    void free()
    {
        if (ptr && free_func)
            free_func(ptr);
        else if (ptr)
            chassert(free_func);
    }
};

}
