#pragma once
#include <Core/Types.h>

namespace DeltaLake
{

template <typename KernelType>
struct KernelPointerWrapper
{
    KernelPointerWrapper() : ptr(nullptr), free_func(nullptr)
    {
    }

    KernelPointerWrapper(KernelType * ptr_, void (*free_func_)(KernelType *))
        : ptr(ptr_), free_func(free_func_)
    {
    }

    KernelPointerWrapper(KernelPointerWrapper && other) noexcept
        : ptr(other.ptr)
    {
        other.ptr = nullptr;
    }

    KernelPointerWrapper & operator=(KernelPointerWrapper && other) noexcept
    {
        std::swap(ptr, other.ptr);
        std::swap(free_func, other.free_func);
        return *this;
    }

    ~KernelPointerWrapper()
    {
        if (ptr && free_func)
            free_func(ptr);
    }

    KernelType * get() const { return ptr; }

private:
    KernelType * ptr;
    void (*free_func)(KernelType *) = nullptr;
};

template <typename KernelType, void (*delete_function)(KernelType *)>
struct TemplatedKernelPointerWrapper : public KernelPointerWrapper<KernelType>
{
    TemplatedKernelPointerWrapper() : KernelPointerWrapper<KernelType>() {}
    TemplatedKernelPointerWrapper(KernelType * ptr) : KernelPointerWrapper<KernelType>(ptr, delete_function) {}
};

}
