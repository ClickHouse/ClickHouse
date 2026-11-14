//
// NoDumpAllocator.h
//
// Library: Foundation
// Package: Core
// Module:  NoDumpAllocator
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NoDumpAllocator_INCLUDED
#define Foundation_NoDumpAllocator_INCLUDED


#include <cstddef>


namespace Poco
{


/// Storage for strings which may hold secrets, such as configuration values and URI credentials.
/// The defaults use malloc; the host application overrides them with memory excluded from core dumps.
void * allocateNoDump(std::size_t bytes);
void deallocateNoDump(void * ptr) noexcept;

template <class T>
struct NoDumpAllocator
{
    using value_type = T;
    NoDumpAllocator() = default;
    template <class U>
    NoDumpAllocator(const NoDumpAllocator<U> &)
    {
    }
    T * allocate(std::size_t n) { return static_cast<T *>(allocateNoDump(n * sizeof(T))); }
    void deallocate(T * ptr, std::size_t) noexcept { deallocateNoDump(ptr); }
};
template <class T, class U>
bool operator==(const NoDumpAllocator<T> &, const NoDumpAllocator<U> &)
{
    return true;
}


} // namespace Poco


#endif // Foundation_NoDumpAllocator_INCLUDED
