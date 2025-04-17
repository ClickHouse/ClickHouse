#include "IBuffer.h"

namespace DB
{
    namespace ErrorCodes
{
    extern const int CANNOT_MPROTECT;
    extern const int CANNOT_ALLOCATE_MEMORY;
}

namespace BufferDetails
{

#ifndef NDEBUG
void protectMemoryRegion(void * addr, size_t len, int prot)
{
    if (0 != mprotect(addr, len, prot))
        throw ErrnoException(ErrorCodes::CANNOT_MPROTECT, "Cannot mprotect memory region");
}
#endif

ALWAYS_INLINE size_t byte_size(size_t num_elements, size_t element_size)
{
    size_t amount;
    if (__builtin_mul_overflow(num_elements, element_size, &amount))
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Amount of memory requested to allocate is more than allowed");
    return amount;
}

ALWAYS_INLINE size_t minimum_memory_for_elements(size_t num_elements, size_t element_size, size_t pad_left, size_t pad_right)
{
    size_t amount;
    if (__builtin_add_overflow(byte_size(num_elements, element_size), pad_left + pad_right, &amount))
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Amount of memory requested to allocate is more than allowed");
    return amount;
}

}

}
