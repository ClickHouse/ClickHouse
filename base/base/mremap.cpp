#include <base/mremap.h>

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <cerrno>


void * mremap_fallback(
    void * old_address, size_t old_size, size_t new_size, int flags, int mmap_prot, int mmap_flags, int mmap_fd, off_t mmap_offset)
{
    /// No actual shrink
    if (new_size < old_size)
        return old_address;

    if (!(flags & MREMAP_MAYMOVE))
    {
        errno = ENOMEM;
        return MAP_FAILED;
    }

    void * new_address = mmap(nullptr, new_size, mmap_prot, mmap_flags, mmap_fd, mmap_offset);
    if (MAP_FAILED == new_address)
        return MAP_FAILED;

    memcpy(new_address, old_address, old_size);

    if (munmap(old_address, old_size))
        abort();

    return new_address;
}
