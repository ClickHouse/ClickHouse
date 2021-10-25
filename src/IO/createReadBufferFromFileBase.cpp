#include <IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/MMapReadBufferFromFileWithCache.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event CreatedReadBufferOrdinary;
    extern const Event CreatedReadBufferDirectIO;
    extern const Event CreatedReadBufferDirectIOFailed;
    extern const Event CreatedReadBufferMMap;
    extern const Event CreatedReadBufferMMapFailed;
}

namespace DB
{

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename,
    size_t estimated_size, size_t direct_io_threshold, size_t mmap_threshold, MMappedFileCache * mmap_cache,
    size_t buffer_size, int flags, char * existing_memory, size_t alignment)
{
    if (!existing_memory && mmap_threshold && mmap_cache && estimated_size >= mmap_threshold)
    {
        try
        {
            auto res = std::make_unique<MMapReadBufferFromFileWithCache>(*mmap_cache, filename, 0);
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferMMap);
            return res;
        }
        catch (const ErrnoException &)
        {
            /// Fallback if mmap is not supported (example: pipe).
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferMMapFailed);
        }
    }

#if defined(OS_LINUX) || defined(__FreeBSD__)
    if (direct_io_threshold && estimated_size >= direct_io_threshold)
    {
        /** O_DIRECT
          * The O_DIRECT flag may impose alignment restrictions on the length and address of user-space buffers and the file offset of I/Os.
          * In Linux alignment restrictions vary by filesystem and kernel version and might be absent entirely.
          * However there is currently no filesystem-independent interface for an application to discover these restrictions
          * for a given file or filesystem. Some filesystems provide their own interfaces for doing so, for example the
          * XFS_IOC_DIOINFO operation in xfsctl(3).
          *
          * Under Linux 2.4, transfer sizes, and the alignment of the user buffer and the file offset must all be
          * multiples of the logical block size of the filesystem. Since Linux 2.6.0, alignment to the logical block size
          * of the underlying storage (typically 512 bytes) suffices.
          *
          * - man 2 open
          */
        constexpr size_t min_alignment = DEFAULT_AIO_FILE_BLOCK_SIZE;

        auto align_up = [=](size_t value) { return (value + min_alignment - 1) / min_alignment * min_alignment; };

        if (alignment == 0)
            alignment = min_alignment;
        else if (alignment % min_alignment)
            alignment = align_up(alignment);

        if (buffer_size % min_alignment)
        {
            existing_memory = nullptr;  /// Cannot reuse existing memory is it has unaligned size.
            buffer_size = align_up(buffer_size);
        }

        if (reinterpret_cast<uintptr_t>(existing_memory) % min_alignment)
        {
            existing_memory = nullptr;  /// Cannot reuse existing memory is it has unaligned offset.
        }

        /// Attempt to open a file with O_DIRECT
        try
        {
            auto res = std::make_unique<ReadBufferFromFilePReadWithCache>(
                filename, buffer_size, (flags == -1 ? O_RDONLY | O_CLOEXEC : flags) | O_DIRECT, existing_memory, alignment);
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferDirectIO);
            return res;
        }
        catch (const ErrnoException &)
        {
            /// Fallback to cached IO if O_DIRECT is not supported.
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferDirectIOFailed);
        }
    }
#else
    (void)direct_io_threshold;
    (void)estimated_size;
#endif

    ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
    return std::make_unique<ReadBufferFromFilePReadWithCache>(filename, buffer_size, flags, existing_memory, alignment);
}

}
