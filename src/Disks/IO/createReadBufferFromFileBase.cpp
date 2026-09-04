#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromEmptyFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/MMapReadBufferFromFileWithCache.h>
#include <IO/AsynchronousReadBufferFromFile.h>
#include <IO/CachedInMemoryReadBufferFromFile.h>
#include <Disks/IO/IOUringReader.h>
#include <Disks/IO/getIOUringReader.h>
#include <Disks/IO/ThreadPoolReader.h>
#include <Disks/IO/getThreadPoolReader.h>
#include <IO/AsynchronousReader.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/ErrnoException.h>
#include <Interpreters/Context.h>
#include "config.h"


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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size,
    int flags,
    char * existing_memory,
    bool allow_userspace_page_cache)
{
    if (file_size.has_value() && !*file_size)
        return std::make_unique<ReadBufferFromEmptyFile>();

    size_t estimated_size = 0;
    if (read_hint.has_value())
        estimated_size = *read_hint;
    else if (file_size.has_value())
        estimated_size = *file_size;

    if (!existing_memory
        && settings.local_fs_method == LocalFSReadMethod::mmap
        && settings.mmap_threshold
        && settings.mmap_cache
        && estimated_size >= settings.mmap_threshold)
    {
        try
        {
            std::unique_ptr<MMapReadBufferFromFileWithCache> res;
            if (file_size)
                res = std::make_unique<MMapReadBufferFromFileWithCache>(*settings.mmap_cache, filename, 0, *file_size);
            else
                res = std::make_unique<MMapReadBufferFromFileWithCache>(*settings.mmap_cache, filename, 0);

            ProfileEvents::increment(ProfileEvents::CreatedReadBufferMMap);
            return res;
        }
        catch (const ErrnoException &)
        {
            /// Fallback if mmap is not supported (example: pipe).
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferMMapFailed);
        }
    }

    bool use_page_cache = allow_userspace_page_cache && !existing_memory && settings.page_cache != nullptr;

    auto get_prefetches_log = [&]()
    {
        return settings.enable_filesystem_read_prefetches_log ? Context::getGlobalContextInstance()->getFilesystemReadPrefetchesLog() : nullptr;
    };

    auto create = [&](size_t buffer_size, size_t buffer_alignment, int actual_flags)
    {
        std::unique_ptr<ReadBufferFromFileBase> res;

        if (settings.local_fs_method == LocalFSReadMethod::read)
        {
            res = std::make_unique<ReadBufferFromFile>(
                filename,
                use_page_cache ? 0 : buffer_size,
                actual_flags,
                existing_memory,
                buffer_alignment,
                file_size,
                settings.local_throttler);
        }
        else if (settings.local_fs_method == LocalFSReadMethod::pread || settings.local_fs_method == LocalFSReadMethod::mmap)
        {
            res = std::make_unique<ReadBufferFromFilePReadWithDescriptorsCache>(
                filename,
                use_page_cache ? 0 : buffer_size,
                actual_flags,
                existing_memory,
                buffer_alignment,
                file_size,
                settings.local_throttler);
        }
        else if (settings.local_fs_method == LocalFSReadMethod::io_uring)
        {
#if USE_LIBURING
            use_page_cache = false;
            auto & reader = getIOUringReaderOrThrow();
            res = std::make_unique<AsynchronousReadBufferFromFileWithDescriptorsCache>(
                reader,
                settings.priority,
                filename,
                buffer_size,
                actual_flags,
                existing_memory,
                buffer_alignment,
                file_size,
                settings.local_throttler,
                get_prefetches_log());
#else
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Read method io_uring is only supported in Linux");
#endif
        }
        else if (settings.local_fs_method == LocalFSReadMethod::pread_fake_async)
        {
            use_page_cache = false;
            auto & reader = getThreadPoolReader(FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER);
            res = std::make_unique<AsynchronousReadBufferFromFileWithDescriptorsCache>(
                reader,
                settings.priority,
                filename,
                buffer_size,
                actual_flags,
                existing_memory,
                buffer_alignment,
                file_size,
                settings.local_throttler,
                get_prefetches_log());
        }
        else if (settings.local_fs_method == LocalFSReadMethod::pread_threadpool)
        {
            use_page_cache = false;
            auto & reader = getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER);
            res = std::make_unique<AsynchronousReadBufferFromFileWithDescriptorsCache>(
                reader,
                settings.priority,
                filename,
                buffer_size,
                actual_flags,
                existing_memory,
                buffer_alignment,
                file_size,
                settings.local_throttler,
                get_prefetches_log());
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown read method");

        return res;
    };

    if (flags == -1)
        flags = O_RDONLY | O_CLOEXEC;

    std::unique_ptr<ReadBufferFromFileBase> res;

#if defined(OS_LINUX) || defined(OS_FREEBSD)
    if (settings.direct_io_threshold && estimated_size >= settings.direct_io_threshold)
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

        size_t buffer_size = settings.local_fs_buffer_size;

        if (buffer_size % min_alignment)
        {
            existing_memory = nullptr;  /// Cannot reuse existing memory as it has unaligned size.
            buffer_size = align_up(buffer_size);
        }

        if (reinterpret_cast<uintptr_t>(existing_memory) % min_alignment)
        {
            existing_memory = nullptr;  /// Cannot reuse existing memory as it has unaligned offset.
        }

        if (use_page_cache && settings.page_cache_block_size % min_alignment)
        {
            LOG_TEST(getLogger("createReadBufferFromFileBase"), "Not using userspace page cache because page cache block size is not divisible by direct IO alignment");
            use_page_cache = false;
        }

        /// Attempt to open a file with O_DIRECT
        try
        {
            res = create(buffer_size, min_alignment, flags | O_DIRECT);
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferDirectIO);
        }
        catch (const ErrnoException &)
        {
            /// Fallback to cached IO if O_DIRECT is not supported.
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferDirectIOFailed);
        }
    }
#endif

    if (!res)
    {
        ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);

        size_t buffer_size = settings.local_fs_buffer_size;
        /// Check if the buffer can be smaller than default
        if (read_hint.has_value() && *read_hint > 0 && *read_hint < buffer_size)
            buffer_size = *read_hint;
        if (file_size.has_value() && *file_size < buffer_size)
            buffer_size = *file_size;

        res = create(buffer_size, /*buffer_alignment*/ 0, flags);
    }

    if (use_page_cache)
    {
        PageCacheKey key;
        key.path = "local:" + filename;
        res = std::make_unique<CachedInMemoryReadBufferFromFile>(key, settings.page_cache, std::move(res), settings);
    }

    return res;
}

}
