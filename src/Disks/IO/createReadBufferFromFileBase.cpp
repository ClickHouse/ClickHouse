#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromEmptyFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/MMapReadBufferFromFileWithCache.h>
#include <IO/MMapReadBufferFromFile.h>
#include <IO/AsynchronousReadBufferFromFile.h>
#include <Disks/IO/ThreadPoolReader.h>
#include <IO/SynchronousReader.h>
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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_STAT;
}


std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileOrFileDescriptorBase(
    const std::string & filename,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size,
    int flags,
    char * existing_memory,
    size_t alignment,
    bool read_from_fd,
    int fd)
{
    if (file_size.has_value() && !*file_size)
        return std::make_unique<ReadBufferFromEmptyFile>();

    struct stat file_stat{};
    if (read_from_fd)
    {
        if (0 != fstat(fd, &file_stat))
            throwFromErrno("Cannot stat file descriptor", ErrorCodes::CANNOT_STAT);
    }
    else
    {
        if (0 != stat(filename.c_str(), &file_stat))
            throwFromErrno("Cannot stat file " + filename, ErrorCodes::CANNOT_STAT);
    }

    size_t estimated_size = file_stat.st_size;
    if (read_hint.has_value())
        estimated_size = *read_hint;
    else if (file_size.has_value())
        estimated_size = *file_size;

    if (!existing_memory
        && settings.local_fs_method == LocalFSReadMethod::mmap
        && settings.mmap_threshold
        && estimated_size >= settings.mmap_threshold
        && S_ISREG(file_stat.st_mode))
    {
        try
        {
            std::unique_ptr<ReadBufferFromFileBase> res;

            if (settings.mmap_cache)
                res = std::make_unique<MMapReadBufferFromFileWithCache>(*settings.mmap_cache, filename, 0, estimated_size);
            else
                res = std::make_unique<MMapReadBufferFromFile>(filename, 0, estimated_size);

            ProfileEvents::increment(ProfileEvents::CreatedReadBufferMMap);
            return res;
        }
        catch (const ErrnoException &)
        {
            /// Fallback if mmap is not supported.
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferMMapFailed);
        }
    }

    auto create = [&](size_t buffer_size, int actual_flags)
    {
        std::unique_ptr<ReadBufferFromFileBase> res;

        /// Pread works only with regular files, so we explicitly fallback to read in other cases.
        if (settings.local_fs_method == LocalFSReadMethod::read || !S_ISREG(file_stat.st_mode))
        {
            if (read_from_fd)
                res = std::make_unique<ReadBufferFromFileDescriptor>(fd, buffer_size, existing_memory, alignment, file_size);
            else
                res = std::make_unique<ReadBufferFromFile>(filename, buffer_size, actual_flags, existing_memory, alignment, file_size);
        }
        else if (settings.local_fs_method == LocalFSReadMethod::pread || settings.local_fs_method == LocalFSReadMethod::mmap)
        {
            if (read_from_fd)
                res = std::make_unique<ReadBufferFromFileDescriptorPRead>(fd, buffer_size, existing_memory, alignment, file_size);
            else
                res = std::make_unique<ReadBufferFromFilePReadWithDescriptorsCache>(
                    filename, buffer_size, actual_flags, existing_memory, alignment, file_size);
        }
        else if (settings.local_fs_method == LocalFSReadMethod::pread_fake_async)
        {
            auto context = Context::getGlobalContextInstance();
            if (!context)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");

            auto & reader = context->getThreadPoolReader(Context::FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER);

            if (read_from_fd)
                res = std::make_unique<AsynchronousReadBufferFromFileDescriptor>(
                    reader, settings.priority, fd, buffer_size, existing_memory, alignment, file_size);
            else
                res = std::make_unique<AsynchronousReadBufferFromFileWithDescriptorsCache>(
                    reader, settings.priority, filename, buffer_size, actual_flags, existing_memory, alignment, file_size);
        }
        else if (settings.local_fs_method == LocalFSReadMethod::pread_threadpool)
        {
            auto context = Context::getGlobalContextInstance();
            if (!context)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");

            auto & reader = context->getThreadPoolReader(Context::FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER);

            if (read_from_fd)
                res = std::make_unique<AsynchronousReadBufferFromFileDescriptor>(
                    reader, settings.priority, fd, buffer_size, existing_memory, alignment, file_size);
            else
                res = std::make_unique<AsynchronousReadBufferFromFileWithDescriptorsCache>(
                    reader, settings.priority, filename, buffer_size, actual_flags, existing_memory, alignment, file_size);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown read method");

        return res;
    };

    if (flags == -1)
        flags = O_RDONLY | O_CLOEXEC;

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

        if (alignment == 0)
            alignment = min_alignment;
        else if (alignment % min_alignment)
            alignment = align_up(alignment);

        size_t buffer_size = settings.local_fs_buffer_size;

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
            std::unique_ptr<ReadBufferFromFileBase> res = create(buffer_size, flags | O_DIRECT);
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferDirectIO);
            return res;
        }
        catch (const ErrnoException &)
        {
            /// Fallback to cached IO if O_DIRECT is not supported.
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferDirectIOFailed);
        }
    }
#endif

    ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);

    size_t buffer_size = settings.local_fs_buffer_size;
    /// Check if the buffer can be smaller than default
    if (read_hint.has_value() && *read_hint > 0 && *read_hint < buffer_size)
        buffer_size = *read_hint;
    if (file_size.has_value() && *file_size < buffer_size)
        buffer_size = *file_size;

    return create(buffer_size, flags);
}

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size,
    int flags_,
    char * existing_memory,
    size_t alignment)
{
    return createReadBufferFromFileOrFileDescriptorBase(filename, settings, read_hint, file_size, flags_, existing_memory, alignment);
}

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileDescriptorBase(
    int fd,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size,
    char * existing_memory ,
    size_t alignment)
{
    return createReadBufferFromFileOrFileDescriptorBase({}, settings, read_hint, file_size, -1, existing_memory, alignment, true, fd);
}
}
