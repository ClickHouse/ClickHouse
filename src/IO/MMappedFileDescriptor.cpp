#if defined(OS_WINDOWS)
#include <io.h>
#include <Poco/UnWindows.h>
#else
#include <sys/mman.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <Common/formatReadable.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <base/getPageSize.h>
#include <IO/MMappedFileDescriptor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MUNMAP;
    extern const int CANNOT_STAT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


static size_t getFileSize(int fd)
{
    struct stat stat_res {};
    if (0 != fstat(fd, &stat_res))
        throw ErrnoException(ErrorCodes::CANNOT_STAT, "MMappedFileDescriptor: Cannot fstat");

    off_t file_size = stat_res.st_size;

    if (file_size < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MMappedFileDescriptor: fstat returned negative file size");

    return file_size;
}


MMappedFileDescriptor::MMappedFileDescriptor(int fd_, size_t offset_, size_t length_)
{
    set(fd_, offset_, length_);
}

MMappedFileDescriptor::MMappedFileDescriptor(int fd_, size_t offset_)
    : fd(fd_), offset(offset_)
{
    set(fd_, offset_);
}

void MMappedFileDescriptor::set(int fd_, size_t offset_, size_t length_)
{
    finish();

    fd = fd_;
    offset = offset_;
    length = length_;

    if (!length)
        return;

#if defined(OS_WINDOWS)
    auto * file_handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (file_handle == INVALID_HANDLE_VALUE)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "MMappedFileDescriptor: not an open descriptor");

    /// A zero maximum size means "as large as the file", which is what `mmap` does implicitly.
    mapping_handle = CreateFileMappingW(file_handle, nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (!mapping_handle)
        throw Exception(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "MMappedFileDescriptor: Cannot CreateFileMapping {}, error code: {}",
            ReadableSize(length),
            GetLastError());

    /// Round the offset down to the allocation granularity and lengthen the view to match, then
    /// point `data` at the requested offset inside it.
    SYSTEM_INFO system_info{};
    GetSystemInfo(&system_info);
    const size_t granularity = system_info.dwAllocationGranularity;
    const size_t aligned_offset = offset / granularity * granularity;
    const size_t delta = offset - aligned_offset;

    void * buf = MapViewOfFile(
        mapping_handle,
        FILE_MAP_READ,
        static_cast<DWORD>(aligned_offset >> 32),
        static_cast<DWORD>(aligned_offset & 0xFFFFFFFFull),
        length + delta);
    if (!buf)
    {
        const auto error = GetLastError();
        CloseHandle(mapping_handle);
        mapping_handle = nullptr;
        throw Exception(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "MMappedFileDescriptor: Cannot MapViewOfFile {}, error code: {}",
            ReadableSize(length),
            error);
    }

    view_base = static_cast<char *>(buf);
    data = view_base + delta;
#else
    void * buf = mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, offset);
    if (MAP_FAILED == buf)
        throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "MMappedFileDescriptor: Cannot mmap {}", ReadableSize(length));

    data = static_cast<char *>(buf);
#endif

    files_metric_increment.changeTo(1);
    bytes_metric_increment.changeTo(length);
}

void MMappedFileDescriptor::set(int fd_, size_t offset_)
{
    size_t file_size = getFileSize(fd_);

    if (offset > file_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MMappedFileDescriptor: requested offset is greater than file size");

    set(fd_, offset_, file_size - offset);
}

void MMappedFileDescriptor::finish()
{
    if (!length)
        return;

#if defined(OS_WINDOWS)
    /// The view and the mapping object are released separately, and in this order.
    if (view_base && !UnmapViewOfFile(view_base))
        throw Exception(
            ErrorCodes::CANNOT_MUNMAP,
            "MMappedFileDescriptor: Cannot UnmapViewOfFile {}, error code: {}",
            ReadableSize(length),
            GetLastError());
    view_base = nullptr;
    if (mapping_handle)
        CloseHandle(mapping_handle);
    mapping_handle = nullptr;
#else
    if (0 != munmap(data, length))
        throw ErrnoException(ErrorCodes::CANNOT_MUNMAP, "MMappedFileDescriptor: Cannot munmap {}", ReadableSize(length));
#endif

    length = 0;

    files_metric_increment.changeTo(0);
    bytes_metric_increment.changeTo(0);
}

MMappedFileDescriptor::~MMappedFileDescriptor()
{
    finish(); /// Exceptions will lead to std::terminate and that's Ok.
}

}
