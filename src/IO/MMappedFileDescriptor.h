#pragma once

#include <cstddef>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric MMappedFiles;
    extern const Metric MMappedFileBytes;
}


namespace DB
{

/// MMaps a region in file (or a whole file) into memory. Unmaps in destructor.
/// Does not open or close file.
class MMappedFileDescriptor
{
public:
    MMappedFileDescriptor(int fd_, size_t offset_, size_t length_);
    MMappedFileDescriptor(int fd_, size_t offset_);

    /// Makes empty object that can be initialized with `set`.
    MMappedFileDescriptor() = default;

    virtual ~MMappedFileDescriptor();

    char * getData() { return data; }
    const char * getData() const { return data; }

    int getFD() const { return fd; }
    size_t getOffset() const { return offset; }
    size_t getLength() const { return length; }

    /// Unmap memory before call to destructor
    void finish();

    /// Initialize or reset to another fd.
    void set(int fd_, size_t offset_, size_t length_);
    void set(int fd_, size_t offset_);

    MMappedFileDescriptor(const MMappedFileDescriptor &) = delete;
    MMappedFileDescriptor(MMappedFileDescriptor &&) = delete;

protected:

    void init();

    int fd = -1;
    size_t offset = 0;
    size_t length = 0;
    char * data = nullptr;

#if defined(OS_WINDOWS)
    /// Windows needs two things POSIX `mmap` does not. A file mapping is a kernel object
    /// distinct from the view of it, so its handle has to outlive the view and be closed
    /// separately. And `MapViewOfFile` only accepts an offset that is a multiple of the
    /// allocation granularity (64 KiB), not merely of the page size, so a view may have to start
    /// before the requested offset - `view_base` is where it actually starts, and `data` points
    /// into it.
    void * mapping_handle = nullptr;
    char * view_base = nullptr;
#endif

    CurrentMetrics::Increment files_metric_increment{CurrentMetrics::MMappedFiles, 0};
    CurrentMetrics::Increment bytes_metric_increment{CurrentMetrics::MMappedFileBytes, 0};
};

}

