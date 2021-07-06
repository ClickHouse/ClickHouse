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
    MMappedFileDescriptor() {}

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

protected:
    MMappedFileDescriptor(const MMappedFileDescriptor &) = delete;
    MMappedFileDescriptor(MMappedFileDescriptor &&) = delete;

    void init();

    int fd = -1;
    size_t offset = 0;
    size_t length = 0;
    char * data = nullptr;

    CurrentMetrics::Increment files_metric_increment{CurrentMetrics::MMappedFiles, 0};
    CurrentMetrics::Increment bytes_metric_increment{CurrentMetrics::MMappedFileBytes, 0};
};

}

