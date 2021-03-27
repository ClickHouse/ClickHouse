#pragma once

#include <cstddef>


namespace DB
{

/// MMaps a region in file (or a whole file) into memory. Unmaps in destructor.
/// Does not open or close file.
class MappedFileDescriptor
{
public:
    MappedFileDescriptor(int fd_, size_t offset_, size_t length_);
    MappedFileDescriptor(int fd_, size_t offset_);

    /// Makes empty object that can be initialized with `set`.
    MappedFileDescriptor() {}

    virtual ~MappedFileDescriptor();

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
    MappedFileDescriptor(const MappedFileDescriptor &) = delete;
    MappedFileDescriptor(MappedFileDescriptor &&) = delete;

    void init();

    int fd = -1;
    size_t offset = 0;
    size_t length = 0;
    char * data = nullptr;
};

}

