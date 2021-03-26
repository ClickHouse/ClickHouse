#pragma once

#include <cstddef>


namespace DB
{

/// MMaps a region in file descriptor (or a whole file) into memory. Unmaps in destructor.
/// Does not open or close file.
class MappedFile
{
public:
    MappedFile(int fd_, size_t offset_, size_t length_);
    MappedFile(int fd_, size_t offset_);

    /// Makes empty object that can be initialized with `set`.
    MappedFile() {}

    ~MappedFile();

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

private:
    MappedFile(const MappedFile &) = delete;
    MappedFile(MappedFile &&) = delete;

    void init();

    int fd = -1;
    size_t offset = 0;
    size_t length = 0;
    char * data = nullptr;
};


}
