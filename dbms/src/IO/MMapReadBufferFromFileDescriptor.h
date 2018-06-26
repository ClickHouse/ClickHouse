#pragma once

#include <IO/ReadBuffer.h>


namespace DB
{

/** MMap range in a file and represent it as a ReadBuffer.
  * Please note that mmap is not always the optimal way to read file.
  * Also you cannot control whether and how long actual IO take place,
  *  so this method is not manageable and not recommended for anything except benchmarks.
  */
class MMapReadBufferFromFileDescriptor : public ReadBuffer
{
protected:
    MMapReadBufferFromFileDescriptor() : ReadBuffer(nullptr, 0) {};

    void init(int fd_, size_t offset, size_t length_);
    void init(int fd_, size_t offset);

public:
    MMapReadBufferFromFileDescriptor(int fd, size_t offset, size_t length);

    /// Map till end of file.
    MMapReadBufferFromFileDescriptor(int fd, size_t offset);

    ~MMapReadBufferFromFileDescriptor() override;

    /// unmap memory before call to destructor
    void finish();

private:
    size_t length = 0;
    int fd = -1;
};

}

