#pragma once

#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

/** MMap range in a file and represent it as a ReadBuffer.
  * Please note that mmap is not always the optimal way to read file.
  * Also you cannot control whether and how long actual IO take place,
  *  so this method is not manageable and not recommended for anything except benchmarks.
  */
class MMapReadBufferFromFileDescriptor : public ReadBufferFromFileBase
{
protected:
    MMapReadBufferFromFileDescriptor() {}

    void init(int fd_, size_t offset, size_t length_);
    void init(int fd_, size_t offset);

    off_t doSeek(off_t off, int whence) override;

public:
    MMapReadBufferFromFileDescriptor(int fd_, size_t offset_, size_t length_);

    /// Map till end of file.
    MMapReadBufferFromFileDescriptor(int fd_, size_t offset_);

    ~MMapReadBufferFromFileDescriptor() override;

    /// unmap memory before call to destructor
    void finish();

    off_t getPositionInFile() const override;
    std::string getFileName() const override;
    int getFD() const override;

private:
    size_t length = 0;
    int fd = -1;
};

}

