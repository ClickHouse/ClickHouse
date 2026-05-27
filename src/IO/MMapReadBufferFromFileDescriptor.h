#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/MMappedFileDescriptor.h>


namespace DB
{

/** MMap range in a file and represent it as a ReadBuffer.
  * Please note that mmap is not always the optimal way to read file.
  * Also you cannot control whether and how long actual IO take place,
  *  so this method is not manageable and not recommended for anything except benchmarks.
  */
class MMapReadBufferFromFileDescriptor : public ReadBufferFromFileBase
{
public:
    off_t seek(off_t off, int whence) override;

protected:
    MMapReadBufferFromFileDescriptor() = default;
    void init();

    MMappedFileDescriptor mapped;

public:
    MMapReadBufferFromFileDescriptor(int fd_, size_t offset_, size_t length_);

    /// Map till end of file.
    MMapReadBufferFromFileDescriptor(int fd_, size_t offset_);

    /// unmap memory before call to destructor
    void finish();

    off_t getPosition() override;

    std::string getFileName() const override;

    int getFD() const;

    std::optional<size_t> tryGetFileSize() override;

    size_t readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> &) const override;
    bool supportsReadAt() override { return true; }

    /// mmap has no producer behind `nextImpl` — `working_buffer` already points
    /// at the entire mapped region. After `set(dest, size)` the buffer cannot
    /// refill into `dest`, so callers must use `read(dest, n)` instead, which
    /// memcpys from the mapped region into the caller's buffer.
    bool supportsExternalBufferMode() const override { return false; }
};

}
