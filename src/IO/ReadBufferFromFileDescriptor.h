#pragma once

#include <IO/ReadBufferFromFileBase.h>

#include <unistd.h>


namespace DB
{

/** Use ready file descriptor. Does not open or close a file.
  */
class ReadBufferFromFileDescriptor : public ReadBufferFromFileBase
{
protected:
    int fd;
    off_t pos_in_file; /// What offset in file corresponds to working_buffer.end().

    bool nextImpl() override;

    /// Name or some description of file.
    std::string getFileName() const override;

public:
    ReadBufferFromFileDescriptor(int fd_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = nullptr, size_t alignment = 0)
        : ReadBufferFromFileBase(buf_size, existing_memory, alignment), fd(fd_), pos_in_file(0) {}

    ReadBufferFromFileDescriptor(ReadBufferFromFileDescriptor &&) = default;

    int getFD() const
    {
        return fd;
    }

    off_t getPosition() override
    {
        return pos_in_file - (working_buffer.end() - pos);
    }

    /// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
    off_t seek(off_t off, int whence) override;

private:
    /// Assuming file descriptor supports 'select', check that we have data to read or wait until timeout.
    bool poll(size_t timeout_microseconds);
};

}
