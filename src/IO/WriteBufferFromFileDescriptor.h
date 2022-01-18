#pragma once

#include <IO/WriteBufferFromFileBase.h>


namespace DB
{

/** Use ready file descriptor. Does not open or close a file.
  */
class WriteBufferFromFileDescriptor : public WriteBufferFromFileBase
{
protected:
    int fd;

    void nextImpl() override;

    /// Name or some description of file.
    std::string getFileName() const override;

public:
    WriteBufferFromFileDescriptor(
        int fd_ = -1,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    /** Could be used before initialization if needed 'fd' was not passed to constructor.
      * It's not possible to change 'fd' during work.
      */
    void setFD(int fd_)
    {
        fd = fd_;
    }

    ~WriteBufferFromFileDescriptor() override;

    int getFD() const
    {
        return fd;
    }

    void sync() override;

    off_t seek(off_t offset, int whence);
    void truncate(off_t length);

    off_t size();
};

}
