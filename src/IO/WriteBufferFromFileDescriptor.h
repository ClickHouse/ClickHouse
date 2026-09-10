#pragma once

#include <IO/WriteBufferFromFileBase.h>
#include <Common/IThrottler.h>


namespace DB
{

/** Use ready file descriptor. Does not open or close a file.
  */
class WriteBufferFromFileDescriptor : public WriteBufferFromFileBase
{
public:
    explicit WriteBufferFromFileDescriptor(
        int fd_ = -1,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        ThrottlerPtr throttler_ = {},
        size_t alignment = 0,
        std::string file_name_ = "",
        bool use_adaptive_buffer_size_ = false,
        size_t adaptive_buffer_initial_size = DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE);

    /** Could be used before initialization if needed 'fd' was not passed to constructor.
      * It's not possible to change 'fd' during work.
      */
    void setFD(int fd_)
    {
        fd = fd_;
    }

    int getFD() const
    {
        return fd;
    }

    void sync() override;

    /// clang-tidy wants these methods to be const, but
    /// they are not const semantically
    off_t seek(off_t offset, int whence); // NOLINT
    void truncate(off_t length); // NOLINT

    /// Name or some description of file.
    std::string getFileName() const override;

    off_t size() const;

protected:
    void nextImpl() override;

    int fd;
    ThrottlerPtr throttler;

    /// If file has name contains filename, otherwise contains string "(fd=...)"
    std::string file_name;

    /// If true, the size of internal buffer will be exponentially increased up to
    /// adaptive_buffer_max_size after each nextImpl call. It can be used to avoid
    /// large buffer allocation when actual size of written data is small.
    bool use_adaptive_buffer_size;
    size_t adaptive_max_buffer_size;

    void finalizeImpl() override;
};

}
