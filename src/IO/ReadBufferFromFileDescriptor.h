#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Context_fwd.h>
#include <Common/IThrottler.h>

#include <unistd.h>


namespace DB
{

/** Use ready file descriptor. Does not open or close a file.
  */
class ReadBufferFromFileDescriptor : public ReadBufferFromFileBase
{
protected:
    const size_t required_alignment = 0;  /// For O_DIRECT both file offsets and memory addresses have to be aligned.
    bool use_pread = false;               /// To access one fd from multiple threads, use 'pread' syscall instead of 'read'.

    size_t file_offset_of_buffer_end = 0; /// What offset in file corresponds to working_buffer.end().

    int fd;

    ThrottlerPtr throttler;

    bool nextImpl() override;
    void prefetch(Priority priority) override;

    /// Name or some description of file.
    std::string getFileName() const override;

    /// Does the read()/pread(), with all the metric increments, error handling, throttling, etc.
    /// Doesn't seek (`offset` must match fd's position if !use_pread).
    /// Stops after min_bytes or eof. Returns 0 if eof.
    /// Thread safe.
    size_t readImpl(char * to, size_t min_bytes, size_t max_bytes, size_t offset) const;

public:
    explicit ReadBufferFromFileDescriptor(
        int fd_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt,
        ThrottlerPtr throttler_ = {})
        : ReadBufferFromFileBase(buf_size, existing_memory, alignment, file_size_)
        , required_alignment(alignment)
        , fd(fd_)
        , throttler(throttler_)
    {
    }

    int getFD() const
    {
        return fd;
    }

    off_t getPosition() override
    {
        return file_offset_of_buffer_end - (working_buffer.end() - pos);
    }

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }

    /// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
    off_t seek(off_t off, int whence) override;

    /// Seek to the beginning, discarding already read data if any. Useful to reread file that changes on every read.
    void rewind();

    std::optional<size_t> tryGetFileSize() override;

    bool checkIfActuallySeekable() override;

    size_t readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> &) const override;
    bool supportsReadAt() override { return use_pread; }
};


/** Similar to ReadBufferFromFileDescriptor but it is using 'pread' allowing multiple concurrent reads from the same fd.
  */
class ReadBufferFromFileDescriptorPRead : public ReadBufferFromFileDescriptor
{
public:
    explicit ReadBufferFromFileDescriptorPRead(
        int fd_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt,
        ThrottlerPtr throttler_ = {})
        : ReadBufferFromFileDescriptor(fd_, buf_size, existing_memory, alignment, file_size_, throttler_)
    {
        use_pread = true;
    }
};

}
