#pragma once

#include <string>
#include <ctime>
#include <functional>
#include <fcntl.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

#ifdef __APPLE__
#include <common/apple_rt.h>
#endif

namespace DB
{

class ReadBufferFromFileBase : public BufferWithOwnMemory<ReadBuffer>
{
public:
    ReadBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment);
    ReadBufferFromFileBase(ReadBufferFromFileBase &&) = default;
    virtual ~ReadBufferFromFileBase();
    off_t seek(off_t off, int whence = SEEK_SET);
    virtual off_t getPositionInFile() = 0;
    virtual std::string getFileName() const = 0;
    virtual int getFD() const = 0;

    /// It is possible to get information about the time of each reading.
    struct ProfileInfo
    {
        size_t bytes_requested;
        size_t bytes_read;
        size_t nanoseconds;
    };

    using ProfileCallback = std::function<void(ProfileInfo)>;

    /// CLOCK_MONOTONIC_COARSE is more than enough to track long reads - for example, hanging for a second.
    void setProfileCallback(const ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        profile_callback = profile_callback_;
        clock_type = clock_type_;
    }

protected:
    ProfileCallback profile_callback;
    clockid_t clock_type;

    virtual off_t doSeek(off_t off, int whence) = 0;
};

}
