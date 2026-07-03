#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileName.h>
#include <Interpreters/Context_fwd.h>
#include <base/time.h>

#include <functional>
#include <utility>
#include <string>

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif


namespace DB
{

/// A consistent snapshot of a remote object's metadata, fetched in a single request.
struct RemoteFileMetadata
{
    size_t size = 0;
    time_t last_modification_time = 0;
};

class ReadBufferFromFileBase : public BufferWithOwnMemory<SeekableReadBuffer>, public WithFileName, public WithFileSize
{
public:
    ReadBufferFromFileBase();
    ReadBufferFromFileBase(
        size_t buf_size,
        char * existing_memory,
        size_t alignment,
        std::optional<size_t> file_size_ = std::nullopt);
    ~ReadBufferFromFileBase() override;

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

    std::optional<size_t> tryGetFileSize() override;

    void setProgressCallback(ContextPtr context);

    /// Returns true if this file is on local filesystem, and getFileName() is its path.
    /// I.e. it can be read using open() or mmap(). If this buffer is a "view" into a subrange of the
    /// file, *out_view_offset is set to the start of that subrange, i.e. the difference between actual
    /// file offset and what getPosition() returns.
    virtual bool isRegularLocalFile(size_t * /*out_view_offsee*/) { return false; }

    virtual bool isCached() const { return false; }

    /// Query the actual object's size and last modification time directly from remote storage.
    /// Unlike tryGetFileSize(), which typically returns a pre-known cached value passed at construction time,
    /// this method issues a real metadata request (e.g. S3 HeadObject, Azure GetProperties) and reflects
    /// the current state of the object. Both fields come from a single request, so they form a consistent
    /// snapshot -- useful for detecting (and confirming, via the timestamp) that an object was overwritten
    /// between listing and reading.
    /// Returns std::nullopt for non-remote or local file buffers.
    virtual std::optional<RemoteFileMetadata> getRemoteFileMetadata() const { return std::nullopt; }

protected:
    std::optional<size_t> file_size;
    ProfileCallback profile_callback;
    clockid_t clock_type{};
};

}
