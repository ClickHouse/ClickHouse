#pragma once

#include <Common/IThrottler.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include <IO/AsynchronousReadBufferFromFileDescriptor.h>
#include <IO/OpenedFileCache.h>


namespace DB
{

/// Transparently shares open file descriptors.
class AsynchronousReadBufferFromFileWithDescriptorsCache : public AsynchronousReadBufferFromFileDescriptor
{
private:
    std::string file_name;
    OpenedFileCache::OpenedFilePtr file;

public:
    AsynchronousReadBufferFromFileWithDescriptorsCache(
        IAsynchronousReader & reader_,
        Priority priority_,
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt,
        ThrottlerPtr throttler_ = {},
        FilesystemReadPrefetchesLogPtr prefetches_log_ = nullptr)
        : AsynchronousReadBufferFromFileDescriptor(
            reader_, priority_, -1, buf_size, flags, existing_memory, alignment, file_size_, throttler_, prefetches_log_)
        , file_name(file_name_)
    {
        file = OpenedFileCache::instance().get(file_name, flags);
        fd = file->getFD();
    }

    ~AsynchronousReadBufferFromFileWithDescriptorsCache() override;

    std::string getFileName() const override
    {
        return file_name;
    }

    bool isRegularLocalFile(size_t * /* out_view_offset */) override { return true; }
};

}
