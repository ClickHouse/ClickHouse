#pragma once

#include <Disks/IDiskRemote.h>
#include <Core/UUID.h>


namespace DB
{

struct DiskWebServerSettings
{
    /// Number of read attempts before throw that network is unreachable.
    size_t max_read_tries;
    /// Passed to SeekAvoidingReadBuffer.
    size_t min_bytes_for_seek;
    /// Used by IDiskRemote.
    size_t thread_pool_size;

    DiskWebServerSettings(size_t max_read_tries_, size_t min_bytes_for_seek_, size_t thread_pool_size_)
        : max_read_tries(max_read_tries_) , min_bytes_for_seek(min_bytes_for_seek_) , thread_pool_size(thread_pool_size_) {}
};


/// Storage to store data on a web server and metadata on the local disk.

class DiskWebServer : public IDiskRemote, WithContext
{
using SettingsPtr = std::unique_ptr<DiskWebServerSettings>;

public:
    DiskWebServer(const String & disk_name_,
               const String & files_root_path_url_,
               const String & metadata_path_,
               ContextPtr context,
               SettingsPtr settings_);

    DiskType::Type getType() const override { return DiskType::Type::WebServer; }

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        size_t buf_size,
        size_t estimated_size,
        size_t aio_threshold,
        size_t mmap_threshold,
        MMappedFileCache * mmap_cache) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode) override;

private:
    String generateName() { return toString(UUIDHelpers::generateV4()); }

    SettingsPtr settings;
};

}
