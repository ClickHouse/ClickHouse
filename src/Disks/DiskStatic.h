#pragma once

#include <Disks/IDiskRemote.h>
#include <Core/UUID.h>


namespace DB
{

struct DiskStaticSettings
{
    size_t min_bytes_for_seek;
    int thread_pool_size;
    int objects_chunk_size_to_delete;

    DiskStaticSettings(
            int min_bytes_for_seek_,
            int thread_pool_size_)
        : min_bytes_for_seek(min_bytes_for_seek_)
        , thread_pool_size(thread_pool_size_) {}
};


class DiskStatic : public IDiskRemote, WithContext
{
using SettingsPtr = std::unique_ptr<DiskStaticSettings>;

public:
    DiskStatic(const String & disk_name_,
               const String & files_root_path_url_,
               const String & metadata_path_,
               ContextPtr context,
               SettingsPtr settings_);

    DiskType::Type getType() const override { return DiskType::Type::Static; }

    virtual void startup() override;

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
