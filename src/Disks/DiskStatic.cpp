#include "DiskStatic.h"

#include <common/logger_useful.h>
#include <Common/quoteString.h>

#include <Interpreters/Context.h>
#include <Disks/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/WriteIndirectBufferFromRemoteFS.h>
#include <IO/ReadBufferFromStatic.h>
#include <IO/WriteBufferFromStatic.h>
#include <IO/WriteBufferFromHTTP.h>
#include <IO/SeekAvoidingReadBuffer.h>

#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

class ReadIndirectBufferFromStatic final : public ReadIndirectBufferFromRemoteFS<ReadBufferFromStatic>
{
public:
    ReadIndirectBufferFromStatic(
            const String & url_,
            DiskStatic::Metadata metadata_,
            ContextPtr context_,
            size_t max_read_tries_,
            size_t buf_size_)
        : ReadIndirectBufferFromRemoteFS<ReadBufferFromStatic>(metadata_)
        , url(url_)
        , context(context_)
        , max_read_tries(max_read_tries_)
        , buf_size(buf_size_)
    {
    }

    std::unique_ptr<ReadBufferFromStatic> createReadBuffer(const String & path) override
    {
        return std::make_unique<ReadBufferFromStatic>(url + path, context, max_read_tries, buf_size);
    }

private:
    String url;
    ContextPtr context;
    size_t max_read_tries;
    size_t buf_size;
};


DiskStatic::DiskStatic(const String & disk_name_,
                       const String & files_root_path_url_,
                       const String & metadata_path_,
                       ContextPtr context_,
                       SettingsPtr settings_)
        : IDiskRemote(disk_name_, files_root_path_url_, metadata_path_, "DiskStatic", settings_->thread_pool_size)
        , WithContext(context_->getGlobalContext())
        , settings(std::move(settings_))
{
}


void DiskStatic::startup()
{
}


std::unique_ptr<ReadBufferFromFileBase> DiskStatic::readFile(const String & path, size_t buf_size, size_t, size_t, size_t, MMappedFileCache *) const
{
    auto metadata = readMeta(path);

    LOG_DEBUG(log, "Read from file by path: {}. Existing objects: {}",
        backQuote(metadata_path + path), metadata.remote_fs_objects.size());

    auto reader = std::make_unique<ReadIndirectBufferFromStatic>(remote_fs_root_path, metadata, getContext(), 1, buf_size);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), settings->min_bytes_for_seek);
}


std::unique_ptr<WriteBufferFromFileBase> DiskStatic::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    auto metadata = readOrCreateMetaForWriting(path, mode);
    /// Update read_only flag in IDiskRemote::metadata.
    /// setReadOnly();

    auto file_name = generateName();
    auto file_path = remote_fs_root_path + file_name;

    LOG_DEBUG(log, "Write to file url: {}", file_path);

    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(getContext());

    Poco::URI uri(file_path);
    auto writer = std::make_unique<WriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_PUT, timeouts, buf_size);

    return std::make_unique<WriteIndirectBufferFromRemoteFS<WriteBufferFromHTTP>>(std::move(writer),
                                                                                  std::move(metadata),
                                                                                  file_name);
}


namespace
{
std::unique_ptr<DiskStaticSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    return std::make_unique<DiskStaticSettings>(
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".thread_pool_size", 16));
}
}

void registerDiskStatic(DiskFactory & factory)
{
    auto creator = [](const String & disk_name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextConstPtr context) -> DiskPtr
    {
        fs::path disk = fs::path(context->getPath()) / "disks" / disk_name;
        fs::create_directories(disk);

        String url{config.getString(config_prefix + ".endpoint")};
        if (!url.ends_with('/'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "URL must end with '/', but '{}' doesn't.", url);

        String metadata_path = fs::path(context->getPath()) / "disks" / disk_name / "";

        return std::make_shared<DiskStatic>(disk_name, url, metadata_path, context, getSettings(config, config_prefix));
    };

    factory.registerDiskType("static", creator);
}


}
