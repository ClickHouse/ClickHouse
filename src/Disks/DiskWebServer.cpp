#include "DiskWebServer.h"

#include <common/logger_useful.h>
#include <Common/quoteString.h>

#include <Interpreters/Context.h>

#include <Disks/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/WriteIndirectBufferFromRemoteFS.h>

#include <IO/WriteBufferFromHTTP.h>
#include <IO/ReadIndirectBufferFromWebServer.h>
#include <IO/SeekAvoidingReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


class ReadBufferFromWebServer final : public ReadIndirectBufferFromRemoteFS<ReadIndirectBufferFromWebServer>
{
public:
    ReadBufferFromWebServer(
            const String & url_,
            DiskWebServer::Metadata metadata_,
            ContextPtr context_,
            size_t max_read_tries_,
            size_t buf_size_)
        : ReadIndirectBufferFromRemoteFS<ReadIndirectBufferFromWebServer>(metadata_)
        , url(url_)
        , context(context_)
        , max_read_tries(max_read_tries_)
        , buf_size(buf_size_)
    {
    }

    std::unique_ptr<ReadIndirectBufferFromWebServer> createReadBuffer(const String & path) override
    {
        return std::make_unique<ReadIndirectBufferFromWebServer>(fs::path(url) / path, context, max_read_tries, buf_size);
    }

private:
    String url;
    ContextPtr context;
    size_t max_read_tries;
    size_t buf_size;
};


DiskWebServer::DiskWebServer(
            const String & disk_name_,
            const String & files_root_path_url_,
            const String & metadata_path_,
            ContextPtr context_,
            SettingsPtr settings_)
        : IDiskRemote(disk_name_, files_root_path_url_, metadata_path_, "DiskWebServer", settings_->thread_pool_size)
        , WithContext(context_->getGlobalContext())
        , settings(std::move(settings_))
{
}


std::unique_ptr<ReadBufferFromFileBase> DiskWebServer::readFile(const String & path, size_t buf_size, size_t, size_t, size_t, MMappedFileCache *) const
{
    auto metadata = readMeta(path);

    LOG_DEBUG(log, "Read from file by path: {}. Existing objects: {}", backQuote(metadata_path + path), metadata.remote_fs_objects.size());

    auto reader = std::make_unique<ReadBufferFromWebServer>(remote_fs_root_path, metadata, getContext(), 1, buf_size);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), settings->min_bytes_for_seek);
}


std::unique_ptr<WriteBufferFromFileBase> DiskWebServer::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    auto metadata = readOrCreateMetaForWriting(path, mode);

    auto file_name = generateName();
    String file_path = fs::path(remote_fs_root_path) / file_name;

    LOG_DEBUG(log, "Write to file url: {}", file_path);

    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(getContext());
    Poco::URI uri(file_path);
    auto writer = std::make_unique<WriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_PUT, timeouts, buf_size);

    return std::make_unique<WriteIndirectBufferFromRemoteFS<WriteBufferFromHTTP>>(std::move(writer), std::move(metadata), file_name);
}


void registerDiskWebServer(DiskFactory & factory)
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

        auto settings = std::make_unique<DiskWebServerSettings>(
            context->getGlobalContext()->getSettingsRef().http_max_single_read_retries,
            config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
            config.getInt(config_prefix + ".thread_pool_size", 16));

        String metadata_path = fs::path(context->getPath()) / "disks" / disk_name / "";

        return std::make_shared<DiskWebServer>(disk_name, url, metadata_path, context, std::move(settings));
    };

    factory.registerDiskType("web", creator);
}

}
