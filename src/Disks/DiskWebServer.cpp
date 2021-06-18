#include "DiskWebServer.h"

#include <common/logger_useful.h>

#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ReadIndirectBufferFromWebServer.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Disks/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IDiskRemote.h>

#include <re2/re2.h>

#define DIRECTORY_FILE_PATTERN(prefix) fmt::format("{}-(\\w+)-(\\w+\\.\\w+)", prefix)
#define ROOT_FILE_PATTERN(prefix) fmt::format("{}-(\\w+\\.\\w+)", prefix)

#define MATCH_DIRECTORY_FILE_PATTERN(prefix) fmt::format("{}/(\\w+)/(\\w+\\.\\w+)", prefix)
#define MATCH_ROOT_FILE_PATTERN(prefix) fmt::format("{}/(\\w+\\.\\w+)", prefix)
#define MATCH_DIRECTORY_PATTERN(prefix) fmt::format("{}/(\\w+)", prefix)


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


static const auto store_uuid_prefix = ".*/[\\w]{3}/[\\w]{8}-[\\w]{4}-[\\w]{4}-[\\w]{4}-[\\w]{12}";


/// Fetch contents of .index file from given uri path.
void DiskWebServer::Metadata::initialize(const String & uri_with_path, const String & files_prefix, ContextPtr context) const
{
    ReadWriteBufferFromHTTP metadata_buf(Poco::URI(fs::path(uri_with_path) / ".index"),
                                         Poco::Net::HTTPRequest::HTTP_GET,
                                         ReadWriteBufferFromHTTP::OutStreamCallback(),
                                         ConnectionTimeouts::getHTTPTimeouts(context));
    String directory, file, remote_file_name;
    size_t file_size;

    while (!metadata_buf.eof())
    {
        readText(remote_file_name, metadata_buf);
        assertChar('\t', metadata_buf);
        readIntText(file_size, metadata_buf);
        assertChar('\n', metadata_buf);
        LOG_DEBUG(&Poco::Logger::get("DiskWeb"), "Read file: {}, size: {}", remote_file_name, file_size);

        /*
         * URI/   {prefix}-all_x_x_x-{file}
         *        ...
         *        {prefix}-format_version.txt
         *        {prefix}-detached-{file}
         *        ...
         */
        if (RE2::FullMatch(remote_file_name, re2::RE2(DIRECTORY_FILE_PATTERN(files_prefix)), &directory, &file))
        {
            files[directory].insert({file, file_size});
        }
        else if (RE2::FullMatch(remote_file_name, re2::RE2(ROOT_FILE_PATTERN(files_prefix)), &file))
        {
            files[file].insert({file, file_size});
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected file: {}", remote_file_name);
    }
}


/* Iterate list of files from .index file on a web server (its contents were put
 * into DiskWebServer::Metadata) and convert them into paths as though paths in local fs.
 */
class DiskWebDirectoryIterator final : public IDiskDirectoryIterator
{
public:
    DiskWebDirectoryIterator(DiskWebServer::Metadata & metadata_, const String & directory_root_)
        : metadata(metadata_), iter(metadata.files.begin()), directory_root(directory_root_)
    {
    }

    void next() override { ++iter; }

    bool isValid() const override { return iter != metadata.files.end(); }

    String path() const override
    {
        return fs::path(directory_root) / name();
    }

    String name() const override
    {
        return iter->first;
    }

private:
    DiskWebServer::Metadata & metadata;
    DiskWebServer::FilesDirectory::iterator iter;
    const String directory_root;
};


class ReadBufferFromWebServer final : public ReadIndirectBufferFromRemoteFS<ReadIndirectBufferFromWebServer>
{
public:
    ReadBufferFromWebServer(
            const String & uri_,
            RemoteMetadata metadata_,
            ContextPtr context_,
            size_t max_read_tries_,
            size_t buf_size_)
        : ReadIndirectBufferFromRemoteFS<ReadIndirectBufferFromWebServer>(metadata_)
        , uri(uri_)
        , context(context_)
        , max_read_tries(max_read_tries_)
        , buf_size(buf_size_)
    {
    }

    std::unique_ptr<ReadIndirectBufferFromWebServer> createReadBuffer(const String & path) override
    {
        return std::make_unique<ReadIndirectBufferFromWebServer>(fs::path(uri) / path, context, max_read_tries, buf_size);
    }

private:
    String uri;
    ContextPtr context;
    size_t max_read_tries;
    size_t buf_size;
};


class WriteBufferFromNothing : public WriteBufferFromFile
{
public:
    WriteBufferFromNothing() : WriteBufferFromFile("/dev/null") {}

    void sync() override {}
};


DiskWebServer::DiskWebServer(
            const String & disk_name_,
            const String & uri_,
            const String & metadata_path_,
            ContextPtr context_,
            SettingsPtr settings_)
        : WithContext(context_->getGlobalContext())
        , log(&Poco::Logger::get("DiskWeb"))
        , uri(uri_)
        , name(disk_name_)
        , metadata_path(metadata_path_)
        , settings(std::move(settings_))
{
}


String DiskWebServer::getFileName(const String & path) const
{
    String result;

    if (RE2::FullMatch(path, MATCH_DIRECTORY_FILE_PATTERN(store_uuid_prefix))
        && RE2::Extract(path, MATCH_DIRECTORY_FILE_PATTERN(".*"), fmt::format("{}-\\1-\\2", settings->files_prefix), &result))
        return result;

    if (RE2::FullMatch(path, MATCH_ROOT_FILE_PATTERN(store_uuid_prefix))
        && RE2::Extract(path, MATCH_ROOT_FILE_PATTERN(".*"), fmt::format("{}-\\1", settings->files_prefix), &result))
        return result;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected file: {}", path);
}


bool DiskWebServer::findFileInMetadata(const String & path, FileAndSize & file_info) const
{
    if (metadata.files.empty())
        metadata.initialize(uri, settings->files_prefix, getContext());

    String directory_name, file_name;

    if (RE2::FullMatch(path, MATCH_DIRECTORY_FILE_PATTERN(store_uuid_prefix), &directory_name, &file_name))
    {
        const auto & directory_files = metadata.files.find(directory_name)->second;
        auto file = directory_files.find(file_name);

        if (file == directory_files.end())
            return false;

        file_info = std::make_pair(file_name, file->second);
    }
    else if (RE2::FullMatch(path, MATCH_ROOT_FILE_PATTERN(store_uuid_prefix), &file_name))
    {
        auto file = metadata.files.find(file_name);

        if (file == metadata.files.end())
            return false;

        file_info = std::make_pair(file_name, file->second.find(file_name)->second);
    }
    else
        return false;

    return true;
}


bool DiskWebServer::exists(const String & path) const
{
    LOG_DEBUG(log, "Checking existance of file: {}", path);

    /// Assume root directory exists.
    if (re2::RE2::FullMatch(path, re2::RE2(fmt::format("({})/", store_uuid_prefix))))
        return true;

    FileAndSize file;
    return findFileInMetadata(path, file);
}


std::unique_ptr<ReadBufferFromFileBase> DiskWebServer::readFile(const String & path, size_t buf_size, size_t, size_t, size_t, MMappedFileCache *) const
{
    LOG_DEBUG(log, "Read from file by path: {}", path);

    FileAndSize file;
    if (!findFileInMetadata(path, file))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} not found", path);

    RemoteMetadata meta(uri, fs::path(path).parent_path() / fs::path(path).filename());
    meta.remote_fs_objects.emplace_back(std::make_pair(getFileName(path), file.second));

    auto reader = std::make_unique<ReadBufferFromWebServer>(uri, meta, getContext(), settings->max_read_tries, buf_size);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), settings->min_bytes_for_seek);
}


std::unique_ptr<WriteBufferFromFileBase> DiskWebServer::writeFile(const String &, size_t, WriteMode)
{
    return std::make_unique<WriteBufferFromNothing>();
}


DiskDirectoryIteratorPtr DiskWebServer::iterateDirectory(const String & path)
{
    LOG_DEBUG(log, "Iterate directory: {}", path);
    return std::make_unique<DiskWebDirectoryIterator>(metadata, path);
}


size_t DiskWebServer::getFileSize(const String & path) const
{
    FileAndSize file;
    if (!findFileInMetadata(path, file))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} not found", path);
    return file.second;
}


bool DiskWebServer::isFile(const String & path) const
{
    return RE2::FullMatch(path, MATCH_ROOT_FILE_PATTERN(".*")) || RE2::FullMatch(path, MATCH_DIRECTORY_FILE_PATTERN(".*"));
}


bool DiskWebServer::isDirectory(const String & path) const
{
    return RE2::FullMatch(path, MATCH_DIRECTORY_PATTERN(".*"));
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

        String uri{config.getString(config_prefix + ".endpoint")};
        if (!uri.ends_with('/'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "URI must end with '/', but '{}' doesn't.", uri);

        auto settings = std::make_unique<DiskWebServerSettings>(
            context->getGlobalContext()->getSettingsRef().http_max_single_read_retries,
            config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
            config.getString(config_prefix + ".files_prefix", disk_name));

        String metadata_path = fs::path(context->getPath()) / "disks" / disk_name / "";

        return std::make_shared<DiskWebServer>(disk_name, uri, metadata_path, context, std::move(settings));
    };

    factory.registerDiskType("web", creator);
}

}
