#include "DiskWebServer.h"

#include <common/logger_useful.h>

#include <IO/ReadWriteBufferFromHTTP.h>
#include <Disks/ReadIndirectBufferFromWebServer.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Disks/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IDiskRemote.h>
#include <Access/AccessControlManager.h>
#include <Poco/Exception.h>

#include <re2/re2.h>


#define UUID_PATTERN "[\\w]{8}-[\\w]{4}-[\\w]{4}-[\\w]{4}-[\\w]{12}"
#define EXTRACT_UUID_PATTERN fmt::format(".*/({})/.*", UUID_PATTERN)

#define DIRECTORY_FILE_PATTERN(prefix) fmt::format("{}-({})-(\\w+)-(\\w+\\.\\w+)", prefix, UUID_PATTERN)
#define ROOT_FILE_PATTERN(prefix) fmt::format("{}-({})-(\\w+\\.\\w+)", prefix, UUID_PATTERN)

#define MATCH_DIRECTORY_FILE_PATTERN fmt::format(".*/({})/(\\w+)/(\\w+\\.\\w+)", UUID_PATTERN)
#define MATCH_ROOT_FILE_PATTERN fmt::format(".*/({})/(\\w+\\.\\w+)", UUID_PATTERN)


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int NOT_IMPLEMENTED;
}


void DiskWebServer::Metadata::initialize(const String & uri_with_path, const String & files_prefix, const String & table_uuid, ContextPtr context) const
{
    ReadWriteBufferFromHTTP metadata_buf(Poco::URI(fs::path(uri_with_path) / (".index-" + table_uuid)),
                                         Poco::Net::HTTPRequest::HTTP_GET,
                                         ReadWriteBufferFromHTTP::OutStreamCallback(),
                                         ConnectionTimeouts::getHTTPTimeouts(context));
    String uuid, directory, file, remote_file_name;
    size_t file_size;

    while (!metadata_buf.eof())
    {
        readText(remote_file_name, metadata_buf);
        assertChar('\t', metadata_buf);
        readIntText(file_size, metadata_buf);
        assertChar('\n', metadata_buf);
        LOG_DEBUG(&Poco::Logger::get("DiskWeb"), "Read file: {}, size: {}", remote_file_name, file_size);

        /*
         * URI/   {prefix}-{uuid}-all_x_x_x-{file}
         *        ...
         *        {prefix}-{uuid}-format_version.txt
         *        {prefix}-{uuid}-detached-{file}
         *        ...
        **/
        if (RE2::FullMatch(remote_file_name, DIRECTORY_FILE_PATTERN(files_prefix), &uuid, &directory, &file))
        {
            if (uuid != table_uuid)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected uuid: {}, expected: {}", uuid, table_uuid);

            tables_data[uuid][directory].emplace(File(file, file_size));
        }
        else if (RE2::FullMatch(remote_file_name, ROOT_FILE_PATTERN(files_prefix), &uuid, &file))
        {
            if (uuid != table_uuid)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected uuid: {}, expected: {}", uuid, table_uuid);

            tables_data[uuid][file].emplace(File(file, file_size));
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected file: {}", remote_file_name);
    }
}


template <typename T>
class DiskWebDirectoryIterator final : public IDiskDirectoryIterator
{
public:
    using Directory = std::unordered_map<String, T>;

    DiskWebDirectoryIterator(Directory & directory_, const String & directory_root_)
        : directory(directory_), iter(directory.begin()), directory_root(directory_root_)
    {
    }

    void next() override { ++iter; }

    bool isValid() const override
    {
        return iter != directory.end();
    }

    String path() const override
    {
        return fs::path(directory_root) / name();
    }

    String name() const override
    {
        return iter->first;
    }

private:
    Directory & directory;
    typename Directory::iterator iter;
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

    if (RE2::FullMatch(path, MATCH_DIRECTORY_FILE_PATTERN)
        && RE2::Extract(path, MATCH_DIRECTORY_FILE_PATTERN, fmt::format(R"({}-\1-\2-\3)", settings->files_prefix), &result))
        return result;

    if (RE2::FullMatch(path, MATCH_ROOT_FILE_PATTERN)
        && RE2::Extract(path, MATCH_ROOT_FILE_PATTERN, fmt::format(R"({}-\1-\2)", settings->files_prefix), &result))
        return result;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected file: {}", path);
}


bool DiskWebServer::findFileInMetadata(const String & path, File & file_info) const
{
    String table_uuid, directory_name, file_name;

    if (RE2::FullMatch(path, MATCH_DIRECTORY_FILE_PATTERN, &table_uuid, &directory_name, &file_name)
       || RE2::FullMatch(path, MATCH_ROOT_FILE_PATTERN, &table_uuid, &file_name))
    {
        if (directory_name.empty())
            directory_name = file_name;

        if (!metadata.tables_data.count(table_uuid))
            return false;

        if (!metadata.tables_data[table_uuid].count(directory_name))
            return false;

        const auto & files = metadata.tables_data[table_uuid][directory_name];
        auto file = files.find(File(file_name));
        if (file == files.end())
            return false;

        file_info = *file;
        return true;
    }

    return false;
}


bool DiskWebServer::exists(const String & path) const
{
    LOG_DEBUG(log, "Checking existence of file: {}", path);

    File file;
    return findFileInMetadata(path, file);
}


std::unique_ptr<ReadBufferFromFileBase> DiskWebServer::readFile(const String & path, size_t buf_size, size_t, size_t, size_t, MMappedFileCache *) const
{
    LOG_DEBUG(log, "Read from file by path: {}", path);

    File file;
    if (!findFileInMetadata(path, file))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} not found", path);

    RemoteMetadata meta(uri, fs::path(path).parent_path() / fs::path(path).filename());
    meta.remote_fs_objects.emplace_back(std::make_pair(getFileName(path), file.size));

    auto reader = std::make_unique<ReadBufferFromWebServer>(uri, meta, getContext(), settings->max_read_tries, buf_size);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), settings->min_bytes_for_seek);
}


std::unique_ptr<WriteBufferFromFileBase> DiskWebServer::writeFile(const String & path, size_t, WriteMode)
{
    if (path.ends_with("format_version.txt"))
        return std::make_unique<WriteBufferFromNothing>();

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk {} is read-only", getName());
}


DiskDirectoryIteratorPtr DiskWebServer::iterateDirectory(const String & path)
{
    LOG_DEBUG(log, "Iterate directory: {}", path);
    String uuid;

    if (RE2::FullMatch(path, ".*/store/"))
        return std::make_unique<DiskWebDirectoryIterator<RootDirectory>>(metadata.tables_data, path);

    if (!RE2::Extract(path, EXTRACT_UUID_PATTERN, "\\1", &uuid))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot extract uuid for: {}", path);

    /// Do not throw if it is not a query, but disk load.
    bool can_throw = CurrentThread::isInitialized() && CurrentThread::get().getQueryContext();

    try
    {
        if (!metadata.tables_data.count(uuid))
            metadata.initialize(uri, settings->files_prefix, uuid, getContext());
    }
    catch (const Poco::Exception &)
    {
        const auto message = getCurrentExceptionMessage(false);
        if (can_throw)
        {
            throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot load disk metadata. Error: {}", message);
        }

        LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Cannot load disk metadata. Error: {}", message);
        /// Empty iterator.
        return std::make_unique<DiskWebDirectoryIterator<Directory>>(metadata.tables_data[""], path);
    }

    return std::make_unique<DiskWebDirectoryIterator<Directory>>(metadata.tables_data[uuid], path);
}


size_t DiskWebServer::getFileSize(const String & path) const
{
    File file;
    if (!findFileInMetadata(path, file))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} not found", path);
    return file.size;
}


bool DiskWebServer::isFile(const String & path) const
{
    return RE2::FullMatch(path, ".*/\\w+.\\w+");
}


bool DiskWebServer::isDirectory(const String & path) const
{
    return RE2::FullMatch(path, ".*/\\w+");
}


void registerDiskWebServer(DiskFactory & factory)
{
    auto creator = [](const String & disk_name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      const DisksMap & /*map*/) -> DiskPtr
    {
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
