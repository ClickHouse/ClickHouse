#include "DiskWebServer.h"

#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>

#include <IO/ConnectionTimeoutsContext.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFile.h>

#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>

#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>

#include <Storages/MergeTree/MergeTreeData.h>

#include <Poco/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int NETWORK_ERROR;
}


void DiskWebServer::initialize(const String & uri_path) const
{
    std::vector<String> directories_to_load;
    LOG_TRACE(log, "Loading metadata for directory: {}", uri_path);
    try
    {
        Poco::Net::HTTPBasicCredentials credentials{};
        ReadWriteBufferFromHTTP metadata_buf(Poco::URI(fs::path(uri_path) / ".index"),
                                            Poco::Net::HTTPRequest::HTTP_GET,
                                            ReadWriteBufferFromHTTP::OutStreamCallback(),
                                            ConnectionTimeouts::getHTTPTimeouts(getContext()),
                                            credentials);
        String file_name;
        FileData file_data{};

        String dir_name = fs::path(uri_path.substr(url.size())) / "";
        LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Adding directory: {}", dir_name);

        while (!metadata_buf.eof())
        {
            readText(file_name, metadata_buf);
            assertChar('\t', metadata_buf);

            bool is_directory;
            readBoolText(is_directory, metadata_buf);
            if (!is_directory)
            {
                assertChar('\t', metadata_buf);
                readIntText(file_data.size, metadata_buf);
            }
            assertChar('\n', metadata_buf);

            file_data.type = is_directory ? FileType::Directory : FileType::File;
            String file_path = fs::path(uri_path) / file_name;
            if (file_data.type == FileType::Directory)
            {
                directories_to_load.push_back(file_path);
                // file_path = fs::path(file_path) / "";
            }

            file_path = file_path.substr(url.size());
            files.emplace(std::make_pair(file_path, file_data));
            LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Adding file: {}, size: {}", file_path, file_data.size);
        }

        files.emplace(std::make_pair(dir_name, FileData({ .type = FileType::Directory })));
    }
    catch (Exception & e)
    {
        e.addMessage("while loading disk metadata");
        throw;
    }

    for (const auto & directory_path : directories_to_load)
        initialize(directory_path);
}


class DiskWebServerDirectoryIterator final : public IDirectoryIterator
{
public:
    explicit DiskWebServerDirectoryIterator(std::vector<fs::path> && dir_file_paths_)
        : dir_file_paths(std::move(dir_file_paths_)), iter(dir_file_paths.begin()) {}

    void next() override { ++iter; }

    bool isValid() const override { return iter != dir_file_paths.end(); }

    String path() const override { return iter->string(); }

    String name() const override { return iter->filename(); }

private:
    std::vector<fs::path> dir_file_paths;
    std::vector<fs::path>::iterator iter;
};


DiskWebServer::DiskWebServer(
            const String & disk_name_,
            const String & url_,
            ContextPtr context_,
            size_t min_bytes_for_seek_)
        : WithContext(context_->getGlobalContext())
        , log(&Poco::Logger::get("DiskWeb"))
        , url(url_)
        , name(disk_name_)
        , min_bytes_for_seek(min_bytes_for_seek_)
{
}


bool DiskWebServer::exists(const String & path) const
{
    LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Checking existence of path: {}", path);

    if (files.find(path) != files.end())
        return true;

    if (path.ends_with(MergeTreeData::FORMAT_VERSION_FILE_NAME) && files.find(fs::path(path).parent_path() / "") == files.end())
    {
        try
        {
            initialize(fs::path(url) / fs::path(path).parent_path());
            return files.find(path) != files.end();
        }
        catch (...)
        {
            const auto message = getCurrentExceptionMessage(false);
            bool can_throw = CurrentThread::isInitialized() && CurrentThread::get().getQueryContext();
            if (can_throw)
                throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot load disk metadata. Error: {}", message);

            LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Cannot load disk metadata. Error: {}", message);
            return false;
        }
    }

    return false;
}


std::unique_ptr<ReadBufferFromFileBase> DiskWebServer::readFile(const String & path, const ReadSettings & read_settings, std::optional<size_t>, std::optional<size_t>) const
{
    LOG_TRACE(log, "Read from path: {}", path);
    auto iter = files.find(path);
    if (iter == files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File path {} does not exist", path);

    auto fs_path = fs::path(url) / path;
    auto remote_path = fs_path.parent_path() / (escapeForFileName(fs_path.stem()) + fs_path.extension().string());
    remote_path = remote_path.string().substr(url.size());

    StoredObjects objects;
    objects.emplace_back(remote_path, iter->second.size);

    auto web_impl = std::make_unique<ReadBufferFromWebServerGather>(url, objects, getContext(), read_settings);

    if (read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        auto reader = IObjectStorage::getThreadPoolReader();
        return std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(reader, read_settings, std::move(web_impl), min_bytes_for_seek);
    }
    else
    {
        auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(web_impl));
        return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), min_bytes_for_seek);
    }
}


DirectoryIteratorPtr DiskWebServer::iterateDirectory(const String & path) const
{
    std::vector<fs::path> dir_file_paths;
    if (files.find(path) == files.end())
    {
        try
        {
            initialize(fs::path(url) / path);
        }
        catch (...)
        {
            const auto message = getCurrentExceptionMessage(false);
            bool can_throw = CurrentThread::isInitialized() && CurrentThread::get().getQueryContext();
            if (can_throw)
                throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot load disk metadata. Error: {}", message);

            LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Cannot load disk metadata. Error: {}", message);
            return std::make_unique<DiskWebServerDirectoryIterator>(std::move(dir_file_paths));
        }
    }

    if (files.find(path) == files.end())
        throw Exception("Directory '" + path + "' does not exist", ErrorCodes::DIRECTORY_DOESNT_EXIST);

    for (const auto & file : files)
        if (parentPath(file.first) == path)
            dir_file_paths.emplace_back(file.first);

    LOG_TRACE(log, "Iterate directory {} with {} files", path, dir_file_paths.size());
    return std::make_unique<DiskWebServerDirectoryIterator>(std::move(dir_file_paths));
}


size_t DiskWebServer::getFileSize(const String & path) const
{
    auto iter = files.find(path);
    if (iter == files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File path {} does not exist", path);

    return iter->second.size;
}


bool DiskWebServer::isFile(const String & path) const
{
    auto iter = files.find(path);
    if (iter == files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File path {} does not exist", path);

    return iter->second.type == FileType::File;
}


bool DiskWebServer::isDirectory(const String & path) const
{
    auto iter = files.find(path);
    if (iter == files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File path {} does not exist", path);

    return iter->second.type == FileType::Directory;
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
        try
        {
            Poco::URI poco_uri(uri);
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad URI: `{}`. Error: {}", uri, e.what());
        }

        return std::make_shared<DiskWebServer>(disk_name, uri, context, config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024));
    };

    factory.registerDiskType("web", creator);
}

}
