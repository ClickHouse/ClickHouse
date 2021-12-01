#include <Disks/HDFS/DiskHDFS.h>

#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Storages/HDFS/HDFSCommon.h>

#include <IO/SeekAvoidingReadBuffer.h>
#include <Disks/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/WriteIndirectBufferFromRemoteFS.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


class HDFSPathKeeper : public RemoteFSPathKeeper
{
public:
    using Chunk = std::vector<std::string>;
    using Chunks = std::list<Chunk>;

    explicit HDFSPathKeeper(size_t chunk_limit_) : RemoteFSPathKeeper(chunk_limit_) {}

    void addPath(const String & path) override
    {
        if (chunks.empty() || chunks.back().size() >= chunk_limit)
        {
            chunks.push_back(Chunks::value_type());
            chunks.back().reserve(chunk_limit);
        }
        chunks.back().push_back(path.data());
    }

    void removePaths(const std::function<void(Chunk &&)> & remove_chunk_func)
    {
        for (auto & chunk : chunks)
            remove_chunk_func(std::move(chunk));
    }

private:
    Chunks chunks;
};


/// Reads data from HDFS using stored paths in metadata.
class ReadIndirectBufferFromHDFS final : public ReadIndirectBufferFromRemoteFS<ReadBufferFromHDFS>
{
public:
    ReadIndirectBufferFromHDFS(
            const Poco::Util::AbstractConfiguration & config_,
            const String & hdfs_uri_,
            DiskHDFS::Metadata metadata_,
            size_t buf_size_)
        : ReadIndirectBufferFromRemoteFS<ReadBufferFromHDFS>(metadata_)
        , config(config_)
        , buf_size(buf_size_)
    {
        const size_t begin_of_path = hdfs_uri_.find('/', hdfs_uri_.find("//") + 2);
        hdfs_directory = hdfs_uri_.substr(begin_of_path);
        hdfs_uri = hdfs_uri_.substr(0, begin_of_path);
    }

    std::unique_ptr<ReadBufferFromHDFS> createReadBuffer(const String & path) override
    {
        return std::make_unique<ReadBufferFromHDFS>(hdfs_uri, hdfs_directory + path, config, buf_size);
    }

private:
    const Poco::Util::AbstractConfiguration & config;
    String hdfs_uri;
    String hdfs_directory;
    size_t buf_size;
};


DiskHDFS::DiskHDFS(
    const String & disk_name_,
    const String & hdfs_root_path_,
    SettingsPtr settings_,
    const String & metadata_path_,
    const Poco::Util::AbstractConfiguration & config_)
    : IDiskRemote(disk_name_, hdfs_root_path_, metadata_path_, "DiskHDFS", settings_->thread_pool_size)
    , config(config_)
    , hdfs_builder(createHDFSBuilder(hdfs_root_path_, config))
    , hdfs_fs(createHDFSFS(hdfs_builder.get()))
    , settings(std::move(settings_))
{
}


std::unique_ptr<ReadBufferFromFileBase> DiskHDFS::readFile(const String & path, size_t buf_size, size_t, size_t, size_t, MMappedFileCache *) const
{
    auto metadata = readMeta(path);

    LOG_DEBUG(log,
        "Read from file by path: {}. Existing HDFS objects: {}",
        backQuote(metadata_path + path), metadata.remote_fs_objects.size());

    auto reader = std::make_unique<ReadIndirectBufferFromHDFS>(config, remote_fs_root_path, metadata, buf_size);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), settings->min_bytes_for_seek);
}


std::unique_ptr<WriteBufferFromFileBase> DiskHDFS::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    auto metadata = readOrCreateMetaForWriting(path, mode);

    /// Path to store new HDFS object.
    auto file_name = getRandomName();
    auto hdfs_path = remote_fs_root_path + file_name;

    LOG_DEBUG(log, "{} to file by path: {}. HDFS path: {}", mode == WriteMode::Rewrite ? "Write" : "Append",
              backQuote(metadata_path + path), remote_fs_root_path + hdfs_path);

    /// Single O_WRONLY in libhdfs adds O_TRUNC
    auto hdfs_buffer = std::make_unique<WriteBufferFromHDFS>(hdfs_path,
                                                             config, buf_size,
                                                             mode == WriteMode::Rewrite ? O_WRONLY :  O_WRONLY | O_APPEND);

    return std::make_unique<WriteIndirectBufferFromRemoteFS<WriteBufferFromHDFS>>(std::move(hdfs_buffer),
                                                                                std::move(metadata),
                                                                                file_name);
}


RemoteFSPathKeeperPtr DiskHDFS::createFSPathKeeper() const
{
    return std::make_shared<HDFSPathKeeper>(settings->objects_chunk_size_to_delete);
}


void DiskHDFS::removeFromRemoteFS(RemoteFSPathKeeperPtr fs_paths_keeper)
{
    auto * hdfs_paths_keeper = dynamic_cast<HDFSPathKeeper *>(fs_paths_keeper.get());
    if (hdfs_paths_keeper)
        hdfs_paths_keeper->removePaths([&](std::vector<std::string> && chunk)
        {
            for (const auto & hdfs_object_path : chunk)
            {
                const String & hdfs_path = hdfs_object_path;
                const size_t begin_of_path = hdfs_path.find('/', hdfs_path.find("//") + 2);

                /// Add path from root to file name
                int res = hdfsDelete(hdfs_fs.get(), hdfs_path.substr(begin_of_path).c_str(), 0);
                if (res == -1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "HDFSDelete failed with path: " + hdfs_path);
            }
        });
}


namespace
{
std::unique_ptr<DiskHDFSSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    return std::make_unique<DiskHDFSSettings>(
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".thread_pool_size", 16),
        config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000));
}
}

void registerDiskHDFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context_) -> DiskPtr
    {
        fs::path disk = fs::path(context_->getPath()) / "disks" / name;
        fs::create_directories(disk);

        String uri{config.getString(config_prefix + ".endpoint")};
        checkHDFSURL(uri);

        if (uri.back() != '/')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDFS path must ends with '/', but '{}' doesn't.", uri);

        String metadata_path = context_->getPath() + "disks/" + name + "/";

        return std::make_shared<DiskHDFS>(
            name, uri,
            getSettings(config, config_prefix),
            metadata_path, config);
    };

    factory.registerDiskType("hdfs", creator);
}

}
