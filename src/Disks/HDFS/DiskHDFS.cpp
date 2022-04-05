#include <Disks/HDFS/DiskHDFS.h>

#if USE_HDFS

#include <Disks/DiskLocal.h>
#include <Disks/RemoteDisksCommon.h>

#include <IO/SeekAvoidingReadBuffer.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Storages/HDFS/HDFSCommon.h>

#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>

#include <boost/algorithm/string/predicate.hpp>

#include <base/logger_useful.h>
#include <base/FnTraits.h>


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

    void removePaths(Fn<void(Chunk &&)> auto && remove_chunk_func)
    {
        for (auto & chunk : chunks)
            remove_chunk_func(std::move(chunk));
    }

private:
    Chunks chunks;
};


DiskHDFS::DiskHDFS(
    const String & disk_name_,
    const String & hdfs_root_path_,
    SettingsPtr settings_,
    DiskPtr metadata_disk_,
    const Poco::Util::AbstractConfiguration & config_)
    : IDiskRemote(disk_name_, hdfs_root_path_, metadata_disk_, nullptr, "DiskHDFS", settings_->thread_pool_size)
    , config(config_)
    , hdfs_builder(createHDFSBuilder(hdfs_root_path_, config))
    , hdfs_fs(createHDFSFS(hdfs_builder.get()))
    , settings(std::move(settings_))
{
}


std::unique_ptr<ReadBufferFromFileBase> DiskHDFS::readFile(const String & path, const ReadSettings & read_settings, std::optional<size_t>, std::optional<size_t>) const
{
    auto metadata = readMetadata(path);

    LOG_TEST(log,
        "Read from file by path: {}. Existing HDFS objects: {}",
        backQuote(metadata_disk->getPath() + path), metadata.remote_fs_objects.size());

    auto hdfs_impl = std::make_unique<ReadBufferFromHDFSGather>(path, config, remote_fs_root_path, metadata, read_settings);
    auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(hdfs_impl));
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), settings->min_bytes_for_seek);
}


std::unique_ptr<WriteBufferFromFileBase> DiskHDFS::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    /// Path to store new HDFS object.
    auto file_name = getRandomName();
    auto hdfs_path = remote_fs_root_path + file_name;

    LOG_TRACE(log, "{} to file by path: {}. HDFS path: {}", mode == WriteMode::Rewrite ? "Write" : "Append",
              backQuote(metadata_disk->getPath() + path), hdfs_path);

    /// Single O_WRONLY in libhdfs adds O_TRUNC
    auto hdfs_buffer = std::make_unique<WriteBufferFromHDFS>(hdfs_path,
                                                             config, settings->replication, buf_size,
                                                             mode == WriteMode::Rewrite ? O_WRONLY :  O_WRONLY | O_APPEND);
    auto create_metadata_callback = [this, path, mode, file_name] (size_t count)
    {
        readOrCreateUpdateAndStoreMetadata(path, mode, false, [file_name, count] (Metadata & metadata) { metadata.addObject(file_name, count); return true; });
    };

    return std::make_unique<WriteIndirectBufferFromRemoteFS>(std::move(hdfs_buffer), std::move(create_metadata_callback), path);
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

bool DiskHDFS::checkUniqueId(const String & hdfs_uri) const
{
    if (!boost::algorithm::starts_with(hdfs_uri, remote_fs_root_path))
        return false;
    const size_t begin_of_path = hdfs_uri.find('/', hdfs_uri.find("//") + 2);
    const String remote_fs_object_path = hdfs_uri.substr(begin_of_path);
    return (0 == hdfsExists(hdfs_fs.get(), remote_fs_object_path.c_str()));
}

namespace
{
std::unique_ptr<DiskHDFSSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const Settings & settings)
{
    return std::make_unique<DiskHDFSSettings>(
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".thread_pool_size", 16),
        config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000),
        settings.hdfs_replication);
}
}

void registerDiskHDFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context_,
                      const DisksMap & /*map*/) -> DiskPtr
    {
        String uri{config.getString(config_prefix + ".endpoint")};
        checkHDFSURL(uri);

        if (uri.back() != '/')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDFS path must ends with '/', but '{}' doesn't.", uri);

        auto metadata_disk = prepareForLocalMetadata(name, config, config_prefix, context_).second;

        return std::make_shared<DiskHDFS>(
            name, uri,
            getSettings(config, config_prefix, context_->getSettingsRef()),
            metadata_disk, config);
    };

    factory.registerDiskType("hdfs", creator);
}

}
#endif
