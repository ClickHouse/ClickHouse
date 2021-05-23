#include <Disks/HDFS/DiskHDFS.h>

#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
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
    : IDiskRemote(disk_name_, hdfs_root_path_, metadata_path_, "DiskHDFS")
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


void DiskHDFS::removeFromRemoteFS(const RemoteFSPathKeeper & fs_paths_keeper)
{
    for (const auto & chunk : fs_paths_keeper)
    {
        for (const auto & hdfs_object_path : chunk)
        {
            const String & hdfs_path = hdfs_object_path.GetKey();
            const size_t begin_of_path = hdfs_path.find('/', hdfs_path.find("//") + 2);

            /// Add path from root to file name
            int res = hdfsDelete(hdfs_fs.get(), hdfs_path.substr(begin_of_path).c_str(), 0);
            if (res == -1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "HDFSDelete failed with path: " + hdfs_path);
        }
    }
}


namespace
{
std::unique_ptr<DiskHDFSSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    return std::make_unique<DiskHDFSSettings>(config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024));
}
}

void registerDiskHDFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextConstPtr context_) -> DiskPtr
    {
        Poco::File disk{context_->getPath() + "disks/" + name};
        disk.createDirectories();

        String uri{config.getString(config_prefix + ".endpoint")};

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
