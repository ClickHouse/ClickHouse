#include <Disks/HDFS/DiskHDFS.h>

#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include "ReadIndirectBufferFromHDFS.h"
#include "WriteIndirectBufferFromHDFS.h"
#include "DiskHDFSDirectoryIterator.h"

#include <random>
#include <utility>
#include <memory>
#include <Poco/File.h>
#include <Interpreters/Context.h>
#include <Common/checkStackSize.h>
#include <Common/createHardLink.h>
#include <Common/quoteString.h>
#include <Common/filesystemHelpers.h>
#include <common/logger_useful.h>
#include <Common/thread_local_rng.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int PATH_ACCESS_DENIED;
}


DiskHDFS::DiskHDFS(
    const String & disk_name_,
    const String & hdfs_root_path_,
    const String & metadata_path_,
    const Poco::Util::AbstractConfiguration & config_)
    : IDiskRemote(disk_name_, hdfs_root_path_, metadata_path_, "DiskHDFS")
    , config(config_)
    , hdfs_builder(createHDFSBuilder(hdfs_root_path_, config))
    , hdfs_fs(createHDFSFS(hdfs_builder.get()))
{
}


std::unique_ptr<ReadBufferFromFileBase> DiskHDFS::readFile(const String & path, size_t buf_size, size_t, size_t, size_t, MMappedFileCache *) const
{
    auto metadata = readMeta(path);

    LOG_DEBUG(log,
        "Read from file by path: {}. Existing HDFS objects: {}",
        backQuote(metadata_path + path), metadata.remote_fs_objects.size());

    return std::make_unique<ReadIndirectBufferFromHDFS>(config, remote_fs_root_path, "", metadata, buf_size);
}


std::unique_ptr<WriteBufferFromFileBase> DiskHDFS::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    bool exist = exists(path);

    if (exist && readMeta(path).read_only)
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File is read-only: " + path);

    /// Path to store new HDFS object.
    auto file_name = getRandomName();
    auto hdfs_path = remote_fs_root_path + file_name;

    if (!exist || mode == WriteMode::Rewrite)
    {
        /// If metadata file exists - remove and new.
        if (exist)
            removeFile(path);

        auto metadata = createMeta(path);
        /// Save empty metadata to disk to have ability to get file size while buffer is not finalized.
        metadata.save();

        LOG_DEBUG(log,
            "Write to file by path: {}. New hdfs path: {}", backQuote(metadata_path + path), hdfs_path);

        return std::make_unique<WriteIndirectBufferFromHDFS>(config, hdfs_path, file_name, metadata, buf_size, O_WRONLY);
}
    else
    {
        auto metadata = readMeta(path);

        LOG_DEBUG(log,
            "Append to file by path: {}. New hdfs path: {}. Existing HDFS objects: {}",
            backQuote(metadata_path + path), hdfs_path, metadata.remote_fs_objects.size());

        return std::make_unique<WriteIndirectBufferFromHDFS>(
                config, hdfs_path, file_name, metadata, buf_size, O_WRONLY | O_APPEND);
    }
}


void DiskHDFS::removeFromRemoteFS(const Metadata & metadata)
{
    for (const auto & [hdfs_object_path, _] : metadata.remote_fs_objects)
    {
        const size_t begin_of_path = remote_fs_root_path.find('/', remote_fs_root_path.find("//") + 2);
        const String hdfs_path = remote_fs_root_path.substr(begin_of_path) + hdfs_object_path;

        LOG_DEBUG(log, "Removing file by path: {}", hdfs_path);
        int res = hdfsDelete(hdfs_fs.get(), hdfs_path.c_str(), 0);
        if (res == -1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "HDFSDelete failed with path: " + hdfs_path);
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

        return std::make_shared<DiskHDFS>(name, uri, metadata_path, config);
    };

    factory.registerDiskType("hdfs", creator);
}

}
