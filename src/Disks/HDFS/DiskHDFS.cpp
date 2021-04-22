#include <Disks/HDFS/DiskHDFS.h>

#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include "ReadIndirectBufferFromHDFS.h"
#include "WriteIndirectBufferFromHDFS.h"
#include <IO/SeekAvoidingReadBuffer.h>
#include <Common/checkStackSize.h>
#include <Common/quoteString.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int PATH_ACCESS_DENIED;
    extern const int CANNOT_DELETE_DIRECTORY;
    extern const int UNKNOWN_FORMAT;
}


DiskHDFS::DiskHDFS(
    const String & disk_name_,
    const String & hdfs_root_path_,
    const String & metadata_path_,
    const Poco::Util::AbstractConfiguration & config_,
    size_t min_bytes_for_seek_)
    : IDiskRemote(disk_name_, hdfs_root_path_, metadata_path_, "DiskHDFS")
    , config(config_)
    , min_bytes_for_seek(min_bytes_for_seek_)
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

    auto reader = std::make_unique<ReadIndirectBufferFromHDFS>(config, remote_fs_root_path, metadata, buf_size);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), min_bytes_for_seek);
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
        /// Add path from root to file name
        const size_t begin_of_path = remote_fs_root_path.find('/', remote_fs_root_path.find("//") + 2);
        const String hdfs_path = remote_fs_root_path.substr(begin_of_path) + hdfs_object_path;

        int res = hdfsDelete(hdfs_fs.get(), hdfs_path.c_str(), 0);
        if (res == -1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "HDFSDelete failed with path: " + hdfs_path);
    }
}


void DiskHDFS::removeSharedFile(const String & path, bool keep_in_remote_fs)
{
    removeMeta(path, keep_in_remote_fs);
}


void DiskHDFS::removeSharedRecursive(const String & path, bool keep_in_remote_fs)
{
    removeMetaRecursive(path, keep_in_remote_fs);
}


void DiskHDFS::removeFileIfExists(const String & path)
{
    if (Poco::File(metadata_path + path).exists())
        removeMeta(path, /* keep_in_remote_fs */ false);
}


void DiskHDFS::removeRecursive(const String & path)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        removeFile(path);
    }
    else
    {
        for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
            removeRecursive(it->path());
        file.remove();
    }
}


void DiskHDFS::removeMeta(const String & path, bool keep_in_remote_fs)
{
    Poco::File file(metadata_path + path);

    if (!file.isFile())
        throw Exception(ErrorCodes::CANNOT_DELETE_DIRECTORY, "Path '{}' is a directory", path);

    try
    {
        auto metadata = readMeta(path);

        /// If there is no references - delete content from remote FS.
        if (metadata.ref_count == 0)
        {
            file.remove();

            if (!keep_in_remote_fs)
                removeFromRemoteFS(metadata);
        }
        else /// In other case decrement number of references, save metadata and delete file.
        {
            --metadata.ref_count;
            metadata.save();
            file.remove();
        }
    }
    catch (const Exception & e)
    {
        /// If it's impossible to read meta - just remove it from FS.
        if (e.code() == ErrorCodes::UNKNOWN_FORMAT)
        {
            LOG_WARNING(
                log,
                "Metadata file {} can't be read by reason: {}. Removing it forcibly.",
                backQuote(path),
                e.nested() ? e.nested()->message() : e.message());

            file.remove();
        }
        else
            throw;
    }
}


void DiskHDFS::removeMetaRecursive(const String & path, bool keep_in_remote_fs)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        removeMeta(path, keep_in_remote_fs);
    }
    else
    {
        for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
            removeMetaRecursive(it->path(), keep_in_remote_fs);
        file.remove();
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
            name, uri, metadata_path, config,
            config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024));
    };

    factory.registerDiskType("hdfs", creator);
}


}
