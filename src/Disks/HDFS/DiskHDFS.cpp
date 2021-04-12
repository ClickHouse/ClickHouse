#include <Disks/HDFS/DiskHDFS.h>

#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include "ReadIndirectBufferFromHDFS.h"
#include "WriteIndirectBufferFromHDFS.h"
#include "DiskHDFSReservation.h"
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
    extern const int FILE_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
}


DiskHDFS::DiskHDFS(
    const String & name_,
    const String & hdfs_name_,
    const String & metadata_path_,
    ContextPtr context_)
    : WithContext(context_)
    , log(&Poco::Logger::get("DiskHDFS"))
    , name(name_)
    , hdfs_name(hdfs_name_)
    , metadata_path(std::move(metadata_path_))
    , config(context_->getGlobalContext()->getConfigRef())
    , builder(createHDFSBuilder(hdfs_name, config))
    , fs(createHDFSFS(builder.get()))
{
}


bool DiskHDFS::exists(const String & path) const
{
    return Poco::File(metadata_path + path).exists();
}


bool DiskHDFS::isFile(const String & path) const
{
    return Poco::File(metadata_path + path).isFile();
}


bool DiskHDFS::isDirectory(const String & path) const
{
    return Poco::File(metadata_path + path).isDirectory();
}


size_t DiskHDFS::getFileSize(const String & path) const
{
    Metadata metadata(metadata_path, path);
    return metadata.total_size;
}


void DiskHDFS::createDirectory(const String & path)
{
    Poco::File(metadata_path + path).createDirectory();
}


void DiskHDFS::createDirectories(const String & path)
{
    Poco::File(metadata_path + path).createDirectories();
}


DiskDirectoryIteratorPtr DiskHDFS::iterateDirectory(const String & path)
{
    return std::make_unique<DiskHDFSDirectoryIterator>(metadata_path + path, path);
}


void DiskHDFS::clearDirectory(const String & path)
{
    for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
        if (isFile(it->path()))
            removeFile(it->path());
}


void DiskHDFS::moveFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);

    Poco::File(metadata_path + from_path).renameTo(metadata_path + to_path);
}


void DiskHDFS::replaceFile(const String & from_path, const String & to_path)
{
    Poco::File from_file(metadata_path + from_path);
    Poco::File to_file(metadata_path + to_path);
    if (to_file.exists())
    {
        Poco::File tmp_file(metadata_path + to_path + ".old");
        to_file.renameTo(tmp_file.path());
        from_file.renameTo(metadata_path + to_path);
        removeFile(to_path + ".old");
    }
    else
        from_file.renameTo(to_file.path());
}


std::unique_ptr<ReadBufferFromFileBase> DiskHDFS::readFile(const String & path, size_t buf_size, size_t, size_t, size_t, MMappedFileCache *) const
{
    Metadata metadata(metadata_path, path);

    LOG_DEBUG(log,
        "Read from file by path: {}. Existing HDFS objects: {}",
        backQuote(metadata_path + path), metadata.hdfs_objects.size());

    return std::make_unique<ReadIndirectBufferFromHDFS>(getContext(), hdfs_name, "", metadata, buf_size);
}


std::unique_ptr<WriteBufferFromFileBase> DiskHDFS::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    bool exist = exists(path);
    /// Path to store new HDFS object.
    auto file_name = getRandomName();
    auto hdfs_path = hdfs_name + file_name;
    if (!exist || mode == WriteMode::Rewrite)
    {
        /// If metadata file exists - remove and new.
        if (exist)
            removeFile(path);
        Metadata metadata(metadata_path, path, true);
        /// Save empty metadata to disk to have ability to get file size while buffer is not finalized.
        metadata.save();

        LOG_DEBUG(log,
            "Write to file by path: {}. New hdfs path: {}", backQuote(metadata_path + path), hdfs_path);

        return std::make_unique<WriteIndirectBufferFromHDFS>(getContext(), hdfs_path, file_name, metadata, buf_size);
    }
    else
    {
        Metadata metadata(metadata_path, path);

        LOG_DEBUG(log,
            "Append to file by path: {}. New hdfs path: {}. Existing HDFS objects: {}",
            backQuote(metadata_path + path), hdfs_path, metadata.hdfs_objects.size());

        return std::make_unique<WriteIndirectBufferFromHDFS>(getContext(), hdfs_path, file_name, metadata, buf_size);
    }
}


void DiskHDFS::removeFile(const String & path)
{
    LOG_DEBUG(&Poco::Logger::get("DiskHDFS"), "Remove file by path: {}", backQuote(metadata_path + path));

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        Metadata metadata(metadata_path, path);

        /// If there is no references - delete content from HDFS.
        if (metadata.ref_count == 0)
        {
            file.remove();
            for (const auto & [hdfs_object_path, _] : metadata.hdfs_objects)
            {
                const size_t begin_of_path = hdfs_name.find('/', hdfs_name.find("//") + 2);
                const std::string hdfs_path = hdfs_name.substr(begin_of_path) + hdfs_object_path;
                int res = hdfsDelete(fs.get(), hdfs_path.c_str(), 0);
                if (res == -1)
                    throw Exception("HDFSDelete failed with path: " + hdfs_path, 1);
            }
        }
        else /// In other case decrement number of references, save metadata and delete file.
        {
            --metadata.ref_count;
            metadata.save();
            file.remove();
        }
    }
    else
        file.remove();
}


void DiskHDFS::removeFileIfExists(const String & path)
{
    int exists_status = hdfsExists(fs.get(), path.data());
    if (exists_status == 0)
        removeFile(path);
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


void DiskHDFS::removeDirectory(const String & path)
{
    Poco::File(metadata_path + path).remove();
}


void DiskHDFS::listFiles(const String & path, std::vector<String> & file_names)
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        file_names.push_back(it->name());
}


void DiskHDFS::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    Poco::File(metadata_path + path).setLastModified(timestamp);
}


Poco::Timestamp DiskHDFS::getLastModified(const String & path)
{
    return Poco::File(metadata_path + path).getLastModified();
}


void DiskHDFS::createHardLink(const String & src_path, const String & dst_path)
{
    /// Increment number of references.
    Metadata src(metadata_path, src_path);
    ++src.ref_count;
    src.save();

    /// Create FS hardlink to metadata file.
    DB::createHardLink(metadata_path + src_path, metadata_path + dst_path);
}


void DiskHDFS::createFile(const String & path)
{
    /// Create empty metadata file.
    Metadata metadata(metadata_path, path, true);
    metadata.save();
}


void DiskHDFS::setReadOnly(const String & path)
{
    Poco::File(metadata_path + path).setReadOnly(true);
}


ReservationPtr DiskHDFS::reserve(UInt64 bytes)
{
    if (!tryReserve(bytes))
        return {};
    return std::make_unique<DiskHDFSReservation>(std::static_pointer_cast<DiskHDFS>(shared_from_this()), bytes);
}


bool DiskHDFS::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(reservation_mutex);

    if (bytes == 0)
    {
        LOG_DEBUG(log, "Reserving 0 bytes on HDFS disk {}", backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_DEBUG(log,
            "Reserving {} on disk {}, having unreserved {}",
            formatReadableSizeWithBinarySuffix(bytes), backQuote(name), formatReadableSizeWithBinarySuffix(unreserved_space));
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }

    return false;
}


void registerDiskHDFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextConstPtr context) -> DiskPtr
    {
        Poco::File disk{context->getPath() + "disks/" + name};
        disk.createDirectories();

        DB::String uri{config.getString(config_prefix + ".endpoint")};

        if (uri.back() != '/')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDFS path must ends with '/', but '{}' doesn't.", uri);

        String metadata_path = context->getPath() + "disks/" + name + "/";
        auto copy_context = Context::createCopy(context);
        return std::make_shared<DiskHDFS>(name, uri, metadata_path, copy_context);
    };

    factory.registerDiskType("hdfs", creator);
}


//void DiskHDFS::copyFile(const String & from_path, const String & to_path)
//{
//    if (exists(to_path))
//        remove(to_path);
//
//    Metadata from(metadata_path, from_path);
//    Metadata to(metadata_path, to_path, true);
//
//    for (const auto & [path, size] : from.hdfs_objects)
//    {
//        auto new_path = hdfs_name + getRandomName();
//        /// TODO:: hdfs copy semantics
//        /*
//        Aws::HDFS::Model::CopyObjectRequest req;
//        req.SetCopySource(bucket + "/" + path);
//        req.SetBucket(bucket);
//        req.SetKey(new_path);
//        throwIfError(client->CopyObject(req));
//        */
//        throw Exception("is not implemented yet", 1);
//        to.addObject(new_path, size);
//    }
//
//    to.save();
//}

}
