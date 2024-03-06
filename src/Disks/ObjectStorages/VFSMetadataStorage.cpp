#include "VFSMetadataStorage.h"
#include <Disks/IDisk.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/S3Common.h>
#include <Common/logger_useful.h>
#include "IMetadataStorage.h"
#if USE_AZURE_BLOB_STORAGE
#    include <azure/storage/common/storage_exception.hpp>
#endif

namespace DB
{
VFSMetadataStorage::VFSMetadataStorage(IDisk & disk_, std::string_view object_key_prefix_)
    : disk(disk_), prefix(object_key_prefix_), logger(getLogger(fmt::format("Metadata({})", disk.getName())))
{
}

bool VFSMetadataStorage::tryDownloadMetadata(std::string_view from_remote_file, const String & to_folder)
{
    auto buf = disk.getObjectStorage()->readObject(getMetadataObject(from_remote_file));
    auto tx = disk.getMetadataStorage()->createTransaction();
    tx->createDirectoryRecursive(to_folder);

    try
    {
        buf->eof();
    }
    // TODO myrrc this works only for s3 and azure
#if USE_AWS_S3
    catch (const S3Exception & e)
    {
        if (e.getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY)
            return false;
        throw;
    }
#endif
#if USE_AZURE_BLOB_STORAGE
    catch (const Azure::Storage::StorageException & e)
    {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
            return false;
        throw;
    }
#endif
    catch (...)
    {
        throw;
    }

    String str;
    while (!buf->eof())
    {
        str.clear();
        readNullTerminated(str, *buf);
        const fs::path full_path = fs::path(to_folder) / str;
        LOG_TRACE(logger, "Downloading {} to {}", str, full_path);
        str.clear();
        readNullTerminated(str, *buf);
        tx->writeStringToFile(full_path, str);
    }
    tx->commit();
    return true;
}

void VFSMetadataStorage::uploadMetadata(std::string_view to_remote_file, const String & from_folder)
{
    const StoredObject to_object = getMetadataObject(to_remote_file);
    LOG_DEBUG(logger, "Uploading to {}", to_object);

    Strings files; // we need recursion for projections
    for (const fs::directory_entry & entry : fs::recursive_directory_iterator(from_folder))
        if (entry.is_regular_file())
            files.emplace_back(entry.path());

    auto buf = disk.getObjectStorage()->writeObject(to_object, WriteMode::Rewrite);
    for (auto & [path, metadata] : disk.getSerializedMetadata(files))
    {
        const String relative_path = fs::path(path).lexically_relative(from_folder);
        LOG_TRACE(logger, "Uploading {}", relative_path);
        writeNullTerminatedString(relative_path, *buf);
        writeNullTerminatedString(metadata, *buf);
    }
    buf->finalize();
}

StoredObject VFSMetadataStorage::getMetadataObject(std::string_view remote) const
{
    // We must include disk name as two disks with different names might use same object storage bucket
    // vfs/ prefix is required for AWS:
    //  https://aws.amazon.com/premiumsupport/knowledge-center/s3-object-key-naming-pattern/
    // TODO myrrc replace name with vfs_disk_id
    String key = fmt::format("vfs/{}/{}", disk.getName(), remote);
    return StoredObject{ObjectStorageKey::createAsRelative(prefix, std::move(key)).serialize()};
}
}
