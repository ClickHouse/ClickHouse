#include <Disks/ObjectStorages/LocalObjectStorage.h>

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Common/FileCache.h>
#include <Common/FileCacheFactory.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace ErrorCodes
{
    extern const int CANNOT_UNLINK;
}

LocalObjectStorage::LocalObjectStorage()
    : log(&Poco::Logger::get("LocalObjectStorage"))
{
}

bool LocalObjectStorage::exists(const StoredObject & object) const
{
    return fs::exists(object.absolute_path);
}

std::unique_ptr<ReadBufferFromFileBase> LocalObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    if (objects.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LocalObjectStorage support read only from single object");

    return readObject(objects[0], read_settings, read_hint, file_size);
}

std::string LocalObjectStorage::getUniqueId(const std::string & path) const
{
    return toString(getINodeNumberFromPath(path));
}

std::unique_ptr<ReadBufferFromFileBase> LocalObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    const auto & path = object.absolute_path;

    if (!file_size)
        file_size = tryGetSizeFromFilePath(path);

    /// For now we cannot allow asynchronous reader from local filesystem when CachedObjectStorage is used.
    ReadSettings modified_settings{read_settings};
    switch (modified_settings.local_fs_method)
    {
        case LocalFSReadMethod::pread_threadpool:
        case LocalFSReadMethod::pread_fake_async:
        {
            modified_settings.local_fs_method = LocalFSReadMethod::pread;
            LOG_INFO(log, "Changing local filesystem read method to `pread`");
            break;
        }
        default:
        {
            break;
        }
    }

    LOG_TEST(log, "Read object: {}", path);
    return createReadBufferFromFileBase(path, modified_settings, read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase> LocalObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> /* attributes */,
    FinalizeCallback && /* finalize_callback */,
    size_t buf_size,
    const WriteSettings & /* write_settings */)
{
    const auto & path = object.absolute_path;
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    LOG_TEST(log, "Write object: {}", path);
    return std::make_unique<WriteBufferFromFile>(path, buf_size, flags);
}

void LocalObjectStorage::listPrefix(const std::string & path, RelativePathsWithSize & children) const
{
    fs::directory_iterator end_it;
    for (auto it = fs::directory_iterator(path); it != end_it; ++it)
        children.emplace_back(it->path().filename(), it->file_size());
}

void LocalObjectStorage::removeObject(const StoredObject & object)
{
    if (0 != unlink(object.absolute_path.data()))
        throwFromErrnoWithPath("Cannot unlink file " + object.absolute_path, object.absolute_path, ErrorCodes::CANNOT_UNLINK);
}

void LocalObjectStorage::removeObjects(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObject(object);
}

void LocalObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    if (exists(object))
        removeObject(object);
}

void LocalObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObjectIfExists(object);
}

ObjectMetadata LocalObjectStorage::getObjectMetadata(const std::string & /* path */) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Metadata is not supported for LocalObjectStorage");
}

void LocalObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from, const StoredObject & object_to, std::optional<ObjectAttributes> /* object_to_attributes */)
{
    fs::path to = object_to.absolute_path;
    fs::path from = object_from.absolute_path;

    /// Same logic as in DiskLocal.
    if (object_from.absolute_path.ends_with('/'))
        from = from.parent_path();
    if (fs::is_directory(from))
        to /= from.filename();

    fs::copy(from, to, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
}

void LocalObjectStorage::shutdown()
{
}

void LocalObjectStorage::startup()
{
}

std::unique_ptr<IObjectStorage> LocalObjectStorage::cloneObjectStorage(
    const std::string & /* new_namespace */,
    const Poco::Util::AbstractConfiguration & /* config */,
    const std::string & /* config_prefix */, ContextPtr /* context */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "cloneObjectStorage() is not implemented for LocalObjectStorage");
}

void LocalObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & /* config */, const std::string & /* config_prefix */, ContextPtr /* context */)
{
}

}
