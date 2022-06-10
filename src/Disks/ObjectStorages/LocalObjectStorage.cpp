#include <Disks/ObjectStorages/LocalObjectStorage.h>

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Common/IFileCache.h>
#include <Common/FileCacheFactory.h>
#include <Common/filesystemHelpers.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int Local_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace ErrorCodes
{
    extern const int CANNOT_UNLINK;
}


bool LocalObjectStorage::exists(const std::string & path) const
{
    return fs::exists(path);
}

std::unique_ptr<ReadBufferFromFileBase> LocalObjectStorage::readObjects( /// NOLINT
    const std::string & common_path_prefix,
    const BlobsPathToSize & blobs_to_read,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    if (blobs_to_read.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LocalObjectStorage support read only from single object");

    std::string path = fs::path(common_path_prefix) / blobs_to_read[0].relative_path;
    return readObject(path, read_settings, read_hint, file_size);
}

std::unique_ptr<ReadBufferFromFileBase> LocalObjectStorage::readObject( /// NOLINT
    const std::string & path,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    if (!file_size.has_value())
        file_size = getFileSizeIfPossible(path);

    return createReadBufferFromFileBase(path, read_settings, read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase> LocalObjectStorage::writeObject( /// NOLINT
    const std::string & path,
    WriteMode mode, // Local doesn't support append, only rewrite
    std::optional<ObjectAttributes> /* atributes */,
    FinalizeCallback && /* finalize_callback */,
    size_t buf_size,
    const WriteSettings & /* write_settings */)
{
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    return std::make_unique<WriteBufferFromFile>(path, buf_size, flags);
}

void LocalObjectStorage::listPrefix(const std::string & path, BlobsPathToSize & children) const
{
    fs::directory_iterator end_it;
    for (auto it = fs::directory_iterator(path); it != end_it; ++it)
        children.emplace_back(it->path().filename(), it->file_size());
}

void LocalObjectStorage::removeObject(const std::string & path)
{
    auto fs_path = fs::path(path) / path;
    if (0 != unlink(fs_path.c_str()))
        throwFromErrnoWithPath("Cannot unlink file " + fs_path.string(), fs_path, ErrorCodes::CANNOT_UNLINK);
}

void LocalObjectStorage::removeObjects(const std::vector<std::string> & paths)
{
    for (const auto & path : paths)
        removeObject(path);
}

void LocalObjectStorage::removeObjectIfExists(const std::string & path)
{
    if (exists(path))
        removeObject(path);
}

void LocalObjectStorage::removeObjectsIfExist(const std::vector<std::string> & paths)
{
    for (const auto & path : paths)
        removeObjectIfExists(path);
}

ObjectMetadata LocalObjectStorage::getObjectMetadata(const std::string & /* path */) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Metadata is not supported for LocalObjectStorage");
}

String LocalObjectStorage::getUniqueIdForBlob(const String & path)
{
    return toString(getINodeNumberFromPath(path));
}

void LocalObjectStorage::copyObjectToAnotherObjectStorage( // NOLINT
    const std::string & /* object_from */,
    const std::string & /* object_to */,
    IObjectStorage & /* object_storage_to */,
    std::optional<ObjectAttributes> /* object_to_attributes */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "cloneObjectStorage() is not implemented for LocalObjectStorage");
}

void LocalObjectStorage::copyObject( // NOLINT
    const std::string & object_from, const std::string & object_to, std::optional<ObjectAttributes> /* object_to_attributes */)
{
    fs::path to = object_to;
    fs::path from = object_from;

    if (object_from.ends_with('/'))
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
