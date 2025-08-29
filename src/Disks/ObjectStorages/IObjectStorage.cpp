#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/ObjectStorageKeyGenerator.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

[[noreturn]] static void throwGetMetadataStorageMetricsNotImplemented() {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method 'getMetadataStorageMetrics' is not implemented");
}

[[noreturn]] static void throwListObjectsNotSupported() {
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "listObjects() is not supported");
}

const MetadataStorageMetrics & IObjectStorage::getMetadataStorageMetrics() const
{
    throwGetMetadataStorageMetricsNotImplemented();
}

bool IObjectStorage::existsOrHasAnyChild(const std::string & path) const
{
    RelativePathsWithMetadata files;
    listObjects(path, files, 1);
    return !files.empty();
}

void IObjectStorage::listObjects(const std::string &, RelativePathsWithMetadata &, size_t) const
{
    throwListObjectsNotSupported();
}


ObjectStorageIteratorPtr IObjectStorage::iterate(const std::string & path_prefix, size_t max_keys) const
{
    RelativePathsWithMetadata files;
    listObjects(path_prefix, files, max_keys);

    return std::make_shared<ObjectStorageIteratorFromList>(std::move(files));
}

std::optional<ObjectMetadata> IObjectStorage::tryGetObjectMetadata(const std::string & path) const
{
    try
    {
        return getObjectMetadata(path);
    }
    catch (...)
    {
        return {};
    }
}

ThreadPool & IObjectStorage::getThreadPoolWriter()
{
    auto context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");

    return context->getThreadPoolWriter();
}

void IObjectStorage::copyObjectToAnotherObjectStorage( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    if (&object_storage_to == this)
        copyObject(object_from, object_to, read_settings, write_settings, object_to_attributes);

    auto in = readObject(object_from, read_settings);
    auto out = object_storage_to.writeObject(object_to, WriteMode::Rewrite, /* attributes= */ {}, /* buf_size= */ DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    copyData(*in, *out);
    out->finalize();
}

[[noreturn]] inline void throwGetCacheNameNotImplemented()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getCacheName is not implemented for object storage");
}

const std::string & IObjectStorage::getCacheName() const
{
    throwGetCacheNameNotImplemented();
}

ReadSettings IObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    return read_settings;
}

WriteSettings IObjectStorage::patchSettings(const WriteSettings & write_settings) const
{
    return write_settings;
}

std::string RelativePathWithMetadata::getPathOrPathToArchiveIfArchive() const
{
    if (isArchive())
        return getPathToArchive();
    return getPath();
}

[[noreturn]] inline void throwGenerateObjectKeyPrefixForDirectoryPathNotImplemented()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'generateObjectKeyPrefixForDirectoryPath' is not implemented");
}

ObjectStorageKey
IObjectStorage::generateObjectKeyPrefixForDirectoryPath(const std::string & /* path */, const std::optional<std::string> & /* key_prefix */) const
{
    throwGenerateObjectKeyPrefixForDirectoryPathNotImplemented();
}

#if USE_AZURE_BLOB_STORAGE
    [[noreturn]] void throwImplementedOnlyForAzureBlobStorage()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This function is only implemented for AzureBlobStorage");
    }

    std::shared_ptr<const AzureBlobStorage::ContainerClient> IObjectStorage::getAzureBlobStorageClient() const
    {
        throwImplementedOnlyForAzureBlobStorage();
    }

    AzureBlobStorage::AuthMethod IObjectStorage::getAzureBlobStorageAuthMethod() const
    {
        throwImplementedOnlyForAzureBlobStorage();
    }
#endif

#if USE_AWS_S3
    [[noreturn]] void throwImplementedOnlyForS3ObjectStorage()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This function is only implemented for S3ObjectStorage");
    }

    std::shared_ptr<const S3::Client> IObjectStorage::getS3StorageClient()
    {
        throwImplementedOnlyForS3ObjectStorage();
    }
#endif

}
