#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Common/getRandomASCIIString.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

bool IObjectStorage::existsOrHasAnyChild(const std::string & path) const
{
    RelativePathsWithMetadata files;
    listObjects(path, files, 1);
    return !files.empty();
}

void IObjectStorage::listObjects(const std::string &, RelativePathsWithMetadata &, int) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "listObjects() is not supported");
}


ObjectStorageIteratorPtr IObjectStorage::iterate(const std::string & path_prefix) const
{
    RelativePathsWithMetadata files;
    listObjects(path_prefix, files, 0);

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
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    if (&object_storage_to == this)
        copyObject(object_from, object_to, object_to_attributes);

    auto in = readObject(object_from);
    auto out = object_storage_to.writeObject(object_to, WriteMode::Rewrite);
    copyData(*in, *out);
    out->finalize();
}

const std::string & IObjectStorage::getCacheName() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getCacheName() is not implemented for object storage");
}

ReadSettings IObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    ReadSettings settings{read_settings};
    settings.for_object_storage = true;
    return settings;
}

WriteSettings IObjectStorage::patchSettings(const WriteSettings & write_settings) const
{
    WriteSettings settings{write_settings};
    settings.for_object_storage = true;
    return settings;
}

std::string IObjectStorage::generateBlobNameForPath(const std::string & /* path */)
{
    /// Path to store the new S3 object.

    /// Total length is 32 a-z characters for enough randomness.
    /// First 3 characters are used as a prefix for
    /// https://aws.amazon.com/premiumsupport/knowledge-center/s3-object-key-naming-pattern/

    constexpr size_t key_name_total_size = 32;
    constexpr size_t key_name_prefix_size = 3;

    /// Path to store new S3 object.
    return fmt::format("{}/{}",
        getRandomASCIIString(key_name_prefix_size),
        getRandomASCIIString(key_name_total_size - key_name_prefix_size));
}

}
