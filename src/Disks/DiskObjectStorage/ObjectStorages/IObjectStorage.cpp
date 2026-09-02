#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IOSchedulingSettings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadPipeline.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <IO/WriteBufferFromString.h>


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

void IObjectStorage::listObjects(const std::string &, RelativePathsWithMetadata &, size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "listObjects() is not supported");
}

/// Read single object
SmallObjectDataWithMetadata IObjectStorage::readSmallObjectAndGetObjectMetadata( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    size_t max_size_bytes,
    std::optional<size_t> read_hint) const
{
    auto buffer = readObject(object, read_settings, read_hint);
    SmallObjectDataWithMetadata result;
    WriteBufferFromString out(result.data);
    copyDataMaxBytes(*buffer, out, max_size_bytes);
    out.finalize();

    /// By default no metadata available, derived classes may override this method

    return result;
}

ObjectStorageIteratorPtr IObjectStorage::iterate(
    const std::string & path_prefix,
    size_t max_keys,
    bool,
    const std::optional<std::string> &) const
{
    RelativePathsWithMetadata files;
    listObjects(path_prefix, files, max_keys);

    return std::make_shared<ObjectStorageIteratorFromList>(std::move(files));
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

const std::string & IObjectStorage::getCacheName() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getCacheName is not implemented for object storage");
}

void IObjectStorage::setIOSchedulingResourceNames(const String & read_resource_name_, const String & write_resource_name_)
{
    std::lock_guard lock(io_scheduling_mutex);
    read_resource_name = read_resource_name_;
    write_resource_name = write_resource_name_;
}

std::pair<String, String> IObjectStorage::getIOSchedulingResourceNames() const
{
    std::lock_guard guard(io_scheduling_mutex);
    return {read_resource_name, write_resource_name};
}

ReadSettings IObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    const auto [read_resource, write_resource] = getIOSchedulingResourceNames();
    return updateIOSchedulingSettings(read_settings, read_resource, write_resource);
}

WriteSettings IObjectStorage::patchSettings(const WriteSettings & write_settings) const
{
    const auto [read_resource, write_resource] = getIOSchedulingResourceNames();
    return updateIOSchedulingSettings(write_settings, read_resource, write_resource);
}

void IObjectStorage::prepareRead(
    ObjectStoragePtr storage,
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    ReadPipeline & pipeline) const
{
    pipeline.setSource(std::move(storage), objects, patchSettings(read_settings), read_hint);
}

}
