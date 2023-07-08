#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

void IObjectStorage::findAllFiles(const std::string &, RelativePathsWithSize &, int) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "findAllFiles() is not supported");
}
void IObjectStorage::getDirectoryContents(const std::string &,
    RelativePathsWithSize &,
    std::vector<std::string> &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getDirectoryContents() is not supported");
}

IAsynchronousReader & IObjectStorage::getThreadPoolReader()
{
    auto context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");

    return context->getThreadPoolReader(Context::FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
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

void IObjectStorage::applyRemoteThrottlingSettings(ContextPtr context)
{
    std::unique_lock lock{throttlers_mutex};
    remote_read_throttler = context->getRemoteReadThrottler();
    remote_write_throttler = context->getRemoteWriteThrottler();
}

ReadSettings IObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    std::unique_lock lock{throttlers_mutex};
    ReadSettings settings{read_settings};
    settings.remote_throttler = remote_read_throttler;
    settings.for_object_storage = true;
    return settings;
}

WriteSettings IObjectStorage::patchSettings(const WriteSettings & write_settings) const
{
    std::unique_lock lock{throttlers_mutex};
    WriteSettings settings{write_settings};
    settings.remote_throttler = remote_write_throttler;
    settings.for_object_storage = true;
    return settings;
}

}
