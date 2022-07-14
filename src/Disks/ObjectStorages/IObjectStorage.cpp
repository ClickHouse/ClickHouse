#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

AsynchronousReaderPtr IObjectStorage::getThreadPoolReader()
{
    constexpr size_t pool_size = 50;
    constexpr size_t queue_size = 1000000;
    static AsynchronousReaderPtr reader = std::make_shared<ThreadPoolRemoteFSReader>(pool_size, queue_size);
    return reader;
}

ThreadPool & IObjectStorage::getThreadPoolWriter()
{
    constexpr size_t pool_size = 100;
    constexpr size_t queue_size = 1000000;
    static ThreadPool writer(pool_size, pool_size, queue_size);
    return writer;
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

std::string IObjectStorage::getCacheBasePath() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getCacheBasePath() is not implemented for {}", getName());
}

}
