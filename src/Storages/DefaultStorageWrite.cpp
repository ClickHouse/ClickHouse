#include <Storages/DefaultStorageWrite.h>
#include <Storages/IStorage.h>

namespace DB
{

DefaultStorageWrite::DefaultStorageWrite(
    StoragePtr storage_,
    ASTPtr query_,
    StorageMetadataPtr metadata_snapshot_,
    ContextPtr context_,
    bool async_insert_)
    : storage(std::move(storage_))
    , query(std::move(query_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , context(std::move(context_))
    , async_insert(async_insert_)
{
}

SinkToStoragePtr DefaultStorageWrite::createSink()
{
    return storage->write(query, metadata_snapshot, context, async_insert);
}

void DefaultStorageWrite::commit()
{
}

}
