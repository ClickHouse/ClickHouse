#pragma once

#include <Storages/IStorageWrite.h>
#include <Storages/IStorage_fwd.h>

#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class DefaultStorageWrite final : public IStorageWrite
{
public:
    DefaultStorageWrite(
        StoragePtr storage_,
        ASTPtr query_,
        StorageMetadataPtr metadata_snapshot_,
        ContextPtr context_,
        bool async_insert_);

    SinkToStoragePtr createSink() override;
    void commit() override;

private:
    StoragePtr storage;
    ASTPtr query;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    bool async_insert;
};

}
