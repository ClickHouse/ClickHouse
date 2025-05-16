#include <Storages/RocksDB/EmbeddedRocksDBSink.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <IO/WriteBufferFromString.h>

#include <rocksdb/utilities/db_ttl.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

EmbeddedRocksDBSink::EmbeddedRocksDBSink(
    StorageEmbeddedRocksDB & storage_,
    const StorageMetadataPtr & metadata_snapshot_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
{
    for (const auto & elem : getHeader())
    {
        if (elem.name == storage.primary_key)
            break;
        ++primary_key_pos;
    }
    serializations = getHeader().getSerializations();
}

void EmbeddedRocksDBSink::consume(Chunk & chunk)
{
    auto rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    WriteBufferFromOwnString wb_key;
    WriteBufferFromOwnString wb_value;

    rocksdb::WriteBatch batch;
    rocksdb::Status status;
    for (size_t i = 0; i < rows; ++i)
    {
        wb_key.restart();
        wb_value.restart();

        for (size_t idx = 0; idx < columns.size(); ++idx)
            serializations[idx]->serializeBinary(*columns[idx], i, idx == primary_key_pos ? wb_key : wb_value, {});

        status = batch.Put(wb_key.str(), wb_value.str());
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
    }

    status = storage.rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
}

}
