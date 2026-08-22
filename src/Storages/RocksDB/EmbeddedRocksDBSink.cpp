#include <IO/WriteBufferFromString.h>
#include <Storages/RocksDB/EmbeddedRocksDBSink.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>

#include <rocksdb/utilities/db_ttl.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ROCKSDB_ERROR;
}

EmbeddedRocksDBSink::EmbeddedRocksDBSink(StorageEmbeddedRocksDB & storage_, const StorageMetadataPtr & metadata_snapshot_)
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
{
    serializations = getHeader().getSerializations();
}

void EmbeddedRocksDBSink::consume(Chunk & chunk)
{
    auto rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();
    const auto & primary_key_pos = storage.getPrimaryKeyPos();
    const auto & value_column_pos = storage.getValueColumnPos();

    WriteBufferFromOwnString wb_key;
    WriteBufferFromOwnString wb_value;

    rocksdb::WriteBatch batch;
    rocksdb::Status status;
    for (size_t i = 0; i < rows; ++i)
    {
        wb_key.restart();
        wb_value.restart();

        for (const auto idx : primary_key_pos)
            serializations[idx]->serializeBinary(*columns[idx], i, wb_key, {});

        for (const auto idx : value_column_pos)
            serializations[idx]->serializeBinary(*columns[idx], i, wb_value, {});

        status = batch.Put(wb_key.str(), wb_value.str());
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
    }

    status = storage.rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
}

}
