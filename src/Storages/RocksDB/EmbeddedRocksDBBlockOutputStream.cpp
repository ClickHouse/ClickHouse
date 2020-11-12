#include <Storages/RocksDB/EmbeddedRocksDBBlockOutputStream.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <IO/WriteBufferFromString.h>

#include <rocksdb/db.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

EmbeddedRocksDBBlockOutputStream::EmbeddedRocksDBBlockOutputStream(
    StorageEmbeddedRocksDB & storage_,
    const StorageMetadataPtr & metadata_snapshot_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
{
    Block sample_block = metadata_snapshot->getSampleBlock();
    for (const auto & elem : sample_block)
    {
        if (elem.name == storage.primary_key)
            break;
        ++primary_key_pos;
    }
}

Block EmbeddedRocksDBBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

void EmbeddedRocksDBBlockOutputStream::write(const Block & block)
{
    metadata_snapshot->check(block, true);
    auto rows = block.rows();

    WriteBufferFromOwnString wb_key;
    WriteBufferFromOwnString wb_value;

    rocksdb::WriteBatch batch;
    rocksdb::Status status;
    for (size_t i = 0; i < rows; i++)
    {
        wb_key.restart();
        wb_value.restart();

        size_t idx = 0;
        for (const auto & elem : block)
        {
            elem.type->serializeBinary(*elem.column, i, idx == primary_key_pos ? wb_key : wb_value);
            ++idx;
        }
        status = batch.Put(wb_key.str(), wb_value.str());
        if (!status.ok())
            throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
    }

    status = storage.rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
}

}
