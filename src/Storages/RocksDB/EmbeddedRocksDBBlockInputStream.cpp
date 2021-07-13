#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/RocksDB/EmbeddedRocksDBBlockInputStream.h>

#include <rocksdb/db.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

EmbeddedRocksDBBlockInputStream::EmbeddedRocksDBBlockInputStream(
            StorageEmbeddedRocksDB & storage_,
            const StorageMetadataPtr & metadata_snapshot_,
            size_t max_block_size_)
            : storage(storage_)
            , metadata_snapshot(metadata_snapshot_)
            , max_block_size(max_block_size_)
{
    sample_block = metadata_snapshot->getSampleBlock();
    primary_key_pos = sample_block.getPositionByName(storage.primary_key);
}

Block EmbeddedRocksDBBlockInputStream::readImpl()
{
    if (finished)
        return {};

    if (!iterator)
    {
        iterator = std::unique_ptr<rocksdb::Iterator>(storage.rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
        iterator->SeekToFirst();
    }

    MutableColumns columns = sample_block.cloneEmptyColumns();

    for (size_t rows = 0; iterator->Valid() && rows < max_block_size; ++rows, iterator->Next())
    {
        ReadBufferFromString key_buffer(iterator->key());
        ReadBufferFromString value_buffer(iterator->value());

        size_t idx = 0;
        for (const auto & elem : sample_block)
        {
            auto serialization = elem.type->getDefaultSerialization();
            serialization->deserializeBinary(*columns[idx], idx == primary_key_pos ? key_buffer : value_buffer);
            ++idx;
        }
    }

    finished = !iterator->Valid();
    if (!iterator->status().ok())
    {
        throw Exception("Engine " + getName() + " got error while seeking key value data: " + iterator->status().ToString(),
            ErrorCodes::ROCKSDB_ERROR);
    }
    return sample_block.cloneWithColumns(std::move(columns));
}

}
