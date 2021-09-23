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
            const Block & header,
            size_t max_block_size_)
            : SourceWithProgress(header)
            , storage(storage_)
            , max_block_size(max_block_size_)
{
    primary_key_pos = header.getPositionByName(storage.primary_key);
}

Chunk EmbeddedRocksDBBlockInputStream::generate()
{
    if (!iterator)
    {
        iterator = std::unique_ptr<rocksdb::Iterator>(storage.rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
        iterator->SeekToFirst();
    }

    if (!iterator->Valid())
        return {};

    const auto & sample_block = getPort().getHeader();
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

    if (!iterator->status().ok())
    {
        throw Exception("Engine " + getName() + " got error while seeking key value data: " + iterator->status().ToString(),
            ErrorCodes::ROCKSDB_ERROR);
    }
    Block block = sample_block.cloneWithColumns(std::move(columns));
    return Chunk(block.getColumns(), block.rows());
}

}
