#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/Rocksdb/StorageEmbeddedRocksdb.h>
#include <Storages/Rocksdb/EmbeddedRocksdbBlockInputStream.h>

#include <ext/enumerate.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

EmbeddedRocksdbBlockInputStream::EmbeddedRocksdbBlockInputStream(
            StorageEmbeddedRocksdb & storage_,
            const StorageMetadataPtr & metadata_snapshot_,
            size_t max_block_size_)
            : storage(storage_)
            , metadata_snapshot(metadata_snapshot_)
            , max_block_size(max_block_size_)
{
    sample_block = metadata_snapshot->getSampleBlock();
    primary_key_pos = sample_block.getPositionByName(storage.primary_key);
}

Block EmbeddedRocksdbBlockInputStream::readImpl()
{
    if (finished)
        return {};

    if (!iterator)
    {
        iterator = std::unique_ptr<rocksdb::Iterator>(storage.rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
        iterator->SeekToFirst();
    }

    MutableColumns columns = sample_block.cloneEmptyColumns();
    size_t rows = 0;
    for (; iterator->Valid(); iterator->Next())
    {
        ReadBufferFromString key_buffer(iterator->key());
        ReadBufferFromString value_buffer(iterator->value());

        for (const auto [idx, column_type] : ext::enumerate(sample_block.getColumnsWithTypeAndName()))
        {
            column_type.type->deserializeBinary(*columns[idx], idx == primary_key_pos? key_buffer: value_buffer);
        }
        ++rows;
        if (rows >= max_block_size)
            break;
    }

    finished = !iterator->Valid();
    if (!iterator->status().ok())
    {
        throw Exception("Engine " + getName() + " got error while seeking key value datas: " + iterator->status().ToString(),
            ErrorCodes::LOGICAL_ERROR);
    }
    return sample_block.cloneWithColumns(std::move(columns));
}

}
