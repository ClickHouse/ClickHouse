#include <Storages/Distributed/DistributedBlockOutputStream.h>
#include <Storages/StorageDistributed.h>

#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/createBlockSelector.h>

#include <DataTypes/DataTypesNumber.h>
#include <Common/ClickHouseRevision.h>

#include <Poco/DirectoryIterator.h>

#include <memory>
#include <iostream>


namespace DB
{

DistributedBlockOutputStream::DistributedBlockOutputStream(StorageDistributed & storage, const ASTPtr & query_ast, const ClusterPtr & cluster_)
    : storage(storage), query_ast(query_ast), cluster(cluster_)
{
}


void DistributedBlockOutputStream::write(const Block & block)
{
    if (storage.getShardingKeyExpr() && (cluster->getShardsInfo().size() > 1))
        return writeSplit(block);

    writeImpl(block);
}


IColumn::Selector DistributedBlockOutputStream::createSelector(Block block)
{
    storage.getShardingKeyExpr()->execute(block);
    const auto & key_column = block.getByName(storage.getShardingKeyColumnName());
    size_t num_shards = cluster->getShardsInfo().size();
    const auto & slot_to_shard = cluster->getSlotToShard();

#define CREATE_FOR_TYPE(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(key_column.type.get())) \
        return createBlockSelector<TYPE>(*key_column.column, num_shards, slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{"Sharding key expression does not evaluate to an integer type", ErrorCodes::TYPE_MISMATCH};
}


void DistributedBlockOutputStream::writeSplit(const Block & block)
{
    const auto num_cols = block.columns();
    /// cache column pointers for later reuse
    std::vector<const IColumn *> columns(num_cols);
    for (size_t i = 0; i < columns.size(); ++i)
        columns[i] = block.safeGetByPosition(i).column.get();

    auto selector = createSelector(block);

    /// Split block to num_shard smaller block, using 'selector'.

    const size_t num_shards = cluster->getShardsInfo().size();
    Blocks splitted_blocks(num_shards);

    for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
        splitted_blocks[shard_idx] = block.cloneEmpty();

    size_t columns_in_block = block.columns();
    for (size_t col_idx_in_block = 0; col_idx_in_block < columns_in_block; ++col_idx_in_block)
    {
        Columns splitted_columns = block.getByPosition(col_idx_in_block).column->scatter(num_shards, selector);
        for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
            splitted_blocks[shard_idx].getByPosition(col_idx_in_block).column = std::move(splitted_columns[shard_idx]);
    }

    for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
        if (splitted_blocks[shard_idx].rows())
            writeImpl(splitted_blocks[shard_idx], shard_idx);
}


void DistributedBlockOutputStream::writeImpl(const Block & block, const size_t shard_id)
{
    const auto & shard_info = cluster->getShardsInfo()[shard_id];
    if (shard_info.getLocalNodeCount() > 0)
        writeToLocal(block, shard_info.getLocalNodeCount());

    /// dir_names is empty if shard has only local addresses
    if (!shard_info.dir_names.empty())
        writeToShard(block, shard_info.dir_names);
}


void DistributedBlockOutputStream::writeToLocal(const Block & block, const size_t repeats)
{
    InterpreterInsertQuery interp{query_ast, storage.context};

    auto block_io = interp.execute();
    block_io.out->writePrefix();

    for (size_t i = 0; i < repeats; ++i)
        block_io.out->write(block);

    block_io.out->writeSuffix();
}


void DistributedBlockOutputStream::writeToShard(const Block & block, const std::vector<std::string> & dir_names)
{
    /** tmp directory is used to ensure atomicity of transactions
      *  and keep monitor thread out from reading incomplete data
      */
    std::string first_file_tmp_path{};

    auto first = true;
    const auto & query_string = queryToString(query_ast);

    /// write first file, hardlink the others
    for (const auto & dir_name : dir_names)
    {
        const auto & path = storage.getPath() + dir_name + '/';

        /// ensure shard subdirectory creation and notify storage
        if (Poco::File(path).createDirectory())
            storage.requireDirectoryMonitor(dir_name);

        const auto & file_name = toString(storage.file_names_increment.get()) + ".bin";
        const auto & block_file_path = path + file_name;

        /** on first iteration write block to a temporary directory for subsequent hardlinking to ensure
            *  the inode is not freed until we're done */
        if (first)
        {
            first = false;

            const auto & tmp_path = path + "tmp/";
            Poco::File(tmp_path).createDirectory();
            const auto & block_file_tmp_path = tmp_path + file_name;

            first_file_tmp_path = block_file_tmp_path;

            WriteBufferFromFile out{block_file_tmp_path};
            CompressedWriteBuffer compress{out};
            NativeBlockOutputStream stream{compress, ClickHouseRevision::get()};

            writeStringBinary(query_string, out);

            stream.writePrefix();
            stream.write(block);
            stream.writeSuffix();
        }

        if (link(first_file_tmp_path.data(), block_file_path.data()))
            throwFromErrno("Could not link " + block_file_path + " to " + first_file_tmp_path);
    }

    /** remove the temporary file, enabling the OS to reclaim inode after all threads
        *  have removed their corresponding files */
    Poco::File(first_file_tmp_path).remove();
}

}
