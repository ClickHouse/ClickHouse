#pragma once

#include <Parsers/formatAST.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Core/Block.h>
#include <Interpreters/Cluster.h>

namespace DB
{

class StorageDistributed;

/** The write is asynchronous - the data is first written to the local file system, and then sent to the remote servers.
 *  If the Distributed table uses more than one shard, then in order to support the write,
 *  when creating the table, an additional parameter must be specified for ENGINE - the sharding key.
 *  Sharding key is an arbitrary expression from the columns. For example, rand() or UserID.
 *  When writing, the data block is splitted by the remainder of the division of the sharding key by the total weight of the shards,
 *  and the resulting blocks are written in a compressed Native format in separate directories for sending.
 *  For each destination address (each directory with data to send), a separate thread is created in StorageDistributed,
 *  which monitors the directory and sends data. */
class DistributedBlockOutputStream : public IBlockOutputStream
{
public:
    DistributedBlockOutputStream(StorageDistributed & storage, const ASTPtr & query_ast, const ClusterPtr & cluster_);

    void write(const Block & block) override;

private:
    IColumn::Selector createSelector(Block block);

    void writeSplit(const Block & block);

    void writeImpl(const Block & block, const size_t shard_id = 0);

    void writeToLocal(const Block & block, const size_t repeats);

    void writeToShard(const Block & block, const std::vector<std::string> & dir_names);

private:
    StorageDistributed & storage;
    ASTPtr query_ast;
    ClusterPtr cluster;
};

}
