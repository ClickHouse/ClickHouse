#pragma once

#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTAlterQuery.h>


namespace DB
{

/** Allows you add or remove a column in the table.
  * It also allows you to manipulate the partitions of the MergeTree family tables.
  */
class InterpreterAlterQuery : public IInterpreter
{
public:
    InterpreterAlterQuery(const ASTPtr & query_ptr_, const Context & context_);

    BlockIO execute() override;

private:
    struct PartitionCommand
    {
        enum Type
        {
            DROP_PARTITION,
            ATTACH_PARTITION,
            FETCH_PARTITION,
            FREEZE_PARTITION,
            RESHARD_PARTITION,
            CLEAR_COLUMN,
        };

        Type type;

        Field partition;
        Field column_name;
        bool detach = false; /// true for DETACH PARTITION.

        bool part = false;

        String from; /// For FETCH PARTITION - path in ZK to the shard, from which to download the partition.

        /// For RESHARD PARTITION.
        WeightedZooKeeperPaths weighted_zookeeper_paths;
        ASTPtr sharding_key_expr;
        bool do_copy = false;
        Field coordinator;

        /// For FREEZE PARTITION
        String with_name;

        static PartitionCommand dropPartition(const Field & partition, bool detach)
        {
            PartitionCommand res;
            res.type = DROP_PARTITION;
            res.partition = partition;
            res.detach = detach;
            return res;
        }

        static PartitionCommand clearColumn(const Field & partition, const Field & column_name)
        {
            PartitionCommand res;
            res.type = CLEAR_COLUMN;
            res.partition = partition;
            res.column_name = column_name;
            return res;
        }

        static PartitionCommand attachPartition(const Field & partition, bool part)
        {
            PartitionCommand res;
            res.type = ATTACH_PARTITION;
            res.partition = partition;
            res.part = part;
            return res;
        }

        static PartitionCommand fetchPartition(const Field & partition, const String & from)
        {
            PartitionCommand res;
            res.type = FETCH_PARTITION;
            res.partition = partition;
            res.from = from;
            return res;
        }

        static PartitionCommand freezePartition(const Field & partition, const String & with_name)
        {
            PartitionCommand res;
            res.type = FREEZE_PARTITION;
            res.partition = partition;
            res.with_name = with_name;
            return res;
        }

        static PartitionCommand reshardPartitions(const Field & partition_,
            const WeightedZooKeeperPaths & weighted_zookeeper_paths_, const ASTPtr & sharding_key_expr_,
            bool do_copy_, const Field & coordinator_)
        {
            PartitionCommand res;
            res.type = RESHARD_PARTITION;
            res.partition = partition_;
            res.weighted_zookeeper_paths = weighted_zookeeper_paths_;
            res.sharding_key_expr = sharding_key_expr_;
            res.do_copy = do_copy_;
            res.coordinator = coordinator_;
            return res;
        }
    };

    class PartitionCommands : public std::vector<PartitionCommand>
    {
    public:
        void validate(const IStorage * table);
    };

    ASTPtr query_ptr;

    Context context;

    static void parseAlter(const ASTAlterQuery::ParameterContainer & params,
        AlterCommands & out_alter_commands, PartitionCommands & out_partition_commands);
};

}
