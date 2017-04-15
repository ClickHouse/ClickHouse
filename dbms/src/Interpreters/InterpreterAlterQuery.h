#pragma once

#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTAlterQuery.h>


namespace DB
{

/** Позволяет добавить или удалить столбец в таблице.
  * Также позволяет осуществить манипуляции с партициями таблиц семейства MergeTree.
  */
class InterpreterAlterQuery : public IInterpreter
{
public:
    InterpreterAlterQuery(ASTPtr query_ptr_, const Context & context_);

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
            DROP_COLUMN,
        };

        Type type;

        Field partition;
        Field column_name;
        bool detach = false; /// true для DETACH PARTITION.

        bool unreplicated = false;
        bool part = false;

        String from; /// Для FETCH PARTITION - путь в ZK к шарду, с которого скачивать партицию.

        /// Для RESHARD PARTITION.
        Field last_partition;
        WeightedZooKeeperPaths weighted_zookeeper_paths;
        ASTPtr sharding_key_expr;
        bool do_copy = false;
        Field coordinator;

        /// For FREEZE PARTITION
        String with_name;

        static PartitionCommand dropPartition(const Field & partition, bool detach, bool unreplicated)
        {
            PartitionCommand res;
            res.type = DROP_PARTITION;
            res.partition = partition;
            res.detach = detach;
            res.unreplicated = unreplicated;
            return res;
        }

        static PartitionCommand dropColumnFromPartition(const Field & partition, const Field & column_name)
        {
            PartitionCommand res;
            res.type = DROP_COLUMN;
            res.partition = partition;
            res.column_name = column_name;
            return res;
        }

        static PartitionCommand attachPartition(const Field & partition, bool unreplicated, bool part)
        {
            PartitionCommand res;
            res.type = ATTACH_PARTITION;
            res.partition = partition;
            res.unreplicated = unreplicated;
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

        static PartitionCommand reshardPartitions(const Field & first_partition_, const Field & last_partition_,
            const WeightedZooKeeperPaths & weighted_zookeeper_paths_, const ASTPtr & sharding_key_expr_,
            bool do_copy_, const Field & coordinator_)
        {
            PartitionCommand res;
            res.type = RESHARD_PARTITION;
            res.partition = first_partition_;
            res.last_partition = last_partition_;
            res.weighted_zookeeper_paths = weighted_zookeeper_paths_;
            res.sharding_key_expr = sharding_key_expr_;
            res.do_copy = do_copy_;
            res.coordinator = coordinator_;
            return res;
        }
    };

    using PartitionCommands = std::vector<PartitionCommand>;

    ASTPtr query_ptr;

    Context context;

    static void parseAlter(const ASTAlterQuery::ParameterContainer & params,
        AlterCommands & out_alter_commands, PartitionCommands & out_partition_commands);
};

}
