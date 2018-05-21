#pragma once

#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
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
            REPLACE_PARTITION,
            FETCH_PARTITION,
            FREEZE_PARTITION,
            CLEAR_COLUMN,
        };

        Type type;

        ASTPtr partition;
        Field column_name;

        /// true for DETACH PARTITION.
        bool detach = false;

        /// true for ATTACH PART (and false for PARTITION)
        bool part = false;

        /// For ATTACH PARTITION partition FROM db.table
        String from_database;
        String from_table;
        bool replace = true;

        /// For FETCH PARTITION - path in ZK to the shard, from which to download the partition.
        String from_zookeeper_path;

        /// For FREEZE PARTITION
        String with_name;

        static PartitionCommand dropPartition(const ASTPtr & partition, bool detach)
        {
            PartitionCommand res;
            res.type = DROP_PARTITION;
            res.partition = partition;
            res.detach = detach;
            return res;
        }

        static PartitionCommand clearColumn(const ASTPtr & partition, const Field & column_name)
        {
            PartitionCommand res;
            res.type = CLEAR_COLUMN;
            res.partition = partition;
            res.column_name = column_name;
            return res;
        }

        static PartitionCommand attachPartition(const ASTPtr & partition, bool part)
        {
            PartitionCommand res;
            res.type = ATTACH_PARTITION;
            res.partition = partition;
            res.part = part;
            return res;
        }

        static PartitionCommand replacePartition(const ASTPtr & partition, bool replace, const String & from_database, const String & from_table)
        {
            PartitionCommand res;
            res.type = REPLACE_PARTITION;
            res.partition = partition;
            res.replace = replace;
            res.from_database = from_database;
            res.from_table = from_table;
            return res;
        }

        static PartitionCommand fetchPartition(const ASTPtr & partition, const String & from)
        {
            PartitionCommand res;
            res.type = FETCH_PARTITION;
            res.partition = partition;
            res.from_zookeeper_path = from;
            return res;
        }

        static PartitionCommand freezePartition(const ASTPtr & partition, const String & with_name)
        {
            PartitionCommand res;
            res.type = FREEZE_PARTITION;
            res.partition = partition;
            res.with_name = with_name;
            return res;
        }
    };

    class PartitionCommands : public std::vector<PartitionCommand>
    {
    public:
        void validate(const IStorage & table);
    };

    ASTPtr query_ptr;

    const Context & context;

    static void parseAlter(const ASTAlterQuery::ParameterContainer & params,
        AlterCommands & out_alter_commands,
        PartitionCommands & out_partition_commands,
        MutationCommands & out_mutation_commands);
};

}
