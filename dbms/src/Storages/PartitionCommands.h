#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>

#include <optional>
#include <vector>


namespace DB
{

class ASTAlterCommand;

struct PartitionCommand
{
    enum Type
    {
        ATTACH_PARTITION,
        MOVE_PARTITION,
        CLEAR_COLUMN,
        CLEAR_INDEX,
        DROP_PARTITION,
        DROP_DETACHED_PARTITION,
        FETCH_PARTITION,
        FREEZE_ALL_PARTITIONS,
        FREEZE_PARTITION,
        REPLACE_PARTITION
    };

    Type type;

    ASTPtr partition;
    Field column_name;
    Field index_name;

    /// true for DETACH PARTITION.
    bool detach = false;

    /// true for ATTACH PART and DROP DETACHED PART (and false for PARTITION)
    bool part = false;

    /// For ATTACH PARTITION partition FROM db.table
    String from_database;
    String from_table;
    bool replace = true;

    /// For FETCH PARTITION - path in ZK to the shard, from which to download the partition.
    String from_zookeeper_path;

    /// For FREEZE PARTITION
    String with_name;

    enum MoveDestinationType
    {
        DISK,
        VOLUME,
    };

    MoveDestinationType move_destination_type;

    String move_destination_name;

    static std::optional<PartitionCommand> parse(const ASTAlterCommand * command);
};

class PartitionCommands : public std::vector<PartitionCommand>
{
public:
    void validate(const IStorage & table);
};


}
