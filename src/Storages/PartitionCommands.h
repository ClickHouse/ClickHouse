#pragma once

#include <Core/Field.h>
#include <base/types.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>

#include <optional>
#include <vector>


namespace DB
{

class ASTAlterCommand;

class Pipe;

struct PartitionCommand
{
    enum Type
    {
        UNKNOWN,

        ATTACH_PARTITION,
        MOVE_PARTITION,
        DROP_PARTITION,
        DROP_DETACHED_PARTITION,
        FETCH_PARTITION,
        FREEZE_ALL_PARTITIONS,
        FREEZE_PARTITION,
        UNFREEZE_ALL_PARTITIONS,
        UNFREEZE_PARTITION,
        REPLACE_PARTITION,
    };

    Type type = UNKNOWN;

    ASTPtr partition;

    /// true for DETACH PARTITION.
    bool detach = false;

    /// true for ATTACH PART and DROP DETACHED PART (and false for PARTITION)
    bool part = false;

    /// For ATTACH PARTITION partition FROM db.table
    String from_database;
    String from_table;
    bool replace = true;

    /// For MOVE PARTITION
    String to_database;
    String to_table;

    /// For FETCH PARTITION - path in ZK to the shard, from which to download the partition.
    String from_zookeeper_path;

    /// For FREEZE PARTITION and UNFREEZE
    String with_name;

    enum MoveDestinationType
    {
        DISK,
        VOLUME,
        TABLE,
        SHARD,
    };

    std::optional<MoveDestinationType> move_destination_type;


    String move_destination_name;

    static std::optional<PartitionCommand> parse(const ASTAlterCommand * command);
    /// Convert type of the command to string (use not only type, but also
    /// different flags)
    std::string typeToString() const;
};

using PartitionCommands = std::vector<PartitionCommand>;

/// Result of executing of a single partition commands. Partition commands quite
/// different, so some fields will be empty for some commands. Currently used in
/// ATTACH and FREEZE commands.
struct PartitionCommandResultInfo
{
    /// Command type, always filled
    String command_type;
    /// Partition id, always filled
    String partition_id;
    /// Part name, always filled
    String part_name;
    /// Part name in /detached directory, filled in ATTACH
    String old_part_name = {};
    /// Absolute path to backup directory, filled in FREEZE
    String backup_path = {};
    /// Absolute path part backup, filled in FREEZE
    String part_backup_path = {};
    /// Name of the backup (specified by user or increment value), filled in
    /// FREEZE
    String backup_name = {};
};

using PartitionCommandsResultInfo = std::vector<PartitionCommandResultInfo>;

/// Convert partition commands result to Source from single Chunk, which will be
/// used to print info to the user. Tries to create narrowest table for given
/// results. For example, if all commands were FREEZE commands, than
/// old_part_name column will be absent.
Pipe convertCommandsResultToSource(const PartitionCommandsResultInfo & commands_result);

}
