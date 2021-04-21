#pragma once

#include <Common/Exception.h>
#include <Common/ZooKeeper/Types.h>
#include <common/types.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeType.h>
#include <Disks/IDisk.h>

#include <mutex>
#include <condition_variable>


namespace DB
{

class ReadBuffer;
class WriteBuffer;
class ReplicatedMergeTreeQueue;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/// Record about what needs to be done. Only data (you can copy them).
struct ReplicatedMergeTreeLogEntryData
{
    enum Type
    {
        EMPTY,          /// Not used.
        GET_PART,       /// Get the part from another replica.
        ATTACH_PART,    /// Attach the part, possibly from our own replica (if found in /detached folder).
                        /// You may think of it as a GET_PART with some optimisations as they're nearly identical.
        MERGE_PARTS,    /// Merge the parts.
        DROP_RANGE,     /// Delete the parts in the specified partition in the specified number range.
        CLEAR_COLUMN,   /// NOTE: Deprecated. Drop specific column from specified partition.
        CLEAR_INDEX,    /// NOTE: Deprecated. Drop specific index from specified partition.
        REPLACE_RANGE,  /// Drop certain range of partitions and replace them by new ones
        MUTATE_PART,    /// Apply one or several mutations to the part.
        ALTER_METADATA, /// Apply alter modification according to global /metadata and /columns paths
    };

    static String typeToString(Type type)
    {
        switch (type)
        {
            case ReplicatedMergeTreeLogEntryData::GET_PART:         return "GET_PART";
            case ReplicatedMergeTreeLogEntryData::ATTACH_PART:      return "ATTACH_PART";
            case ReplicatedMergeTreeLogEntryData::MERGE_PARTS:      return "MERGE_PARTS";
            case ReplicatedMergeTreeLogEntryData::DROP_RANGE:       return "DROP_RANGE";
            case ReplicatedMergeTreeLogEntryData::CLEAR_COLUMN:     return "CLEAR_COLUMN";
            case ReplicatedMergeTreeLogEntryData::CLEAR_INDEX:      return "CLEAR_INDEX";
            case ReplicatedMergeTreeLogEntryData::REPLACE_RANGE:    return "REPLACE_RANGE";
            case ReplicatedMergeTreeLogEntryData::MUTATE_PART:      return "MUTATE_PART";
            case ReplicatedMergeTreeLogEntryData::ALTER_METADATA:   return "ALTER_METADATA";
            default:
                throw Exception("Unknown log entry type: " + DB::toString<int>(type), ErrorCodes::LOGICAL_ERROR);
        }
    }

    String typeToString() const
    {
        return typeToString(type);
    }

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
    String toString() const;

    String znode_name;

    Type type = EMPTY;
    String source_replica; /// Empty string means that this entry was added to the queue immediately, and not copied from the log.

    String part_checksum; /// Part checksum for ATTACH_PART, empty otherwise.

    /// The name of resulting part for GET_PART and MERGE_PARTS
    /// Part range for DROP_RANGE and CLEAR_COLUMN
    String new_part_name;
    MergeTreeDataPartType new_part_type;
    String block_id;                        /// For parts of level zero, the block identifier for deduplication (node name in /blocks/).
    mutable String actual_new_part_name;    /// GET_PART could actually fetch a part covering 'new_part_name'.
    UUID new_part_uuid = UUIDHelpers::Nil;

    Strings source_parts;
    bool deduplicate = false; /// Do deduplicate on merge
    Strings deduplicate_by_columns = {}; // Which columns should be checked for duplicates, empty means 'all' (default).
    MergeType merge_type = MergeType::REGULAR;
    String column_name;
    String index_name;

    /// For DROP_RANGE, true means that the parts need not be deleted, but moved to the `detached` directory.
    bool detach = false;

    /// REPLACE PARTITION FROM command
    struct ReplaceRangeEntry
    {
        String drop_range_part_name;

        String from_database;
        String from_table;
        Strings src_part_names; // as in from_table
        Strings new_part_names;
        Strings part_names_checksums;
        int columns_version;

        void writeText(WriteBuffer & out) const;
        void readText(ReadBuffer & in);
    };

    std::shared_ptr<ReplaceRangeEntry> replace_range_entry;

    /// ALTER METADATA and MUTATE PART command

    /// Version of metadata which will be set after this alter
    /// Also present in MUTATE_PART command, to track mutations
    /// required for complete alter execution.
    int alter_version = -1; /// May be equal to -1, if it's normal mutation, not metadata update.

    /// only ALTER METADATA command
    bool have_mutation = false; /// If this alter requires additional mutation step, for data update

    String columns_str; /// New columns data corresponding to alter_version
    String metadata_str; /// New metadata corresponding to alter_version

    /// Returns a set of parts that will appear after executing the entry + parts to block
    /// selection of merges. These parts are added to queue.virtual_parts.
    Strings getVirtualPartNames() const
    {
        /// Doesn't produce any part
        if (type == ALTER_METADATA)
            return {};

        /// DROP_RANGE does not add a real part, but we must disable merges in that range
        if (type == DROP_RANGE)
            return {new_part_name};

        /// Return {} because selection of merges in the partition where the column is cleared
        /// should not be blocked (only execution of merges should be blocked).
        if (type == CLEAR_COLUMN || type == CLEAR_INDEX)
            return {};

        if (type == REPLACE_RANGE)
        {
            Strings res = replace_range_entry->new_part_names;
            res.emplace_back(replace_range_entry->drop_range_part_name);
            return res;
        }

        return {new_part_name};
    }

    /// Returns set of parts that denote the block number ranges that should be blocked during the entry execution.
    /// These parts are added to future_parts.
    Strings getBlockingPartNames() const
    {
        Strings res = getVirtualPartNames();

        if (type == CLEAR_COLUMN)
            res.emplace_back(new_part_name);

        return res;
    }

    /// Access under queue_mutex, see ReplicatedMergeTreeQueue.
    bool currently_executing = false;    /// Whether the action is executing now.
    bool removed_by_other_entry = false;
    /// These several fields are informational only (for viewing by the user using system tables).
    /// Access under queue_mutex, see ReplicatedMergeTreeQueue.
    size_t num_tries = 0;                 /// The number of attempts to perform the action (since the server started, including the running one).
    std::exception_ptr exception;         /// The last exception, in the case of an unsuccessful attempt to perform the action.
    time_t last_attempt_time = 0;         /// The time at which the last attempt was attempted to complete the action.
    size_t num_postponed = 0;             /// The number of times the action was postponed.
    String postpone_reason;               /// The reason why the action was postponed, if it was postponed.
    time_t last_postpone_time = 0;        /// The time of the last time the action was postponed.

    /// Creation time or the time to copy from the general log to the queue of a particular replica.
    time_t create_time = 0;

    /// The quorum value (for GET_PART) is a non-zero value when the quorum write is enabled.
    size_t quorum = 0;

    /// If this MUTATE_PART entry caused by alter(modify/drop) query.
    bool isAlterMutation() const
    {
        return type == MUTATE_PART && alter_version != -1;
    }
};


struct ReplicatedMergeTreeLogEntry : public ReplicatedMergeTreeLogEntryData, std::enable_shared_from_this<ReplicatedMergeTreeLogEntry>
{
    using Ptr = std::shared_ptr<ReplicatedMergeTreeLogEntry>;

    std::condition_variable execution_complete; /// Awake when currently_executing becomes false.

    static Ptr parse(const String & s, const Coordination::Stat & stat);
};

using ReplicatedMergeTreeLogEntryPtr = std::shared_ptr<ReplicatedMergeTreeLogEntry>;


}
