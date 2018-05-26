#pragma once

#include <Common/Exception.h>
#include <Common/ZooKeeper/Types.h>
#include <Core/Types.h>
#include <IO/WriteHelpers.h>

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
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int CANNOT_PARSE_TEXT;
}


/// Record about what needs to be done. Only data (you can copy them).
struct ReplicatedMergeTreeLogEntryData
{
    enum Type
    {
        EMPTY,          /// Not used.
        GET_PART,       /// Get the part from another replica.
        MERGE_PARTS,    /// Merge the parts.
        DROP_RANGE,     /// Delete the parts in the specified partition in the specified number range.
        CLEAR_COLUMN,   /// Drop specific column from specified partition.
        REPLACE_RANGE,  /// Drop certain range of partitions and replace them by new ones
    };

    static String typeToString(Type type)
    {
        switch (type)
        {
            case ReplicatedMergeTreeLogEntryData::GET_PART:         return "GET_PART";
            case ReplicatedMergeTreeLogEntryData::MERGE_PARTS:      return "MERGE_PARTS";
            case ReplicatedMergeTreeLogEntryData::DROP_RANGE:       return "DROP_RANGE";
            case ReplicatedMergeTreeLogEntryData::CLEAR_COLUMN:     return "CLEAR_COLUMN";
            case ReplicatedMergeTreeLogEntryData::REPLACE_RANGE:    return "REPLACE_RANGE";
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

    /// The name of resulting part for GET_PART and MERGE_PARTS
    /// Part range for DROP_RANGE and CLEAR_COLUMN
    String new_part_name;
    String block_id;                        /// For parts of level zero, the block identifier for deduplication (node name in /blocks/).
    mutable String actual_new_part_name;    /// GET_PART could actually fetch a part covering 'new_part_name'.

    Strings parts_to_merge;
    bool deduplicate = false; /// Do deduplicate on merge
    String column_name;

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

    /// Part names that supposed to be added to virtual_parts and future_parts
    Strings getVirtualPartNames() const
    {
        /// TODO: Instead of new_part_name use another field for these commands
        if (type == DROP_RANGE || type == CLEAR_COLUMN)
            return {new_part_name};

        if (type == REPLACE_RANGE)
        {
            Strings res = replace_range_entry->new_part_names;
            res.emplace_back(replace_range_entry->drop_range_part_name);
            return res;
        }

        return {new_part_name};
    }

    /// Access under queue_mutex, see ReplicatedMergeTreeQueue.
    bool currently_executing = false;    /// Whether the action is executing now.
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
};


struct ReplicatedMergeTreeLogEntry : ReplicatedMergeTreeLogEntryData
{
    using Ptr = std::shared_ptr<ReplicatedMergeTreeLogEntry>;

    std::condition_variable execution_complete; /// Awake when currently_executing becomes false.

    static Ptr parse(const String & s, const zkutil::Stat & stat);
};


}
