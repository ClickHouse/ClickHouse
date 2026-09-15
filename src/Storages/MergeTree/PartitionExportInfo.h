#pragma once

#include <ctime>
#include <map>
#include <vector>
#include <base/types.h>

namespace DB
{

struct PartitionExportInfo
{
    /// Most recent exception recorded for this task by one replica. A plain `MergeTree` reports a
    /// single entry with an empty `replica`. `count` is best-effort: concurrent failing writers on
    /// the same replica may under-count by one.
    struct LastException
    {
        String replica;     /// empty for plain MergeTree
        String message;
        String part;        /// empty for task-level exceptions (commit failure, timeout)
        time_t time = 0;
        size_t count = 0;
    };

    /// A part waiting before its next export attempt. Local to the node that reports it and never
    /// shared across replicas.
    struct PartBackoff
    {
        String part;
        size_t attempts = 0;
        time_t next_retry_time = 0;
    };

    String destination_database;
    String destination_table;
    String partition_id;
    String transaction_id;
    String query_id;
    time_t create_time = 0;
    /// Replica that received the export command. Empty for plain MergeTree.
    String source_replica;
    size_t parts_count = 0;
    size_t parts_to_do = 0;
    std::vector<String> parts;
    String status;

    /// One entry per replica that has recorded at least one exception for this task.
    std::vector<LastException> last_exception_per_replica;
    /// Sum of every `count` in `last_exception_per_replica`.
    size_t exception_count = 0;

    /// Destination file paths produced by each exported part, keyed by part name. Empty until parts
    /// complete, partial while the task is PENDING. On the replicated path a value may be the
    /// sentinel "<failed to read from zk>" when a Keeper refresh was incomplete.
    std::map<String, std::vector<String>> destination_file_paths_per_part;

    /// Commit-time paths reported by the destination storage. All empty before the commit lands.
    String committed_metadata_file;
    String committed_manifest_list;
    String committed_manifest_file;
    String committed_marker_file;

    /// Parts of this task currently backing off on this node. Empty if none.
    std::vector<PartBackoff> backoff_per_part;
};

}
