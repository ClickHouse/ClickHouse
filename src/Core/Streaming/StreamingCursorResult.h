#pragma once

#include <base/types.h>

#include <map>
#include <memory>
#include <mutex>

namespace DB
{

/// Collects the final per-partition streaming cursor produced by a `STREAM [BOUNDED]` read, so the
/// outer query (for example, an incremental refreshable materialized view) can persist it and resume
/// from it. Shared through the query `Context`: every reading source merges the partitions it owns.
class StreamingCursorResult
{
public:
    /// partition_id -> {"block_number": N, "block_offset": M}
    using PartitionCursors = std::map<String, std::map<String, Int64>>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    void merge(const PartitionCursors & partition_cursors)
    {
        std::lock_guard lock(mutex);
        for (const auto & [partition_id, cursor] : partition_cursors)
            cursors[partition_id] = cursor;
    }

    PartitionCursors get() const
    {
        std::lock_guard lock(mutex);
        return cursors;
    }

private:
    mutable std::mutex mutex;
    PartitionCursors cursors;
};

using StreamingCursorResultPtr = std::shared_ptr<StreamingCursorResult>;

class CursorTreeNode;
using CursorTreeNodePtr = std::shared_ptr<CursorTreeNode>;

/// Serialize/deserialize the cursor map to/from an opaque string for durable persistence (e.g. Keeper).
String serializeStreamingCursor(const StreamingCursorResult::PartitionCursors & cursors);
StreamingCursorResult::PartitionCursors deserializeStreamingCursor(const String & data);

/// Build a generic cursor tree (partition_id -> {block_number, block_offset}) usable as a `STREAM ... CURSOR {...}` clause.
CursorTreeNodePtr streamingCursorToTree(const StreamingCursorResult::PartitionCursors & cursors);

}
