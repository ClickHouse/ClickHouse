#pragma once
#include <IO/Progress.h>
#include <base/types.h>
#include <Core/Block.h>

#include <functional>
#include <optional>
#include <string_view>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

/// Native revision of the payload blocks a worker appends to a `get_status` reply. Frozen on purpose:
/// a writer and a reader of any age agree on it by construction, so there is nothing to negotiate.
/// Revision 0 is the layout the persistent storages write to disk (no `BlockInfo` prefix, no
/// custom-serialization byte) and is readable by every build. The blocks themselves are
/// self-describing (every column carries its name and type), which is what lets the two sides
/// evolve independently; see `readTaskLogsPayload`.
constexpr UInt64 STATELESS_WORKER_PAYLOAD_NATIVE_REVISION = 0;

/// Payload tags of a `get_status` reply. A tag names a shape; a shape only ever gains columns (never
/// retypes or removes one), and a genuinely new shape is a new tag. A reader skips tags it does not
/// know by their byte length, so a coordinator and a worker of different ages can talk.
constexpr UInt64 TASK_STATUS_PAYLOAD_END = 0;
constexpr UInt64 TASK_STATUS_PAYLOAD_LOGS = 1;

/// What the coordinator asked the worker to collect for a task: the `collect` URL parameter of the
/// `start` request, a comma-separated list. The worker attaches only the listed collectors and
/// appends only their payloads to status replies. Names a worker does not know are ignored, which
/// is how an older worker answers a newer coordinator: with fewer payloads, never with an error.
struct TaskCollectors
{
    bool logs = false;

    static TaskCollectors parse(std::string_view value);
    String toString() const;
    bool any() const { return logs; }
};

/// Forwarded worker text logs. Present on every reply while logs are collected, also when empty.
struct TaskLogsPayload
{
    /// Lines this task already put into earlier replies. The coordinator compares it with the end
    /// of the previous batch it received; a gap is a batch lost to a retried status poll.
    UInt64 begin_offset = 0;
    /// Lines dropped on the worker because its forwarding buffer was full, cumulative.
    UInt64 dropped_total = 0;
    /// The batch, in the `InternalTextLogsQueue` block layout. May have zero rows.
    Block rows;
};

struct DistributedQueryTaskStatus
{
    String status;
    String error_message;
    Progress progress;
    /// Error code of a failed task, 0 otherwise. Sent since task serialization version 3.
    Int32 error_code = 0;

    /// Set when the reply carried the logs payload, i.e. the coordinator asked for logs and the
    /// worker is new enough to collect them.
    std::optional<TaskLogsPayload> logs;

    /// The fixed part of the reply (status, error, progress, error code) is written exactly as it
    /// always was. Payloads follow only when `collectors` names at least one, so a coordinator that
    /// asked for nothing gets a reply it has always understood.
    void write(WriteBuffer & out, const TaskCollectors & collectors) const;
    /// Reads the fixed part, then payloads until the end tag if the body continues. A worker that
    /// appended nothing (older, or not asked) leaves `logs` unset.
    void read(ReadBuffer & in);

    /// Reads one payload of the list into the matching field; a tag this build does not know is left
    /// for `forEachTaskStatusPayload` to skip. One branch per collector.
    void readPayload(UInt64 tag, ReadBuffer & payload);

    /// Walks the payload list that follows the fixed body: for every payload calls `readPayload` with
    /// its tag and a buffer bounded to exactly its bytes, then skips whatever was left unread, and
    /// stops at the end tag.
    ///
    /// The list is a set, not a sequence. The worker appends one payload per collector the coordinator
    /// asked for, in whatever order it likes, and a reader may know only some of the tags. So the
    /// reader never assumes a position: it dispatches on the tag, skips unknown tags by their length,
    /// and skips bytes a newer worker appended inside a known payload. That is what lets collectors be
    /// added and combined later without both sides agreeing on an order. A truncated body throws
    /// instead of desynchronizing the list.
    void forEachTaskStatusPayload(ReadBuffer & in);
};

/// Frames one payload: tag, byte length, bytes. The payload is materialized first to learn its length.
void writeTaskStatusPayloadFrame(WriteBuffer & out, UInt64 tag, const std::function<void(WriteBuffer &)> & write_payload);

/// The logs payload framed under `TASK_STATUS_PAYLOAD_LOGS`.
void writeTaskStatusPayload(WriteBuffer & out, const TaskLogsPayload & logs);

/// The logs payload body: a one-row Native block with the counters as `UInt64` columns, then the
/// rows block. Both at `STATELESS_WORKER_PAYLOAD_NATIVE_REVISION`.
void writeTaskLogsPayload(const TaskLogsPayload & logs, WriteBuffer & out);

/// Reads the counters by column name, so a meta block with extra columns (newer worker) or missing
/// ones (older worker) reads fine: unknown columns are ignored, missing ones keep their defaults.
/// Consumes exactly the two blocks; whatever a newer worker appended after them is left to the
/// caller, which skips the rest of the payload by its length.
TaskLogsPayload readTaskLogsPayload(ReadBuffer & in);

}
