#pragma once
#include <IO/Progress.h>
#include <base/types.h>
#include <Core/Block.h>

#include <optional>
#include <string_view>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

/// Native revision of the payload blocks. Fixed, so there is nothing to negotiate. Revision 0 has no
/// `BlockInfo` and no custom serialization, and every build reads it.
constexpr UInt64 STATELESS_WORKER_PAYLOAD_NATIVE_REVISION = 0;

/// Payload tags of a `get_status` reply. A new payload shape gets a new tag; unknown tags are skipped by length.
constexpr UInt64 TASK_STATUS_PAYLOAD_END = 0;
constexpr UInt64 TASK_STATUS_PAYLOAD_LOGS = 1;

/// The `collect` parameter of `start`: which payloads the worker adds to status replies. Unknown names are ignored.
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
    /// Lines this task put into earlier replies.
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
    void read(ReadBuffer & in, UInt64 version);

    void readPayload(UInt64 tag, ReadBuffer & payload);

    /// Reads payloads until the end tag. Unknown tags and unread bytes inside a payload are skipped by length.
    void forEachTaskStatusPayload(ReadBuffer & in);
};

/// Frames one already serialized payload: tag, byte length, bytes.
void writeTaskStatusPayloadFrame(WriteBuffer & out, UInt64 tag, std::string_view payload);

/// Serializes the logs payload and frames it under `TASK_STATUS_PAYLOAD_LOGS`.
void writeTaskLogsPayloadFrame(WriteBuffer & out, const TaskLogsPayload & logs);

/// The logs payload body: a one-row Native block with the counters as `UInt64` columns, then the
/// rows block. Both at `STATELESS_WORKER_PAYLOAD_NATIVE_REVISION`.
void writeTaskLogsPayload(const TaskLogsPayload & logs, WriteBuffer & out);

/// Reads the counters by column name; unknown columns are ignored, missing ones default to 0.
TaskLogsPayload readTaskLogsPayload(ReadBuffer & in);

}
