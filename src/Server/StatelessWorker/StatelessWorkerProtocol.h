#pragma once
#include <IO/Progress.h>
#include <base/types.h>
#include <Core/Block.h>

#include <optional>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

struct DistributedQueryTaskStatus
{
    String status;
    String error_message;
    Progress progress;
    /// Error code of a failed task, 0 otherwise. Sent since task serialization version 3.
    Int32 error_code = 0;

    /// Log lines collected on the worker since the previous status poll, in the
    /// InternalTextLogsQueue block format. Empty when there is nothing to send or the
    /// negotiated version predates DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_TASK_LOGS.
    Block logs;

    void write(WriteBuffer & out, UInt64 version) const;
    void read(ReadBuffer & in, UInt64 version);
};

/// Choose the version used to serialize a `get_status` response. `requested_version` is the
/// coordinator's `task_status_version` request parameter (nullopt when it did not send one).
/// A coordinator that does not negotiate (old binary) gets the legacy format; otherwise the
/// requested version is clamped to what this worker can actually serialize
/// (DBMS_TCP_PROTOCOL_VERSION). The worker echoes the result in the
/// X-ClickHouse-Task-Status-Version response header so the coordinator parses the exact same
/// version it was written with. See StatelessWorkerClient::getTaskStatus.
UInt64 negotiateTaskStatusVersion(std::optional<UInt64> requested_version);

}
