#pragma once
#include <IO/Progress.h>
#include <base/types.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>

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
    /// negotiated task status version is below 4.
    Block logs;

    /// Two orthogonal versions gate the wire format: `task_version` is the stateless-worker task
    /// status version (DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION domain) and gates which fields
    /// exist (error_code, logs); `progress_version` is a native TCP protocol revision and gates how
    /// the native structures (progress, the logs block) serialize their bytes.
    void write(WriteBuffer & out, UInt64 task_version, UInt64 progress_version) const;
    void read(ReadBuffer & in, UInt64 task_version, UInt64 progress_version);
};

/// The task status version every coordinator/worker speaks regardless of age: the pre-logs baseline.
/// Used as the negotiation fallback when the peer does not advertise a version (an old binary sends
/// no `task_status_version` param / no echo header).
constexpr UInt64 getCommonTaskStatusVersion() { return DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION_WITHOUT_LOGS; }

/// The native progress revision every coordinator/worker speaks regardless of age: the revision
/// master serializes status progress at. Used as the negotiation fallback when the peer does not
/// advertise a `progress_version` param / echo header.
constexpr UInt64 getCommonProgressVersion() { return DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS; }

/// Choose the task status version used to serialize a `get_status` response. `requested_version`
/// is the coordinator's `task_status_version` request parameter (nullopt when it did not send
/// one). A coordinator that does not negotiate (old binary) speaks task status version 3; a
/// newer coordinator is clamped to what this worker can serialize
/// (DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION). The worker echoes the result in the
/// X-ClickHouse-Task-Status-Version response header so the coordinator parses the exact same
/// version it was written with. See StatelessWorkerClient::getTaskStatus.
UInt64 negotiateTaskStatusVersion(std::optional<UInt64> requested_version);

/// Choose the native protocol revision used to serialize the progress and logs in a `get_status`
/// response. `requested_version` is the coordinator's `progress_version` request parameter (nullopt
/// when it did not send one). A coordinator that does not negotiate (old binary) reads progress at
/// DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS, so answer in that revision; a newer
/// coordinator is clamped to this worker's own DBMS_TCP_PROTOCOL_VERSION. Echoed in the
/// X-ClickHouse-Progress-Version response header. Negotiating this separately lets progress gain
/// fields (e.g. total_bytes_to_read) across rolling upgrades independently of the task protocol.
UInt64 negotiateProgressVersion(std::optional<UInt64> requested_version);

}
