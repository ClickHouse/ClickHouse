#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Core/ProtocolDefines.h>

namespace DB
{

void DistributedQueryTaskStatus::write(WriteBuffer & out, UInt64 task_version, UInt64 progress_version) const
{
    writeStringBinary(status, out);
    writeStringBinary(error_message, out);
    progress.write(out, progress_version);
    writeIntBinary(error_code, out);

    if (task_version >= DBMS_MIN_DISTRIBUTED_TASK_SERIALIZATION_VERSION_WITH_LOGS)
    {
        const bool has_logs = logs.rows() != 0;
        writeBinary(has_logs, out);
        if (has_logs)
        {
            NativeWriter writer(out, progress_version, std::make_shared<const Block>(InternalTextLogsQueue::getSampleBlock()));
            writer.write(logs);
        }
        writeVarUInt(num_dropped_logs, out);
        writeVarUInt(forwarded_log_count, out);
    }
}

void DistributedQueryTaskStatus::read(ReadBuffer & in, UInt64 task_version, UInt64 progress_version)
{
    readStringBinary(status, in);
    readStringBinary(error_message, in);
    progress.read(in, progress_version);
    readIntBinary(error_code, in);

    if (task_version >= DBMS_MIN_DISTRIBUTED_TASK_SERIALIZATION_VERSION_WITH_LOGS)
    {
        bool has_logs = false;
        readBinary(has_logs, in);
        if (has_logs)
        {
            NativeReader reader(in, progress_version);
            logs = reader.read();
        }
        readVarUInt(num_dropped_logs, in);
        readVarUInt(forwarded_log_count, in);
    }
}

UInt64 negotiateTaskStatusVersion(std::optional<UInt64> requested_version)
{
    /// An old coordinator does not send the parameter; it speaks the pre-logs task status version
    /// (status/error/progress/error_code, no logs), so answer in that format.
    if (!requested_version)
        return getCommonTaskStatusVersion();

    /// The coordinator asked for `*requested_version`, but this worker can serialize at most
    /// DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION (it may be older than the coordinator).
    return std::min<UInt64>(*requested_version, DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION);
}

UInt64 negotiateProgressVersion(std::optional<UInt64> requested_version)
{
    /// An old coordinator does not send the parameter; master serializes status progress at the
    /// common baseline revision, so answer in that revision.
    if (!requested_version)
        return getCommonProgressVersion();

    /// The coordinator asked for `*requested_version`, but this worker can serialize at most its own
    /// native protocol version (it may be older than the coordinator).
    return std::min<UInt64>(*requested_version, DBMS_TCP_PROTOCOL_VERSION);
}

}
