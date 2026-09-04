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

void DistributedQueryTaskStatus::write(WriteBuffer & out, UInt64 version) const
{
    writeStringBinary(status, out);
    writeStringBinary(error_message, out);
    progress.write(out, version);

    if (version >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_TASK_LOGS)
    {
        const bool has_logs = logs.rows() != 0;
        writeBinary(has_logs, out);
        if (has_logs)
        {
            NativeWriter writer(out, version, std::make_shared<const Block>(InternalTextLogsQueue::getSampleBlock()));
            writer.write(logs);
        }
    }
}

void DistributedQueryTaskStatus::read(ReadBuffer & in, UInt64 version)
{
    readStringBinary(status, in);
    readStringBinary(error_message, in);
    progress.read(in, version);

    if (version >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_TASK_LOGS)
    {
        bool has_logs = false;
        readBinary(has_logs, in);
        if (has_logs)
        {
            NativeReader reader(in, version);
            logs = reader.read();
        }
    }
}

}
