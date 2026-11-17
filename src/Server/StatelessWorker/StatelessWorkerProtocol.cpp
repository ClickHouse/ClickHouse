#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/LimitReadBuffer.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ProtocolDefines.h>
#include <Common/Exception.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

constexpr std::string_view COLLECTOR_LOGS = "logs";

constexpr std::string_view META_BEGIN_OFFSET = "begin_offset";
constexpr std::string_view META_DROPPED_TOTAL = "dropped_total";

void writeNativeBlock(const Block & block, WriteBuffer & out)
{
    NativeWriter writer(out, STATELESS_WORKER_PAYLOAD_NATIVE_REVISION, std::make_shared<const Block>(block.cloneEmpty()));
    writer.write(block);
}

Block readNativeBlock(ReadBuffer & in)
{
    NativeReader reader(in, STATELESS_WORKER_PAYLOAD_NATIVE_REVISION);
    return reader.read();
}

void insertUInt64Column(Block & block, std::string_view name, UInt64 value)
{
    auto column = ColumnUInt64::create();
    column->insertValue(value);
    block.insert({std::move(column), std::make_shared<DataTypeUInt64>(), String(name)});
}

/// A one-row meta block is read by column name: absent means "the writer predates this field".
UInt64 getUInt64OrDefault(const Block & meta, std::string_view name, UInt64 default_value)
{
    if (meta.rows() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Expected exactly one row in the meta block of a stateless worker logs payload, got {}", meta.rows());

    String column_name(name);
    if (!meta.has(column_name))
        return default_value;
    return meta.getByName(column_name).column->getUInt(0);
}

}

TaskCollectors TaskCollectors::parse(std::string_view value)
{
    TaskCollectors result;
    std::vector<std::string_view> names;
    boost::split(names, value, boost::is_any_of(","));
    for (auto name : names)
    {
        if (name == COLLECTOR_LOGS)
            result.logs = true;
        /// Any other name belongs to a collector this build does not have. A newer coordinator sent
        /// it; it learns from the missing payload that we did not collect it.
    }
    return result;
}

String TaskCollectors::toString() const
{
    return logs ? String(COLLECTOR_LOGS) : String();
}

void DistributedQueryTaskStatus::write(WriteBuffer & out, const TaskCollectors & collectors) const
{
    writeStringBinary(status, out);
    writeStringBinary(error_message, out);
    progress.write(out, DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);
    writeIntBinary(error_code, out);

    /// A coordinator that asked for nothing may predate payloads and reads the body up to here.
    if (!collectors.any())
        return;

    if (collectors.logs)
    {
        /// Present on every reply while logs are collected, also when the task has nothing to send.
        writeTaskLogsPayloadFrame(out, logs.value_or(TaskLogsPayload{}));
    }

    writeVarUInt(TASK_STATUS_PAYLOAD_END, out);
}

void DistributedQueryTaskStatus::read(ReadBuffer & in, UInt64 version)
{
    readStringBinary(status, in);
    readStringBinary(error_message, in);
    progress.read(in, version);
    readIntBinary(error_code, in);

    /// A worker that appended nothing: older than payloads, or not asked to collect anything.
    if (in.eof())
        return;

    forEachTaskStatusPayload(in);
}

void DistributedQueryTaskStatus::readPayload(UInt64 tag, ReadBuffer & payload)
{
    if (tag == TASK_STATUS_PAYLOAD_LOGS)
        logs = readTaskLogsPayload(payload);
}

void DistributedQueryTaskStatus::forEachTaskStatusPayload(ReadBuffer & in)
{
    while (true)
    {
        UInt64 tag = 0;
        readVarUInt(tag, in);
        if (tag == TASK_STATUS_PAYLOAD_END)
            break;

        UInt64 length = 0;
        readVarUInt(length, in);
        /// Exactly `length` bytes belong to this payload; `read_no_less` makes a truncated body throw.
        LimitReadBuffer payload(in, {.read_no_less = length, .read_no_more = length});

        readPayload(tag, payload);

        /// A tag the handler does not know, or bytes a newer worker appended inside a known payload.
        payload.ignoreAll();
    }
}

void writeTaskStatusPayloadFrame(WriteBuffer & out, UInt64 tag, std::string_view payload)
{
    writeVarUInt(tag, out);
    /// Length-prefixed bytes: the same layout `forEachTaskStatusPayload` reads.
    writeStringBinary(payload, out);
}

void writeTaskLogsPayloadFrame(WriteBuffer & out, const TaskLogsPayload & logs)
{
    WriteBufferFromOwnString payload;
    writeTaskLogsPayload(logs, payload);
    payload.finalize();
    writeTaskStatusPayloadFrame(out, TASK_STATUS_PAYLOAD_LOGS, payload.stringView());
}

void writeTaskLogsPayload(const TaskLogsPayload & logs, WriteBuffer & out)
{
    Block meta;
    insertUInt64Column(meta, META_BEGIN_OFFSET, logs.begin_offset);
    insertUInt64Column(meta, META_DROPPED_TOTAL, logs.dropped_total);
    writeNativeBlock(meta, out);

    /// The rows block is always written, with its header even when there are no rows, so the reader
    /// finds the two blocks it expects.
    if (logs.rows.columns() != 0)
        writeNativeBlock(logs.rows, out);
    else
        writeNativeBlock(InternalTextLogsQueue::getSampleBlock(), out);
}

TaskLogsPayload readTaskLogsPayload(ReadBuffer & in)
{
    TaskLogsPayload result;

    Block meta = readNativeBlock(in);
    result.begin_offset = getUInt64OrDefault(meta, META_BEGIN_OFFSET, 0);
    result.dropped_total = getUInt64OrDefault(meta, META_DROPPED_TOTAL, 0);

    result.rows = readNativeBlock(in);
    return result;
}

namespace
{

/// More than any query has tasks on one worker; bounds what a malformed body makes the reader allocate.
constexpr UInt64 MAX_TASK_IDS_IN_REQUEST = 100000;

UInt64 readTaskCount(ReadBuffer & in)
{
    UInt64 count = 0;
    readVarUInt(count, in);
    if (count > MAX_TASK_IDS_IN_REQUEST)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Too many tasks in a stateless worker request: {}", count);
    return count;
}

}

void writeTaskIds(const Strings & task_ids, WriteBuffer & out)
{
    writeVarUInt(task_ids.size(), out);
    for (const auto & task_id : task_ids)
        writeStringBinary(task_id, out);
}

Strings readTaskIds(ReadBuffer & in)
{
    Strings task_ids(readTaskCount(in));
    for (auto & task_id : task_ids)
        readStringBinary(task_id, in);
    return task_ids;
}

void writeTaskStatuses(const std::vector<DistributedQueryTaskStatusEntry> & entries, WriteBuffer & out)
{
    writeVarUInt(entries.size(), out);
    for (const auto & entry : entries)
    {
        writeStringBinary(entry.task_id, out);
        WriteBufferFromOwnString status;
        entry.status.write(status, entry.collectors);
        status.finalize();
        writeStringBinary(status.str(), out);
    }
}

std::vector<DistributedQueryTaskStatusEntry> readTaskStatuses(ReadBuffer & in)
{
    std::vector<DistributedQueryTaskStatusEntry> entries(readTaskCount(in));
    for (auto & entry : entries)
    {
        readStringBinary(entry.task_id, in);
        String status;
        readStringBinary(status, in);
        ReadBufferFromString status_in(status);
        entry.status.read(status_in, DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);
        if (!status_in.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected trailing data in the status of stateless worker task {}", entry.task_id);
    }
    return entries;
}

}
