#include "Progress.h"

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/ProtocolDefines.h>


namespace DB
{
void ProgressValues::read(ReadBuffer & in, UInt64 server_revision)
{
    readVarUInt(read_rows, in);
    readVarUInt(read_bytes, in);
    readVarUInt(total_rows_to_read, in);
    if (server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)
    {
        readVarUInt(written_rows, in);
        readVarUInt(written_bytes, in);
    }
    if (server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS)
    {
        readVarUInt(elapsed_ns, in);
    }
}


void ProgressValues::write(WriteBuffer & out, UInt64 client_revision) const
{
    writeVarUInt(read_rows, out);
    writeVarUInt(read_bytes, out);
    writeVarUInt(total_rows_to_read, out);
    if (client_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)
    {
        writeVarUInt(written_rows, out);
        writeVarUInt(written_bytes, out);
    }
    if (client_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS)
    {
        writeVarUInt(elapsed_ns, out);
    }
}

void ProgressValues::writeJSON(WriteBuffer & out) const
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.

    writeCString("{\"read_rows\":\"", out);
    writeText(read_rows, out);
    writeCString("\",\"read_bytes\":\"", out);
    writeText(read_bytes, out);
    writeCString("\",\"written_rows\":\"", out);
    writeText(written_rows, out);
    writeCString("\",\"written_bytes\":\"", out);
    writeText(written_bytes, out);
    writeCString("\",\"total_rows_to_read\":\"", out);
    writeText(total_rows_to_read, out);
    writeCString("\",\"result_rows\":\"", out);
    writeText(result_rows, out);
    writeCString("\",\"result_bytes\":\"", out);
    writeText(result_bytes, out);
    writeCString("\",\"elapsed_ns\":\"", out);
    writeText(elapsed_ns, out);
    writeCString("\"}", out);
}

bool Progress::incrementPiecewiseAtomically(const Progress & rhs)
{
    read_rows += rhs.read_rows;
    read_bytes += rhs.read_bytes;

    total_rows_to_read += rhs.total_rows_to_read;
    total_bytes_to_read += rhs.total_bytes_to_read;

    written_rows += rhs.written_rows;
    written_bytes += rhs.written_bytes;

    result_rows += rhs.result_rows;
    result_bytes += rhs.result_bytes;

    elapsed_ns += rhs.elapsed_ns;

    return rhs.read_rows || rhs.written_rows;
}

void Progress::reset()
{
    read_rows = 0;
    read_bytes = 0;

    total_rows_to_read = 0;
    total_bytes_to_read = 0;

    written_rows = 0;
    written_bytes = 0;

    result_rows = 0;
    result_bytes = 0;

    elapsed_ns = 0;
}

ProgressValues Progress::getValues() const
{
    ProgressValues res;

    res.read_rows = read_rows.load(std::memory_order_relaxed);
    res.read_bytes = read_bytes.load(std::memory_order_relaxed);

    res.total_rows_to_read = total_rows_to_read.load(std::memory_order_relaxed);
    res.total_bytes_to_read = total_bytes_to_read.load(std::memory_order_relaxed);

    res.written_rows = written_rows.load(std::memory_order_relaxed);
    res.written_bytes = written_bytes.load(std::memory_order_relaxed);

    res.result_rows = result_rows.load(std::memory_order_relaxed);
    res.result_bytes = result_bytes.load(std::memory_order_relaxed);

    res.elapsed_ns = elapsed_ns.load(std::memory_order_relaxed);

    return res;
}

ProgressValues Progress::fetchValuesAndResetPiecewiseAtomically()
{
    ProgressValues res;

    res.read_rows = read_rows.fetch_and(0);
    res.read_bytes = read_bytes.fetch_and(0);

    res.total_rows_to_read = total_rows_to_read.fetch_and(0);
    res.total_bytes_to_read = total_bytes_to_read.fetch_and(0);

    res.written_rows = written_rows.fetch_and(0);
    res.written_bytes = written_bytes.fetch_and(0);

    res.result_rows = result_rows.fetch_and(0);
    res.result_bytes = result_bytes.fetch_and(0);

    res.elapsed_ns = elapsed_ns.fetch_and(0);

    return res;
}

Progress Progress::fetchAndResetPiecewiseAtomically()
{
    Progress res;

    res.read_rows = read_rows.fetch_and(0);
    res.read_bytes = read_bytes.fetch_and(0);

    res.total_rows_to_read = total_rows_to_read.fetch_and(0);
    res.total_bytes_to_read = total_bytes_to_read.fetch_and(0);

    res.written_rows = written_rows.fetch_and(0);
    res.written_bytes = written_bytes.fetch_and(0);

    res.result_rows = result_rows.fetch_and(0);
    res.result_bytes = result_bytes.fetch_and(0);

    res.elapsed_ns = elapsed_ns.fetch_and(0);

    return res;
}

Progress & Progress::operator=(Progress && other) noexcept
{
    read_rows = other.read_rows.load(std::memory_order_relaxed);
    read_bytes = other.read_bytes.load(std::memory_order_relaxed);

    total_rows_to_read = other.total_rows_to_read.load(std::memory_order_relaxed);
    total_bytes_to_read = other.total_bytes_to_read.load(std::memory_order_relaxed);

    written_rows = other.written_rows.load(std::memory_order_relaxed);
    written_bytes = other.written_bytes.load(std::memory_order_relaxed);

    result_rows = other.result_rows.load(std::memory_order_relaxed);
    result_bytes = other.result_bytes.load(std::memory_order_relaxed);

    elapsed_ns = other.elapsed_ns.load(std::memory_order_relaxed);

    return *this;
}

void Progress::read(ReadBuffer & in, UInt64 server_revision)
{
    ProgressValues values;
    values.read(in, server_revision);

    read_rows.store(values.read_rows, std::memory_order_relaxed);
    read_bytes.store(values.read_bytes, std::memory_order_relaxed);
    total_rows_to_read.store(values.total_rows_to_read, std::memory_order_relaxed);

    written_rows.store(values.written_rows, std::memory_order_relaxed);
    written_bytes.store(values.written_bytes, std::memory_order_relaxed);

    elapsed_ns.store(values.elapsed_ns, std::memory_order_relaxed);
}

void Progress::write(WriteBuffer & out, UInt64 client_revision) const
{
    getValues().write(out, client_revision);
}

void Progress::writeJSON(WriteBuffer & out) const
{
    getValues().writeJSON(out);
}

}
