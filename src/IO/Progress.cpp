#include "Progress.h"

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/ProtocolDefines.h>


namespace DB
{

namespace
{
    UInt64 getApproxTotalRowsToRead(UInt64 read_rows, UInt64 read_bytes, UInt64 total_bytes_to_read)
    {
        if (!read_rows || !read_bytes)
            return 0;

        auto bytes_per_row = std::ceil(static_cast<double>(read_bytes) / read_rows);
        return static_cast<UInt64>(std::ceil(static_cast<double>(total_bytes_to_read) / bytes_per_row));
    }
}


bool Progress::empty() const
{
    return read_rows == 0
        && read_bytes == 0
        && written_rows == 0
        && written_bytes == 0
        && total_rows_to_read == 0
        && result_rows == 0
        && result_bytes == 0;
    /// We deliberately don't include "elapsed_ns" as a volatile value.
}


void ProgressValues::read(ReadBuffer & in, UInt64 server_revision)
{
    readVarUInt(read_rows, in);
    readVarUInt(read_bytes, in);
    readVarUInt(total_rows_to_read, in);
    if (server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS)
    {
        readVarUInt(total_bytes_to_read, in);
    }
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
    /// In new TCP protocol we can send total_bytes_to_read without total_rows_to_read.
    /// If client doesn't support total_bytes_to_read, send approx total_rows_to_read
    /// to indicate at least approx progress.
    if (client_revision < DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS && total_bytes_to_read && !total_rows_to_read)
        writeVarUInt(getApproxTotalRowsToRead(read_rows, read_bytes, total_bytes_to_read), out);
    else
        writeVarUInt(total_rows_to_read, out);
    if (client_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS)
    {
        writeVarUInt(total_bytes_to_read, out);
    }
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

void ProgressValues::writeJSON(WriteBuffer & out, bool write_zero_values) const
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.

    bool has_value = false;

    auto write = [&](const char * name, UInt64 value)
    {
        if (!value && !write_zero_values)
            return;
        if (has_value)
            writeChar(',', out);
        writeCString(name, out);
        writeCString(":\"", out);
        writeIntText(value, out);
        writeChar('"', out);
        has_value = true;
    };

    writeCString("{", out);
    write("\"read_rows\"", read_rows);
    write("\"read_bytes\"", read_bytes);
    write("\"written_rows\"", written_rows);
    write("\"written_bytes\"", written_bytes);
    write("\"total_rows_to_read\"", total_rows_to_read);
    write("\"result_rows\"", result_rows);
    write("\"result_bytes\"", result_bytes);
    write("\"elapsed_ns\"", elapsed_ns);
    writeCString("}", out);
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
    total_bytes_to_read.store(values.total_bytes_to_read, std::memory_order_relaxed);

    written_rows.store(values.written_rows, std::memory_order_relaxed);
    written_bytes.store(values.written_bytes, std::memory_order_relaxed);

    elapsed_ns.store(values.elapsed_ns, std::memory_order_relaxed);
}

void Progress::write(WriteBuffer & out, UInt64 client_revision) const
{
    getValues().write(out, client_revision);
}

void Progress::writeJSON(WriteBuffer & out, DisplayMode mode) const
{
    getValues().writeJSON(out, mode == DisplayMode::Verbose);
}

void Progress::incrementElapsedNs(UInt64 elapsed_ns_)
{
    elapsed_ns.fetch_add(elapsed_ns_, std::memory_order_relaxed);
}

}
