#include "Progress.h"

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
void ProgressValues::read(ReadBuffer & in, UInt64 server_revision)
{
    size_t new_read_rows = 0;
    size_t new_read_bytes = 0;
    size_t new_total_rows_to_read = 0;
    size_t new_written_rows = 0;
    size_t new_written_bytes = 0;

    readVarUInt(new_read_rows, in);
    readVarUInt(new_read_bytes, in);
    readVarUInt(new_total_rows_to_read, in);
    if (server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)
    {
        readVarUInt(new_written_rows, in);
        readVarUInt(new_written_bytes, in);
    }

    this->read_rows = new_read_rows;
    this->read_bytes = new_read_bytes;
    this->total_rows_to_read = new_total_rows_to_read;
    this->written_rows = new_written_rows;
    this->written_bytes = new_written_bytes;
}


void ProgressValues::write(WriteBuffer & out, UInt64 client_revision) const
{
    writeVarUInt(this->read_rows, out);
    writeVarUInt(this->read_bytes, out);
    writeVarUInt(this->total_rows_to_read, out);
    if (client_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)
    {
        writeVarUInt(this->written_rows, out);
        writeVarUInt(this->written_bytes, out);
    }
}

void ProgressValues::writeJSON(WriteBuffer & out) const
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.

    writeCString("{\"read_rows\":\"", out);
    writeText(this->read_rows, out);
    writeCString("\",\"read_bytes\":\"", out);
    writeText(this->read_bytes, out);
    writeCString("\",\"written_rows\":\"", out);
    writeText(this->written_rows, out);
    writeCString("\",\"written_bytes\":\"", out);
    writeText(this->written_bytes, out);
    writeCString("\",\"total_rows_to_read\":\"", out);
    writeText(this->total_rows_to_read, out);
    writeCString("\"}", out);
}

bool Progress::incrementPiecewiseAtomically(const Progress & rhs)
{
    read_rows += rhs.read_rows;
    read_bytes += rhs.read_bytes;
    read_raw_bytes += rhs.read_raw_bytes;

    total_rows_to_read += rhs.total_rows_to_read;
    total_raw_bytes_to_read += rhs.total_raw_bytes_to_read;

    written_rows += rhs.written_rows;
    written_bytes += rhs.written_bytes;

    return rhs.read_rows || rhs.written_rows;
}

void Progress::reset()
{
    read_rows = 0;
    read_bytes = 0;
    read_raw_bytes = 0;

    total_rows_to_read = 0;
    total_raw_bytes_to_read = 0;

    written_rows = 0;
    written_bytes = 0;
}

ProgressValues Progress::getValues() const
{
    ProgressValues res;

    res.read_rows = read_rows.load(std::memory_order_relaxed);
    res.read_bytes = read_bytes.load(std::memory_order_relaxed);
    res.read_raw_bytes = read_raw_bytes.load(std::memory_order_relaxed);

    res.total_rows_to_read = total_rows_to_read.load(std::memory_order_relaxed);
    res.total_raw_bytes_to_read = total_raw_bytes_to_read.load(std::memory_order_relaxed);

    res.written_rows = written_rows.load(std::memory_order_relaxed);
    res.written_bytes = written_bytes.load(std::memory_order_relaxed);

    return res;
}

ProgressValues Progress::fetchAndResetPiecewiseAtomically()
{
    ProgressValues res;

    res.read_rows = read_rows.fetch_and(0);
    res.read_bytes = read_bytes.fetch_and(0);
    res.read_raw_bytes = read_raw_bytes.fetch_and(0);

    res.total_rows_to_read = total_rows_to_read.fetch_and(0);
    res.total_raw_bytes_to_read = total_raw_bytes_to_read.fetch_and(0);

    res.written_rows = written_rows.fetch_and(0);
    res.written_bytes = written_bytes.fetch_and(0);

    return res;
}

Progress & Progress::operator=(Progress && other)
{
    read_rows = other.read_rows.load(std::memory_order_relaxed);
    read_bytes = other.read_bytes.load(std::memory_order_relaxed);
    read_raw_bytes = other.read_raw_bytes.load(std::memory_order_relaxed);

    total_rows_to_read = other.total_rows_to_read.load(std::memory_order_relaxed);
    total_raw_bytes_to_read = other.total_raw_bytes_to_read.load(std::memory_order_relaxed);

    written_rows = other.written_rows.load(std::memory_order_relaxed);
    written_bytes = other.written_bytes.load(std::memory_order_relaxed);

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
