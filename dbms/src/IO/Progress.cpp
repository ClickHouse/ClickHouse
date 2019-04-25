#include "Progress.h"

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void ProgressValues::read(ReadBuffer & in, UInt64 /*server_revision*/)
{
    size_t new_rows = 0;
    size_t new_bytes = 0;
    size_t new_total_rows = 0;
    size_t new_write_rows = 0;
    size_t new_write_bytes = 0;
 
    readVarUInt(new_rows, in);
    readVarUInt(new_bytes, in);
    readVarUInt(new_total_rows, in);
    readVarUInt(new_write_rows, in);
    readVarUInt(new_write_bytes, in);
 
    rows = new_rows;
    bytes = new_bytes;
    total_rows = new_total_rows;
    write_rows = new_write_rows;
    write_bytes = new_write_bytes;
} 


void ProgressValues::write(WriteBuffer & out, UInt64 /*client_revision*/) const
{
    writeVarUInt(rows, out);
    writeVarUInt(bytes, out);
    writeVarUInt(total_rows, out);
    writeVarUInt(write_rows, out);
    writeVarUInt(write_bytes, out);
}


void ProgressValues::writeJSON(WriteBuffer & out) const
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.

    writeCString("{\"read_rows\":\"", out);
    writeText(rows, out);
    writeCString("\",\"read_bytes\":\"", out);
    writeText(bytes, out);
    writeCString("\",\"write_rows\":\"", out);
    writeText(write_rows, out);
    writeCString("\",\"write_bytes\":\"", out);
    writeText(write_bytes, out);
    writeCString("\",\"total_rows\":\"", out);
    writeText(total_rows, out);
    writeCString("\"}", out);
}


void Progress::read(ReadBuffer & in, UInt64 server_revision)
{
    ProgressValues values;
    values.read(in, server_revision);

    rows.store(values.rows, std::memory_order_relaxed);
    bytes.store(values.bytes, std::memory_order_relaxed);
    total_rows.store(values.total_rows, std::memory_order_relaxed);
    write_rows.store(values.write_rows, std::memory_order_relaxed);
    write_bytes.store(values.write_bytes, std::memory_order_relaxed);
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
