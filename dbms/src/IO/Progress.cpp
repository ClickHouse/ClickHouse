#include "Progress.h"

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
void AllProgressValueImpl::read(ProgressValues & value, ReadBuffer & in, UInt64 /*server_revision*/)
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
 
    value.rows = new_rows;
    value.bytes = new_bytes;
    value.total_rows = new_total_rows;
    value.write_rows = new_write_rows;
    value.write_bytes = new_write_bytes;
} 


void AllProgressValueImpl::write(const ProgressValues & value, WriteBuffer & out, UInt64 /*client_revision*/)
{
    writeVarUInt(value.rows, out);
    writeVarUInt(value.bytes, out);
    writeVarUInt(value.total_rows, out);
    writeVarUInt(value.write_rows, out);
    writeVarUInt(value.write_bytes, out);
}

void AllProgressValueImpl::writeJSON(const ProgressValues & value, WriteBuffer & out)
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.

    writeCString("{\"read_rows\":\"", out);
    writeText(value.rows, out);
    writeCString("\",\"read_bytes\":\"", out);
    writeText(value.bytes, out);
    writeCString("\",\"write_rows\":\"", out);
    writeText(value.write_rows, out);
    writeCString("\",\"write_bytes\":\"", out);
    writeText(value.write_bytes, out);
    writeCString("\",\"total_rows\":\"", out);
    writeText(value.total_rows, out);
    writeCString("\"}", out);
}

void ReadProgressValueImpl::writeJSON(const ProgressValues & value, WriteBuffer & out)
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.

    writeCString("{\"read_rows\":\"", out);
    writeText(value.rows, out);
    writeCString("\",\"read_bytes\":\"", out);
    writeText(value.bytes, out);
    writeCString("\",\"total_rows\":\"", out);
    writeText(value.total_rows, out);
    writeCString("\"}", out);
}

void WriteProgressValueImpl::writeJSON(const ProgressValues & value, WriteBuffer & out)
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.

    writeCString("{\"write_rows\":\"", out);
    writeText(value.write_rows, out);
    writeCString("\",\"write_bytes\":\"", out);
    writeText(value.write_bytes, out);
    writeCString("\"}", out);
}

}
