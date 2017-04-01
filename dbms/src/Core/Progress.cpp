#include <Core/Progress.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void Progress::read(ReadBuffer & in, UInt64 server_revision)
{
    size_t new_rows = 0;
    size_t new_bytes = 0;
    size_t new_total_rows = 0;

    readVarUInt(new_rows, in);
    readVarUInt(new_bytes, in);

    if (server_revision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS)
        readVarUInt(new_total_rows, in);

    rows = new_rows;
    bytes = new_bytes;
    total_rows = new_total_rows;
}


void Progress::write(WriteBuffer & out, UInt64 client_revision) const
{
    writeVarUInt(rows.load(), out);
    writeVarUInt(bytes.load(), out);

    if (client_revision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS)
        writeVarUInt(total_rows.load(), out);
}


void Progress::writeJSON(WriteBuffer & out) const
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.

    writeCString("{\"read_rows\":\"", out);
    writeText(rows.load(), out);
    writeCString("\",\"read_bytes\":\"", out);
    writeText(bytes.load(), out);
    writeCString("\",\"total_rows\":\"", out);
    writeText(total_rows.load(), out);
    writeCString("\"}", out);
}

}
