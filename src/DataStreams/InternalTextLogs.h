#pragma once
#include <IO/WriteBuffer.h>
#include <Core/Block.h>


namespace DB
{

/// Prints internal server logs
/// Input blocks have to have the same structure as SystemLogsQueue::getSampleBlock()
/// NOTE: IRowOutputFormat does not suite well for this case
class InternalTextLogs
{
public:
    InternalTextLogs(WriteBuffer & buf_out, bool color_) : wb(buf_out), color(color_) {}


    void write(const Block & block);

    void flush()
    {
        wb.next();
    }

private:
    WriteBuffer & wb;
    bool color;
};

}
