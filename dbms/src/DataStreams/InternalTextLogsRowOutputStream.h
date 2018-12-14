#pragma once
#include <DataStreams/IBlockOutputStream.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/// Prints internal server logs
/// Input blocks have to have the same structure as SystemLogsQueue::getSampleBlock()
/// NOTE: IRowOutputStream does not suite well for this case
class InternalTextLogsRowOutputStream : public IBlockOutputStream
{
public:

    InternalTextLogsRowOutputStream(WriteBuffer & buf_out) : wb(buf_out) {}

    Block getHeader() const override;

    void write(const Block & block) override;

    void flush() override
    {
        wb.next();
    }

private:

    WriteBuffer & wb;
};

}
