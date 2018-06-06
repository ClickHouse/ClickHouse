#pragma once
#include <DataStreams/IBlockOutputStream.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/// Prints internal server logs
/// Input blocks have to have the same structure as SystemLogsQueue::getSampleBlock()
/// NOTE: IRowOutputStream does not suite well for this case
class SystemLogsRowOutputStream : public IBlockOutputStream
{
public:

    static BlockOutputStreamPtr create(WriteBuffer & buf_out);

    SystemLogsRowOutputStream(WriteBuffer & buf_out) : wb(buf_out) {}

    Block getHeader() const override;

    void write(const Block & block, size_t row_num);

    void write(const Block & block) override;

    void flush() override
    {
        wb.next();
    }

private:

    WriteBuffer & wb;
};

}
