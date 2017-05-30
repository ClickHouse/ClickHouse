#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>


/// Since HexWriteBuffer is often created in the inner loop, we'll make its buffer size small.
#define DBMS_HEX_WRITE_BUFFER_SIZE 32


namespace DB
{

/** Everything that is written into it, translates to HEX (in capital letters) and writes to another WriteBuffer.
  */
class HexWriteBuffer : public WriteBuffer
{
protected:
    char buf[DBMS_HEX_WRITE_BUFFER_SIZE];

    WriteBuffer & out;

    void nextImpl() override
    {
        if (!offset())
            return;

        for (Position p = working_buffer.begin(); p != pos; ++p)
        {
            out.write("0123456789ABCDEF"[static_cast<unsigned char>(*p) >> 4]);
            out.write("0123456789ABCDEF"[static_cast<unsigned char>(*p) & 0xF]);
        }
    }

public:
    HexWriteBuffer(WriteBuffer & out_) : WriteBuffer(buf, sizeof(buf)), out(out_) {}

    ~HexWriteBuffer() override
    {
        try
        {
            nextImpl();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
};

}
