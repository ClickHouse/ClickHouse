#include <common/types.h>
#include <Common/hex.h>
#include <Common/MemoryTracker.h>
#include <IO/HexWriteBuffer.h>


namespace DB
{

void HexWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    for (Position p = working_buffer.begin(); p != pos; ++p)
    {
        UInt8 byte = *p;
        out.write(hexDigitUppercase(byte / 16));
        out.write(hexDigitUppercase(byte % 16));
    }
}

HexWriteBuffer::~HexWriteBuffer()
{
    /// FIXME move final flush into the caller
    MemoryTracker::LockExceptionInThread lock;
    nextImpl();
}

}
