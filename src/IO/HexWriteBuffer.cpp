#include "src/Core/Types.h"
#include "src/Common/hex.h"
#include "src/Common/Exception.h"
#include "src/IO/HexWriteBuffer.h"


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
    try
    {
        nextImpl();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
