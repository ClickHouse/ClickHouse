#include <IO/HexWriteBuffer.h>

#include <Core/Types.h>
#include <Common/Exception.h>
#include <Common/hex.h>
#include <common/Logger.h>


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
        LOG(EXCEPT) << __PRETTY_FUNCTION__;
    }
}

}
