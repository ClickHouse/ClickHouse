#pragma once

#include <Core/Defines.h>
#include <IO/WriteBuffer.h>
#include <common/itoa.h>
#include <common/likely.h>

/// 40 digits or 39 digits and a sign
#define WRITE_HELPERS_MAX_INT_WIDTH 40U

namespace DB
{

namespace detail
{
    template <typename T>
    void NO_INLINE writeUIntTextFallback(T x, WriteBuffer & buf)
    {
        char tmp[WRITE_HELPERS_MAX_INT_WIDTH];
        int len = itoa(x, tmp) - tmp;
        buf.write(tmp, len);
    }
}

template <typename T>
void writeIntText(T x, WriteBuffer & buf)
{
    if (likely(buf.position() + WRITE_HELPERS_MAX_INT_WIDTH < buf.buffer().end()))
        buf.position() = itoa(x, buf.position());
    else
        detail::writeUIntTextFallback(x, buf);
}

}
