#pragma once

#include <IO/ReadBuffer.h>


namespace DB
{

/** Allows to read from memory range.
  * In comparison with just ReadBuffer, it only adds convenient constructors, that do const_cast.
  * In fact, ReadBuffer will not modify data in buffer, but it requires non-const pointer.
  */
class ReadBufferFromMemory : public ReadBuffer
{
public:
    template <typename CharT>
    ReadBufferFromMemory(const CharT * buf, size_t size)
        : ReadBuffer(const_cast<char *>(reinterpret_cast<const char *>(buf)), size, 0) {}
};

}
