#pragma once

#include "SeekableReadBuffer.h"


namespace DB
{
/** Allows to read from memory range.
  * In comparison with just ReadBuffer, it only adds convenient constructors, that do const_cast.
  * In fact, ReadBuffer will not modify data in buffer, but it requires non-const pointer.
  */
class ReadBufferFromMemory : public SeekableReadBuffer
{
public:
    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    ReadBufferFromMemory(const CharT * buf, size_t size)
        : SeekableReadBuffer(const_cast<char *>(reinterpret_cast<const char *>(buf)), size, 0) {}

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;
};

}
