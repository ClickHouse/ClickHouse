#pragma once

#include <IO/ReadBuffer.h>
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
    ReadBufferFromMemory(const char * buf, size_t size)
        : SeekableReadBuffer(const_cast<char *>(buf), size, 0) {}

    ReadBufferFromMemory(const unsigned char * buf, size_t size)
        : SeekableReadBuffer(const_cast<char *>(reinterpret_cast<const char *>(buf)), size, 0) {}

    ReadBufferFromMemory(const signed char * buf, size_t size)
        : SeekableReadBuffer(const_cast<char *>(reinterpret_cast<const char *>(buf)), size, 0) {}

protected:
    off_t doSeek(off_t, int) override { return 0; }
};

}
