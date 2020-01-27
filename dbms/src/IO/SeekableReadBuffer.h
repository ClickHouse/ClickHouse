#pragma once

#include <string>
#include <ctime>
#include <functional>
#include <fcntl.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <port/clock.h>

namespace DB {

class SeekableReadBuffer : public ReadBuffer {
public:
    SeekableReadBuffer(Position ptr, size_t size)
        : ReadBuffer(ptr, size) {}
    SeekableReadBuffer(Position ptr, size_t size, size_t offset)
        : ReadBuffer(ptr, size, offset) {}

    virtual off_t seek(off_t off, int whence) = 0;
};

}
