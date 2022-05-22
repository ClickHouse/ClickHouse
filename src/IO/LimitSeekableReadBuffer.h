#pragma once

#include <base/types.h>
#include <IO/SeekableReadBuffer.h>


namespace DB
{

/** Allows to read from another SeekableReadBuffer no far than the specified offset.
  * Note that the nested SeekableReadBuffer may read slightly more data internally to fill its buffer.
  */
class LimitSeekableReadBuffer : public SeekableReadBuffer
{
public:
    LimitSeekableReadBuffer(SeekableReadBuffer & in_, UInt64 limit_);
    LimitSeekableReadBuffer(std::unique_ptr<SeekableReadBuffer> in_, UInt64 limit_);
    ~LimitSeekableReadBuffer() override;

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override { return end_position - available(); }

private:
    SeekableReadBuffer * in;
    bool owns_in;
    UInt64 limit;
    off_t end_position; /// Offset of the end of working_buffer.

    LimitSeekableReadBuffer(SeekableReadBuffer * in_, bool owns, UInt64 limit_);
    bool nextImpl() override;
};

}
