#pragma once

#include <base/types.h>
#include <IO/SeekableReadBuffer.h>


namespace DB
{

/** Allows to read from another SeekableReadBuffer up to `limit_size` bytes starting from `start_offset`.
  * Note that the nested buffer may read slightly more data internally to fill its buffer.
  */
class LimitSeekableReadBuffer : public SeekableReadBuffer
{
public:
    LimitSeekableReadBuffer(SeekableReadBuffer & in_, UInt64 start_offset_, UInt64 limit_size_);
    LimitSeekableReadBuffer(std::unique_ptr<SeekableReadBuffer> in_, UInt64 start_offset_, UInt64 limit_size_);

    /// Returns adjusted position, i.e. returns `3` if the position in the nested buffer is `start_offset + 3`.
    off_t getPosition() override;

    off_t seek(off_t off, int whence) override;

private:
    std::unique_ptr<SeekableReadBuffer> in;
    off_t min_offset;
    off_t max_offset;
    std::optional<off_t> need_seek;

    bool nextImpl() override;
};

}
