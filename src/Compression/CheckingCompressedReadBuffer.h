#pragma once

#include <Compression/CompressedReadBufferBase.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>


namespace DB
{

/** A buffer for reading from a compressed file with just checking checksums of
  * the compressed blocks, without any decompression.
  */
class CheckingCompressedReadBuffer : public CompressedReadBufferBase, public ReadBuffer
{
protected:
    bool nextImpl() override;

public:
    explicit CheckingCompressedReadBuffer(ReadBuffer & in_, bool allow_different_codecs_ = false)
        : CompressedReadBufferBase(&in_, allow_different_codecs_)
        , ReadBuffer(nullptr, 0)
    {
    }
};

}
