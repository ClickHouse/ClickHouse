#pragma once

#include <Compression/CompressedReadBufferBase.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>


namespace DB
{

class CompressedReadBuffer final : public CompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
    size_t size_compressed = 0;

    bool nextImpl() override;

public:
    explicit CompressedReadBuffer(ReadBuffer & in_, bool allow_different_codecs_ = false, bool external_data_ = false)
        : CompressedReadBufferBase(&in_, allow_different_codecs_, external_data_), BufferWithOwnMemory<ReadBuffer>(0)
    {
    }

    [[nodiscard]] size_t readBig(char * to, size_t n) override;

    /// The compressed size of the current block.
    size_t getSizeCompressed() const
    {
        return size_compressed;
    }
};

}
