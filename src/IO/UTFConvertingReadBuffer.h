#pragma once
#include <memory>
#include <vector>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class UTFConvertingReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    enum class Encoding
    {
        UTF8,
        UTF16_LE,
        UTF16_BE,
        UTF32_LE,
        UTF32_BE
    };

    UTFConvertingReadBuffer(std::unique_ptr<ReadBuffer> impl_, Encoding encoding_);
    ~UTFConvertingReadBuffer() override;

private:
    bool nextImpl() override;

    std::unique_ptr<ReadBuffer> impl;
    Encoding encoding;

    struct UConverter;
    UConverter * source_converter = nullptr;
    UConverter * target_converter = nullptr;

    std::vector<uint16_t> pivot_buffer; // UChar is 16-bit
    uint16_t * pivot_source = nullptr;
    uint16_t * pivot_target = nullptr;

    bool eof = false;
};

}
