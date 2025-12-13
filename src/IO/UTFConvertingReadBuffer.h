#pragma once
#include <memory>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class UTFConvertingReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit UTFConvertingReadBuffer(std::unique_ptr<ReadBuffer> impl_);

private:
    enum class Encoding
    {
        UTF8,
        UTF16_LE,
        UTF16_BE,
        UTF32_LE,
        UTF32_BE,
        UNKNOWN
    };

    bool nextImpl() override;

    void detectAndProcessBOM();
    size_t convertUTF16ToUTF8(const char * src, size_t src_size, char * dst, size_t dst_capacity) const;
    size_t convertUTF32ToUTF8(const char * src, size_t src_size, char * dst, size_t dst_capacity) const;

    std::unique_ptr<ReadBuffer> impl;
    Encoding detected_encoding;
    bool bom_detected;
};

}
