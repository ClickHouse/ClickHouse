#pragma once
#include <memory>
#include <vector>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/// ReadBuffer wrapper that detects BOM (Byte Order Mark) and converts non-UTF-8 encodings to UTF-8.
/// Supports:
/// - UTF-8 BOM: skipped, data passed through in zero-copy fashion
/// - UTF-16 LE/BE: converted to UTF-8 on the fly
/// - UTF-32 LE/BE: converted to UTF-8 on the fly
/// Invalid sequences are replaced with U+FFFD (�).
class UTFConvertingReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    enum class Encoding
    {
        UTF8, /// UTF-8 BOM detected or no BOM (passthrough mode)
        UTF16_LE, /// UTF-16 Little Endian
        UTF16_BE, /// UTF-16 Big Endian
        UTF32_LE, /// UTF-32 Little Endian
        UTF32_BE /// UTF-32 Big Endian
    };

    explicit UTFConvertingReadBuffer(std::unique_ptr<ReadBuffer> impl_);
    ~UTFConvertingReadBuffer() override;

private:
    bool nextImpl() override;

    /// Detect BOM and determine encoding
    void detectBOM();

    /// Read a single UTF-16 code unit (2 bytes) with proper endianness
    bool readUTF16CodeUnit(uint16_t & code_unit);

    /// Read a single UTF-32 code point (4 bytes) with proper endianness
    bool readUTF32CodePoint(uint32_t & code_point);

    /// Encode a Unicode code point to UTF-8 (1-4 bytes)
    /// Returns the number of bytes written
    size_t encodeUTF8(uint32_t code_point, char * output);

    /// Convert from UTF-16 to UTF-8
    /// Returns true if any bytes were written
    bool convertFromUTF16();

    /// Convert from UTF-32 to UTF-8
    /// Returns true if any bytes were written
    bool convertFromUTF32();

    std::unique_ptr<ReadBuffer> impl;
    Encoding encoding = Encoding::UTF8;

    /// For handling incomplete multi-byte sequences at buffer boundaries
    std::vector<uint8_t> pending_bytes;

    /// High surrogate from UTF-16 waiting for low surrogate
    uint16_t pending_high_surrogate = 0;

    /// Track if we've reached EOF on the underlying buffer
    bool eof = false;
};

}
