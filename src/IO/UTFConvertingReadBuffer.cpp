#include <IO/UTFConvertingReadBuffer.h>
#include <Common/Exception.h>

namespace DB
{
namespace
{
/// BOM (Byte Order Mark) signatures
constexpr uint8_t UTF8_BOM[] = {0xEF, 0xBB, 0xBF};
constexpr uint8_t UTF16_LE_BOM[] = {0xFF, 0xFE};
constexpr uint8_t UTF16_BE_BOM[] = {0xFE, 0xFF};
constexpr uint8_t UTF32_LE_BOM[] = {0xFF, 0xFE, 0x00, 0x00};
constexpr uint8_t UTF32_BE_BOM[] = {0x00, 0x00, 0xFE, 0xFF};

/// Unicode replacement character for invalid sequences
constexpr uint32_t REPLACEMENT_CHARACTER = 0xFFFD;

/// UTF-16 surrogate ranges
constexpr uint32_t HIGH_SURROGATE_START = 0xD800;
constexpr uint32_t HIGH_SURROGATE_END = 0xDBFF;
constexpr uint32_t LOW_SURROGATE_START = 0xDC00;
constexpr uint32_t LOW_SURROGATE_END = 0xDFFF;

/// Maximum valid Unicode code point
constexpr uint32_t MAX_UNICODE = 0x10FFFF;

bool isHighSurrogate(uint16_t code_unit)
{
    return code_unit >= HIGH_SURROGATE_START && code_unit <= HIGH_SURROGATE_END;
}

bool isLowSurrogate(uint16_t code_unit)
{
    return code_unit >= LOW_SURROGATE_START && code_unit <= LOW_SURROGATE_END;
}

/// Combine UTF-16 surrogate pair into a code point
uint32_t combineSurrogates(uint16_t high, uint16_t low)
{
    return 0x10000 + ((static_cast<uint32_t>(high) & 0x3FF) << 10) + (static_cast<uint32_t>(low) & 0x3FF);
}
}

UTFConvertingReadBuffer::UTFConvertingReadBuffer(std::unique_ptr<ReadBuffer> impl_)
    : ReadBuffer(nullptr, 0)
    , impl(std::move(impl_))
{
    detectBOM();
}

UTFConvertingReadBuffer::~UTFConvertingReadBuffer() = default;

void UTFConvertingReadBuffer::detectBOM()
{
    uint8_t bom_buffer[4] = {};
    size_t bytes_read = 0;

    while (bytes_read < 4)
    {
        if (!impl->hasPendingData())
        {
            if (!impl->next())
                break;
        }

        size_t available = impl->available();
        size_t to_copy = std::min(available, 4 - bytes_read);
        memcpy(bom_buffer + bytes_read, impl->position(), to_copy);
        impl->position() += to_copy;
        bytes_read += to_copy;
    }

    /// Check for BOMs (order matters: check UTF-32 before UTF-16)
    if (bytes_read >= 4 && memcmp(bom_buffer, UTF32_LE_BOM, 4) == 0)
    {
        encoding = Encoding::UTF32_LE;
        return;
    }

    if (bytes_read >= 4 && memcmp(bom_buffer, UTF32_BE_BOM, 4) == 0)
    {
        encoding = Encoding::UTF32_BE;
        return;
    }

    if (bytes_read >= 3 && memcmp(bom_buffer, UTF8_BOM, 3) == 0)
    {
        encoding = Encoding::UTF8;
        if (bytes_read > 3)
        {
            memcpy(pending_bytes, bom_buffer + 3, bytes_read - 3);
            pending_bytes_count = bytes_read - 3;
        }
        return;
    }

    if (bytes_read >= 2 && memcmp(bom_buffer, UTF16_LE_BOM, 2) == 0)
    {
        encoding = Encoding::UTF16_LE;
        if (bytes_read > 2)
        {
            memcpy(pending_bytes, bom_buffer + 2, bytes_read - 2);
            pending_bytes_count = bytes_read - 2;
        }
        return;
    }

    if (bytes_read >= 2 && memcmp(bom_buffer, UTF16_BE_BOM, 2) == 0)
    {
        encoding = Encoding::UTF16_BE;
        if (bytes_read > 2)
        {
            memcpy(pending_bytes, bom_buffer + 2, bytes_read - 2);
            pending_bytes_count = bytes_read - 2;
        }
        return;
    }

    /// No BOM detected, assume UTF-8 and put back all bytes into our pending buffer
    encoding = Encoding::UTF8;
    if (bytes_read > 0)
    {
        memcpy(pending_bytes, bom_buffer, bytes_read);
        pending_bytes_count = bytes_read;
    }
}

bool UTFConvertingReadBuffer::readUTF16CodeUnit(uint16_t & code_unit)
{
    uint8_t bytes[2];
    size_t bytes_got = 0;

    /// Try to get from pending bytes first
    while (bytes_got < 2 && pending_bytes_count > 0)
    {
        bytes[bytes_got++] = pending_bytes[0];
        memmove(pending_bytes, pending_bytes + 1, pending_bytes_count - 1);
        pending_bytes_count--;
    }

    /// Read remaining bytes from underlying buffer
    while (bytes_got < 2)
    {
        if (!impl->hasPendingData())
        {
            if (!impl->next())
            {
                /// Incomplete sequence at EOF - save what we have for next call
                if (bytes_got > 0)
                {
                    memcpy(pending_bytes, bytes, bytes_got);
                    pending_bytes_count = bytes_got;
                }
                return false;
            }
        }
        bytes[bytes_got++] = *impl->position();
        impl->ignore(1);
    }

    /// Decode based on endianness
    if (encoding == Encoding::UTF16_LE)
    {
        code_unit = static_cast<uint16_t>(static_cast<uint16_t>(bytes[0]) | (static_cast<uint16_t>(bytes[1]) << 8));
    }
    else
    {
        code_unit = static_cast<uint16_t>((static_cast<uint16_t>(bytes[0]) << 8) | static_cast<uint16_t>(bytes[1]));
    }

    return true;
}

bool UTFConvertingReadBuffer::readUTF32CodePoint(uint32_t & code_point)
{
    uint8_t bytes[4];
    size_t bytes_got = 0;

    /// Try to get from pending bytes first
    while (bytes_got < 4 && pending_bytes_count > 0)
    {
        bytes[bytes_got++] = pending_bytes[0];
        memmove(pending_bytes, pending_bytes + 1, pending_bytes_count - 1);
        pending_bytes_count--;
    }

    /// Read remaining bytes from underlying buffer
    while (bytes_got < 4)
    {
        if (!impl->hasPendingData())
        {
            if (!impl->next())
            {
                /// Incomplete sequence at EOF - save what we have for next call
                if (bytes_got > 0)
                {
                    memcpy(pending_bytes, bytes, bytes_got);
                    pending_bytes_count = bytes_got;
                }
                return false;
            }
        }
        bytes[bytes_got++] = *impl->position();
        impl->ignore(1);
    }

    /// Decode based on endianness
    if (encoding == Encoding::UTF32_LE)
    {
        code_point = static_cast<uint32_t>(bytes[0]) | (static_cast<uint32_t>(bytes[1]) << 8) | (static_cast<uint32_t>(bytes[2]) << 16)
            | (static_cast<uint32_t>(bytes[3]) << 24);
    }
    else
    {
        code_point = (static_cast<uint32_t>(bytes[0]) << 24) | (static_cast<uint32_t>(bytes[1]) << 16)
            | (static_cast<uint32_t>(bytes[2]) << 8) | static_cast<uint32_t>(bytes[3]);
    }

    return true;
}

size_t UTFConvertingReadBuffer::encodeUTF8(uint32_t code_point, char * output)
{
    /// Validate code point
    if (code_point > MAX_UNICODE || (code_point >= HIGH_SURROGATE_START && code_point <= LOW_SURROGATE_END))
    {
        code_point = REPLACEMENT_CHARACTER;
    }

    if (code_point <= 0x7F)
    {
        /// 1-byte sequence: 0xxxxxxx
        output[0] = static_cast<char>(code_point);
        return 1;
    }
    else if (code_point <= 0x7FF)
    {
        /// 2-byte sequence: 110xxxxx 10xxxxxx
        output[0] = static_cast<char>(0xC0 | (code_point >> 6));
        output[1] = static_cast<char>(0x80 | (code_point & 0x3F));
        return 2;
    }
    else if (code_point <= 0xFFFF)
    {
        /// 3-byte sequence: 1110xxxx 10xxxxxx 10xxxxxx
        output[0] = static_cast<char>(0xE0 | (code_point >> 12));
        output[1] = static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
        output[2] = static_cast<char>(0x80 | (code_point & 0x3F));
        return 3;
    }
    else
    {
        /// 4-byte sequence: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
        output[0] = static_cast<char>(0xF0 | (code_point >> 18));
        output[1] = static_cast<char>(0x80 | ((code_point >> 12) & 0x3F));
        output[2] = static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
        output[3] = static_cast<char>(0x80 | (code_point & 0x3F));
        return 4;
    }
}

bool UTFConvertingReadBuffer::convertFromUTF16()
{
    char * output_ptr = memory.data();
    char * output_end = memory.data() + memory.size();

    while (output_ptr + 6 <= output_end) /// Ensure space for maximum possible emission (6 bytes)
    {
        /// Handle pending high surrogate first
        if (pending_high_surrogate != 0)
        {
            uint16_t low_surrogate;
            if (!readUTF16CodeUnit(low_surrogate))
            {
                /// EOF reached while waiting for low surrogate
                /// Emit replacement character for orphaned high surrogate
                size_t bytes = encodeUTF8(REPLACEMENT_CHARACTER, output_ptr);
                output_ptr += bytes;
                pending_high_surrogate = 0;
                break;
            }

            if (isLowSurrogate(low_surrogate))
            {
                /// Valid surrogate pair
                uint32_t code_point = combineSurrogates(pending_high_surrogate, low_surrogate);
                size_t bytes = encodeUTF8(code_point, output_ptr);
                output_ptr += bytes;
                pending_high_surrogate = 0;
            }
            else
            {
                /// Invalid: high surrogate not followed by low surrogate
                /// Emit replacement for high surrogate and process current code unit
                size_t bytes = encodeUTF8(REPLACEMENT_CHARACTER, output_ptr);
                output_ptr += bytes;

                /// Process the current code unit (might be another high surrogate)
                if (isHighSurrogate(low_surrogate))
                {
                    /// Carry forward the new high surrogate to next iteration
                    pending_high_surrogate = low_surrogate;
                }
                else if (isLowSurrogate(low_surrogate))
                {
                    /// Orphaned low surrogate
                    bytes = encodeUTF8(REPLACEMENT_CHARACTER, output_ptr);
                    output_ptr += bytes;
                    pending_high_surrogate = 0;
                }
                else
                {
                    /// Regular BMP character
                    bytes = encodeUTF8(low_surrogate, output_ptr);
                    output_ptr += bytes;
                    pending_high_surrogate = 0;
                }
            }
            continue;
        }

        /// Read next code unit
        uint16_t code_unit;
        if (!readUTF16CodeUnit(code_unit))
        {
            /// No more data available
            break;
        }

        if (isHighSurrogate(code_unit))
        {
            /// Store and wait for low surrogate
            pending_high_surrogate = code_unit;
        }
        else if (isLowSurrogate(code_unit))
        {
            /// Orphaned low surrogate
            size_t bytes = encodeUTF8(REPLACEMENT_CHARACTER, output_ptr);
            output_ptr += bytes;
        }
        else
        {
            /// Regular BMP character (Basic Multilingual Plane)
            size_t bytes = encodeUTF8(code_unit, output_ptr);
            output_ptr += bytes;
        }
    }

    /// Handle incomplete sequences at EOF
    /// If we have a pending high surrogate or incomplete bytes, emit replacement character
    if (output_ptr + 4 <= output_end)
    {
        if (pending_high_surrogate != 0)
        {
            size_t bytes = encodeUTF8(REPLACEMENT_CHARACTER, output_ptr);
            output_ptr += bytes;
            pending_high_surrogate = 0;
        }
        else if (pending_bytes_count > 0)
        {
            /// Incomplete UTF-16 code unit at EOF
            size_t bytes = encodeUTF8(REPLACEMENT_CHARACTER, output_ptr);
            output_ptr += bytes;
            pending_bytes_count = 0;
        }
    }

    size_t written = output_ptr - memory.data();
    if (written > 0)
    {
        working_buffer = Buffer(memory.data(), memory.data() + written);
        pos = working_buffer.begin();
        return true;
    }

    return false;
}

bool UTFConvertingReadBuffer::convertFromUTF32()
{
    char * output_ptr = memory.data();
    char * output_end = memory.data() + memory.size();

    while (output_ptr + 4 <= output_end) /// Ensure space for maximum UTF-8 sequence
    {
        uint32_t code_point;
        if (!readUTF32CodePoint(code_point))
        {
            /// No more data available
            break;
        }

        size_t bytes = encodeUTF8(code_point, output_ptr);
        output_ptr += bytes;
    }

    /// Handle incomplete sequences at EOF
    /// If we have leftover incomplete bytes, emit replacement character
    if (output_ptr + 4 <= output_end && pending_bytes_count > 0)
    {
        /// Incomplete UTF-32 code point at EOF
        size_t bytes = encodeUTF8(REPLACEMENT_CHARACTER, output_ptr);
        output_ptr += bytes;
        pending_bytes_count = 0;
    }

    size_t written = output_ptr - memory.data();
    if (written > 0)
    {
        working_buffer = Buffer(memory.data(), memory.data() + written);
        pos = working_buffer.begin();
        return true;
    }

    return false;
}

bool UTFConvertingReadBuffer::nextImpl()
{
    if (eof_reached)
        return false;

    if (encoding == Encoding::UTF8)
    {
        if (pending_bytes_count > 0)
        {
            working_buffer = Buffer(pending_bytes, pending_bytes + pending_bytes_count);
            pos = working_buffer.begin();
            pending_bytes_count = 0;
            return true;
        }

        /// If we were reading from impl's buffer directly, sync our position back to it
        /// so it knows how much data we consumed.
        if (!working_buffer.empty() && working_buffer.begin() != pending_bytes)
        {
            impl->position() = pos;
        }

        if (impl->hasPendingData() || impl->next())
        {
            working_buffer = impl->buffer();
            nextimpl_working_buffer_offset = impl->offset();
            return true;
        }

        eof_reached = true;
        return false;
    }

    /// For UTF-16/32, we need to allocate memory if not already allocated
    if (memory.size() == 0)
    {
        memory.resize(DBMS_DEFAULT_BUFFER_SIZE);
    }

    /// Handle UTF-16 conversion
    if (encoding == Encoding::UTF16_LE || encoding == Encoding::UTF16_BE)
    {
        if (convertFromUTF16())
            return true;

        eof_reached = true;
        return false;
    }

    /// Handle UTF-32 conversion
    if (encoding == Encoding::UTF32_LE || encoding == Encoding::UTF32_BE)
    {
        if (convertFromUTF32())
            return true;

        eof_reached = true;
        return false;
    }

    eof_reached = true;
    return false;
}

}
