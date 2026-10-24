#include <Processors/Formats/Impl/ORC/RLE.h>

#include <Common/Exception.h>

#include <algorithm>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
}

namespace DB::ORC
{

namespace
{

inline Int64 unZigZag(UInt64 value)
{
    return static_cast<Int64>((value >> 1) ^ (~(value & 1) + 1));
}

inline UInt64 zigZag(Int64 value)
{
    return (static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63);
}

/// The 5-bit codes of the bit widths in RLE v2.
constexpr UInt8 code_to_bit_width[32] = {
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 28, 30, 32, 40, 48, 56, 64};

/// The smallest bit width representable in RLE v2 that fits the given number of bits.
constexpr UInt8 closest_fixed_bits[65] = {
    1,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
    22, 23, 24, 26, 26, 28, 28, 30, 30, 32, 32, 40, 40, 40, 40, 40, 40, 40, 40, 48, 48, 48,
    48, 48, 48, 48, 48, 56, 56, 56, 56, 56, 56, 56, 56, 64, 64, 64, 64, 64, 64, 64, 64};

inline UInt32 decodeBitWidth(UInt32 code)
{
    return code_to_bit_width[code & 0x1F];
}

inline UInt32 getClosestFixedBits(UInt32 n)
{
    return n <= 64 ? closest_fixed_bits[n] : 64;
}

inline UInt32 encodeBitWidth(UInt32 width)
{
    width = getClosestFixedBits(width);
    if (width <= 24)
        return width - 1;
    switch (width)
    {
        case 26: return 24;
        case 28: return 25;
        case 30: return 26;
        case 32: return 27;
        case 40: return 28;
        case 48: return 29;
        case 56: return 30;
        default: return 31;
    }
}

/// The number of bits required to represent the value, rounded to a representable bit width.
/// Negative values require 64 bits.
inline UInt32 findClosestNumBits(Int64 value)
{
    if (value < 0)
        return 64;
    UInt32 count = 64 - static_cast<UInt32>(std::countl_zero(static_cast<UInt64>(value)));
    return getClosestFixedBits(count);
}

}


UInt64 readVarint(InputByteStream & stream)
{
    UInt64 result = 0;
    for (size_t shift = 0; shift < 64; shift += 7)
    {
        UInt8 byte = stream.readByte();
        result |= static_cast<UInt64>(byte & 0x7F) << shift;
        if (!(byte & 0x80))
            return result;
    }
    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed varint in ORC stream {}", stream.getName());
}

Int64 readSignedVarint(InputByteStream & stream)
{
    return unZigZag(readVarint(stream));
}


void ByteRleDecoder::readHeader()
{
    Int8 header = static_cast<Int8>(stream->readByte());
    if (header >= 0)
    {
        repeating = true;
        remaining_values = static_cast<size_t>(header) + 3;
        value = static_cast<char>(stream->readByte());
    }
    else
    {
        repeating = false;
        remaining_values = static_cast<size_t>(-static_cast<Int32>(header));
    }
}

void ByteRleDecoder::next(char * data, size_t num_values, const char * not_null)
{
    size_t position = 0;
    while (position < num_values)
    {
        if (not_null)
        {
            while (position < num_values && !not_null[position])
                ++position;
            if (position == num_values)
                return;
        }

        if (remaining_values == 0)
            readHeader();

        size_t count = std::min(num_values - position, remaining_values);
        size_t consumed = 0;
        if (repeating)
        {
            if (not_null)
            {
                for (size_t i = 0; i < count; ++i)
                {
                    if (not_null[position + i])
                    {
                        data[position + i] = value;
                        ++consumed;
                    }
                }
            }
            else
            {
                memset(data + position, value, count);
                consumed = count;
            }
        }
        else
        {
            if (not_null)
            {
                for (size_t i = 0; i < count; ++i)
                {
                    if (not_null[position + i])
                    {
                        data[position + i] = static_cast<char>(stream->readByte());
                        ++consumed;
                    }
                }
            }
            else
            {
                stream->readBytes(data + position, count);
                consumed = count;
            }
        }
        remaining_values -= consumed;
        position += count;
    }
}

void ByteRleDecoder::skip(size_t num_values)
{
    while (num_values)
    {
        if (remaining_values == 0)
            readHeader();
        size_t count = std::min(num_values, remaining_values);
        remaining_values -= count;
        num_values -= count;
        if (!repeating)
            stream->skipBytes(count);
    }
}

void ByteRleDecoder::seek(PositionProvider & positions)
{
    stream->seek(positions);
    remaining_values = 0;
    skip(positions.next());
}


void BooleanRleDecoder::next(char * data, size_t num_values, const char * not_null)
{
    for (size_t i = 0; i < num_values; ++i)
    {
        if (not_null && !not_null[i])
        {
            /// The PRESENT stream of a child column is read with the mask of its parent,
            /// and the masked positions must be null.
            data[i] = 0;
            continue;
        }
        if (remaining_bits == 0)
        {
            char byte;
            byte_decoder.next(&byte, 1, nullptr);
            last_byte = static_cast<UInt8>(byte);
            remaining_bits = 8;
        }
        --remaining_bits;
        data[i] = (last_byte >> remaining_bits) & 1;
    }
}

void BooleanRleDecoder::skip(size_t num_values)
{
    if (num_values <= remaining_bits)
    {
        remaining_bits -= num_values;
        return;
    }
    num_values -= remaining_bits;
    byte_decoder.skip(num_values / 8);
    if (num_values % 8)
    {
        char byte;
        byte_decoder.next(&byte, 1, nullptr);
        last_byte = static_cast<UInt8>(byte);
        remaining_bits = 8 - num_values % 8;
    }
    else
        remaining_bits = 0;
}

void BooleanRleDecoder::seek(PositionProvider & positions)
{
    byte_decoder.seek(positions);
    UInt64 consumed = positions.next();
    remaining_bits = 0;
    if (consumed > 8)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid seek position {} of a boolean stream in ORC file", consumed);
    if (consumed)
    {
        char byte;
        byte_decoder.next(&byte, 1, nullptr);
        last_byte = static_cast<UInt8>(byte);
        remaining_bits = 8 - consumed;
    }
}


namespace
{

class IntRleDecoderV1 final : public IntRleDecoder
{
public:
    IntRleDecoderV1(std::unique_ptr<InputByteStream> stream_, bool is_signed_) : stream(std::move(stream_)), is_signed(is_signed_) {}

    void next(Int64 * data, size_t num_values, const char * not_null) override
    {
        size_t position = 0;
        while (position < num_values)
        {
            if (not_null)
            {
                while (position < num_values && !not_null[position])
                    ++position;
                if (position == num_values)
                    return;
            }

            if (remaining_values == 0)
                readHeader();

            size_t count = std::min(num_values - position, remaining_values);
            size_t consumed = 0;
            for (size_t i = 0; i < count; ++i)
            {
                if (not_null && !not_null[position + i])
                    continue;
                if (repeating)
                {
                    data[position + i] = value;
                    value = static_cast<Int64>(static_cast<UInt64>(value) + static_cast<UInt64>(delta));
                }
                else
                    data[position + i] = readValue();
                ++consumed;
            }
            remaining_values -= consumed;
            position += count;
        }
    }

    void skip(size_t num_values) override
    {
        while (num_values)
        {
            if (remaining_values == 0)
                readHeader();
            size_t count = std::min(num_values, remaining_values);
            remaining_values -= count;
            num_values -= count;
            if (repeating)
                value = static_cast<Int64>(static_cast<UInt64>(value) + static_cast<UInt64>(delta) * count);
            else
                for (size_t i = 0; i < count; ++i)
                    readVarint(*stream);
        }
    }

    void seek(PositionProvider & positions) override
    {
        stream->seek(positions);
        remaining_values = 0;
        skip(positions.next());
    }

private:
    Int64 readValue()
    {
        return is_signed ? readSignedVarint(*stream) : static_cast<Int64>(readVarint(*stream));
    }

    void readHeader()
    {
        Int8 header = static_cast<Int8>(stream->readByte());
        if (header >= 0)
        {
            repeating = true;
            remaining_values = static_cast<size_t>(header) + 3;
            delta = static_cast<Int8>(stream->readByte());
            value = readValue();
        }
        else
        {
            repeating = false;
            remaining_values = static_cast<size_t>(-static_cast<Int32>(header));
        }
    }

    std::unique_ptr<InputByteStream> stream;
    const bool is_signed;
    size_t remaining_values = 0;
    bool repeating = false;
    Int64 value = 0;
    Int64 delta = 0;
};


class IntRleDecoderV2 final : public IntRleDecoder
{
public:
    IntRleDecoderV2(std::unique_ptr<InputByteStream> stream_, bool is_signed_) : stream(std::move(stream_)), is_signed(is_signed_) {}

    void next(Int64 * data, size_t num_values, const char * not_null) override
    {
        size_t position = 0;
        while (position < num_values)
        {
            if (not_null)
            {
                while (position < num_values && !not_null[position])
                    ++position;
                if (position == num_values)
                    return;
            }

            if (run_read == run_length)
                readRun();

            size_t count = std::min(num_values - position, run_length - run_read);
            if (not_null)
            {
                for (size_t i = position; i < position + count; ++i)
                    if (not_null[i])
                        data[i] = literals[run_read++];
            }
            else
            {
                memcpy(data + position, literals.data() + run_read, count * sizeof(Int64));
                run_read += count;
            }
            position += count;
        }
    }

    void skip(size_t num_values) override
    {
        while (num_values)
        {
            if (run_read == run_length)
                readRun();
            size_t count = std::min(num_values, run_length - run_read);
            run_read += count;
            num_values -= count;
        }
    }

    void seek(PositionProvider & positions) override
    {
        stream->seek(positions);
        run_read = run_length = 0;
        skip(positions.next());
    }

private:
    enum EncodingType : UInt8
    {
        SHORT_REPEAT = 0,
        DIRECT = 1,
        PATCHED_BASE = 2,
        DELTA = 3,
    };

    Int64 readLongBE(size_t num_bytes)
    {
        UInt64 result = 0;
        for (size_t i = 0; i < num_bytes; ++i)
            result = (result << 8) | stream->readByte();
        return static_cast<Int64>(result);
    }

    /// Reads `count` bit-packed values of `bit_width` bits, starting and ending at a byte boundary.
    void readInts(Int64 * out, size_t count, UInt32 bit_width)
    {
        if (bit_width % 8 == 0)
        {
            size_t num_bytes = bit_width / 8;
            for (size_t i = 0; i < count; ++i)
                out[i] = readLongBE(num_bytes);
            return;
        }

        /// The only bit widths that are not a multiple of 8 are at most 30.
        UInt64 buffer = 0;
        UInt32 bits_in_buffer = 0;
        const UInt64 mask = (1ULL << bit_width) - 1;
        for (size_t i = 0; i < count; ++i)
        {
            while (bits_in_buffer < bit_width)
            {
                buffer = (buffer << 8) | stream->readByte();
                bits_in_buffer += 8;
            }
            bits_in_buffer -= bit_width;
            out[i] = static_cast<Int64>((buffer >> bits_in_buffer) & mask);
        }
    }

    void readRun()
    {
        UInt8 first_byte = stream->readByte();
        auto encoding = static_cast<EncodingType>(first_byte >> 6);
        run_read = 0;
        switch (encoding)
        {
            case SHORT_REPEAT:
            {
                size_t num_bytes = ((first_byte >> 3) & 0x07) + 1;
                run_length = (first_byte & 0x07) + 3;
                Int64 value = readLongBE(num_bytes);
                if (is_signed)
                    value = unZigZag(static_cast<UInt64>(value));
                std::fill(literals.begin(), literals.begin() + run_length, value);
                break;
            }
            case DIRECT:
            {
                UInt32 bit_width = decodeBitWidth((first_byte >> 1) & 0x1F);
                run_length = ((static_cast<size_t>(first_byte & 0x01) << 8) | stream->readByte()) + 1;
                readInts(literals.data(), run_length, bit_width);
                if (is_signed)
                    for (size_t i = 0; i < run_length; ++i)
                        literals[i] = unZigZag(static_cast<UInt64>(literals[i]));
                break;
            }
            case PATCHED_BASE:
                readPatchedBase(first_byte);
                break;
            case DELTA:
                readDelta(first_byte);
                break;
        }
    }

    void readPatchedBase(UInt8 first_byte)
    {
        UInt32 bit_width = decodeBitWidth((first_byte >> 1) & 0x1F);
        run_length = ((static_cast<size_t>(first_byte & 0x01) << 8) | stream->readByte()) + 1;

        UInt8 third_byte = stream->readByte();
        size_t base_bytes = ((third_byte >> 5) & 0x07) + 1;
        UInt32 patch_bit_width = decodeBitWidth(third_byte & 0x1F);

        UInt8 fourth_byte = stream->readByte();
        UInt32 patch_gap_width = ((fourth_byte >> 5) & 0x07) + 1;
        size_t patch_list_length = fourth_byte & 0x1F;
        if (patch_list_length == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupt PATCHED_BASE encoded data in ORC stream {} (empty patch list)", stream->getName());

        Int64 base = readLongBE(base_bytes);
        Int64 sign_mask = static_cast<Int64>(1ULL << (base_bytes * 8 - 1));
        if (base & sign_mask)
        {
            base &= ~sign_mask;
            base = -base;
        }

        readInts(literals.data(), run_length, bit_width);

        if (patch_bit_width + patch_gap_width > 64)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Corrupt PATCHED_BASE encoded data in ORC stream {} (patch width + patch gap width > 64)", stream->getName());

        Int64 patches[32];
        readInts(patches, patch_list_length, getClosestFixedBits(patch_bit_width + patch_gap_width));

        const Int64 patch_mask = patch_bit_width >= 64 ? -1 : static_cast<Int64>((1ULL << patch_bit_width) - 1);
        size_t patch_index = 0;
        Int64 gap = 0;
        Int64 patch = 0;

        auto adjust_gap_and_patch = [&]
        {
            UInt64 current_gap = static_cast<UInt64>(patches[patch_index]) >> patch_bit_width;
            patch = patches[patch_index] & patch_mask;
            Int64 actual_gap = 0;
            /// A gap of 255 with a zero patch means that the gap continues in the next entry.
            while (current_gap == 255 && patch == 0)
            {
                actual_gap += 255;
                ++patch_index;
                if (patch_index >= patch_list_length)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupt PATCHED_BASE encoded data in ORC stream {} (unterminated gap)", stream->getName());
                current_gap = static_cast<UInt64>(patches[patch_index]) >> patch_bit_width;
                patch = patches[patch_index] & patch_mask;
            }
            gap = actual_gap + static_cast<Int64>(current_gap);
        };

        adjust_gap_and_patch();
        for (size_t i = 0; i < run_length; ++i)
        {
            if (static_cast<Int64>(i) != gap)
            {
                literals[i] = static_cast<Int64>(static_cast<UInt64>(literals[i]) + static_cast<UInt64>(base));
            }
            else
            {
                UInt64 patched = static_cast<UInt64>(literals[i]) | (bit_width < 64 ? static_cast<UInt64>(patch) << bit_width : 0);
                literals[i] = static_cast<Int64>(static_cast<UInt64>(base) + patched);
                ++patch_index;
                if (patch_index < patch_list_length)
                {
                    adjust_gap_and_patch();
                    /// The next gap is relative to the current position.
                    gap += static_cast<Int64>(i);
                }
            }
        }
    }

    void readDelta(UInt8 first_byte)
    {
        UInt32 code = (first_byte >> 1) & 0x1F;
        UInt32 bit_width = code ? decodeBitWidth(code) : 0;
        run_length = ((static_cast<size_t>(first_byte & 0x01) << 8) | stream->readByte()) + 1;

        Int64 first_value = is_signed ? readSignedVarint(*stream) : static_cast<Int64>(readVarint(*stream));
        literals[0] = first_value;
        Int64 delta_base = readSignedVarint(*stream);

        if (bit_width == 0)
        {
            /// Fixed delta.
            for (size_t i = 1; i < run_length; ++i)
                literals[i] = static_cast<Int64>(static_cast<UInt64>(literals[i - 1]) + static_cast<UInt64>(delta_base));
            return;
        }

        if (run_length < 2)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Illegal run length {} for delta encoding in ORC stream {}", run_length, stream->getName());

        Int64 previous = static_cast<Int64>(static_cast<UInt64>(first_value) + static_cast<UInt64>(delta_base));
        literals[1] = previous;
        readInts(literals.data() + 2, run_length - 2, bit_width);
        for (size_t i = 2; i < run_length; ++i)
        {
            if (delta_base < 0)
                previous = static_cast<Int64>(static_cast<UInt64>(previous) - static_cast<UInt64>(literals[i]));
            else
                previous = static_cast<Int64>(static_cast<UInt64>(previous) + static_cast<UInt64>(literals[i]));
            literals[i] = previous;
        }
    }

    std::unique_ptr<InputByteStream> stream;
    const bool is_signed;
    std::array<Int64, IntRleEncoderV2::MAX_LITERAL_SIZE> literals{};
    size_t run_length = 0;
    size_t run_read = 0;
};

}

std::unique_ptr<IntRleDecoder> createIntRleDecoder(std::unique_ptr<InputByteStream> stream, bool is_signed, RleVersion version)
{
    if (version == RleVersion::V1)
        return std::make_unique<IntRleDecoderV1>(std::move(stream), is_signed);
    return std::make_unique<IntRleDecoderV2>(std::move(stream), is_signed);
}


void ByteRleEncoder::writeValues()
{
    if (num_literals == 0)
        return;
    if (repeat)
    {
        stream.writeByte(static_cast<UInt8>(num_literals - MIN_REPEAT));
        stream.writeByte(static_cast<UInt8>(literals[0]));
    }
    else
    {
        stream.writeByte(static_cast<UInt8>(-static_cast<Int32>(num_literals)));
        stream.write(literals, num_literals);
    }
    repeat = false;
    tail_run_length = 0;
    num_literals = 0;
}

void ByteRleEncoder::write(char value)
{
    if (num_literals == 0)
    {
        literals[num_literals++] = value;
        tail_run_length = 1;
    }
    else if (repeat)
    {
        if (value == literals[0])
        {
            ++num_literals;
            if (num_literals == MAX_REPEAT_SIZE)
                writeValues();
        }
        else
        {
            writeValues();
            literals[num_literals++] = value;
            tail_run_length = 1;
        }
    }
    else
    {
        if (value == literals[num_literals - 1])
            ++tail_run_length;
        else
            tail_run_length = 1;

        if (tail_run_length == MIN_REPEAT)
        {
            if (num_literals + 1 == MIN_REPEAT)
            {
                repeat = true;
                ++num_literals;
            }
            else
            {
                num_literals -= MIN_REPEAT - 1;
                writeValues();
                literals[0] = value;
                repeat = true;
                num_literals = MIN_REPEAT;
            }
        }
        else
        {
            literals[num_literals++] = value;
            if (num_literals == MAX_LITERAL_SIZE)
                writeValues();
        }
    }
}

void ByteRleEncoder::recordPosition(std::vector<UInt64> & positions) const
{
    stream.recordPosition(positions);
    positions.push_back(num_literals);
}

void ByteRleEncoder::flush()
{
    writeValues();
}


void BooleanRleEncoder::write(bool value)
{
    current = static_cast<UInt8>((current << 1) | (value ? 1 : 0));
    ++bits_in_current;
    if (bits_in_current == 8)
    {
        byte_encoder.write(static_cast<char>(current));
        current = 0;
        bits_in_current = 0;
    }
}

void BooleanRleEncoder::recordPosition(std::vector<UInt64> & positions) const
{
    byte_encoder.recordPosition(positions);
    positions.push_back(bits_in_current);
}

void BooleanRleEncoder::flush()
{
    if (bits_in_current)
    {
        byte_encoder.write(static_cast<char>(current << (8 - bits_in_current)));
        current = 0;
        bits_in_current = 0;
    }
    byte_encoder.flush();
}


void IntRleEncoderV2::recordPosition(std::vector<UInt64> & positions) const
{
    stream.recordPosition(positions);
    positions.push_back(num_literals);
}

void IntRleEncoderV2::flush()
{
    size_t literal_start = 0;
    size_t i = 0;
    while (i < num_literals)
    {
        size_t j = i + 1;
        while (j < num_literals && literals[j] == literals[i])
            ++j;
        if (j - i >= 3)
        {
            if (literal_start < i)
                encodeLiterals(literals + literal_start, i - literal_start);
            encodeRepeat(literals[i], j - i);
            literal_start = j;
        }
        i = j;
    }
    if (literal_start < num_literals)
        encodeLiterals(literals + literal_start, num_literals - literal_start);
    num_literals = 0;
}

void IntRleEncoderV2::writeVarint(UInt64 value)
{
    while (value >= 0x80)
    {
        stream.writeByte(static_cast<UInt8>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    stream.writeByte(static_cast<UInt8>(value));
}

void IntRleEncoderV2::writeInts(const Int64 * values, size_t count, UInt32 bit_width)
{
    if (bit_width % 8 == 0)
    {
        size_t num_bytes = bit_width / 8;
        for (size_t i = 0; i < count; ++i)
        {
            UInt64 v = static_cast<UInt64>(values[i]);
            for (size_t b = num_bytes; b > 0; --b)
                stream.writeByte(static_cast<UInt8>(v >> ((b - 1) * 8)));
        }
        return;
    }

    UInt64 buffer = 0;
    UInt32 bits_in_buffer = 0;
    const UInt64 mask = (1ULL << bit_width) - 1;
    for (size_t i = 0; i < count; ++i)
    {
        buffer = (buffer << bit_width) | (static_cast<UInt64>(values[i]) & mask);
        bits_in_buffer += bit_width;
        while (bits_in_buffer >= 8)
        {
            bits_in_buffer -= 8;
            stream.writeByte(static_cast<UInt8>(buffer >> bits_in_buffer));
        }
    }
    if (bits_in_buffer)
        stream.writeByte(static_cast<UInt8>(buffer << (8 - bits_in_buffer)));
}

void IntRleEncoderV2::encodeRepeat(Int64 value, size_t count)
{
    UInt64 encoded = is_signed ? zigZag(value) : static_cast<UInt64>(value);
    if (count <= 10)
    {
        /// SHORT_REPEAT
        UInt32 bits = findClosestNumBits(static_cast<Int64>(encoded));
        UInt32 num_bytes = (bits + 7) / 8;
        stream.writeByte(static_cast<UInt8>(((num_bytes - 1) << 3) | (count - 3)));
        for (UInt32 b = num_bytes; b > 0; --b)
            stream.writeByte(static_cast<UInt8>(encoded >> ((b - 1) * 8)));
        return;
    }

    /// DELTA with zero fixed delta.
    size_t length = count - 1;
    stream.writeByte(static_cast<UInt8>((3 << 6) | ((length >> 8) & 1)));
    stream.writeByte(static_cast<UInt8>(length & 0xFF));
    writeVarint(encoded);
    writeSignedVarint(0);
}

void IntRleEncoderV2::writeDirect(const Int64 * values, size_t count)
{
    Int64 encoded[MAX_LITERAL_SIZE];
    UInt32 bit_width = 1;
    for (size_t i = 0; i < count; ++i)
    {
        encoded[i] = is_signed ? static_cast<Int64>(zigZag(values[i])) : values[i];
        bit_width = std::max(bit_width, findClosestNumBits(encoded[i]));
    }
    size_t length = count - 1;
    stream.writeByte(static_cast<UInt8>((1 << 6) | (encodeBitWidth(bit_width) << 1) | ((length >> 8) & 1)));
    stream.writeByte(static_cast<UInt8>(length & 0xFF));
    writeInts(encoded, count, bit_width);
}

namespace
{

/// The bit width that is enough for the given fraction `p` of the values (as in the reference implementation).
UInt32 percentileBits(const Int64 * values, size_t count, double p)
{
    UInt32 histogram[32] = {};
    for (size_t i = 0; i < count; ++i)
        ++histogram[encodeBitWidth(findClosestNumBits(values[i]))];

    Int64 per_len = static_cast<Int64>(static_cast<double>(count) * (1.0 - p));
    for (Int32 i = 31; i >= 0; --i)
    {
        per_len -= histogram[i];
        if (per_len < 0)
            return decodeBitWidth(i);
    }
    return 0;
}

}

void IntRleEncoderV2::encodeLiterals(const Int64 * values, size_t count)
{
    if (count <= 3)
    {
        writeDirect(values, count);
        return;
    }

    Int64 min = values[0];
    Int64 max = values[0];
    bool is_increasing = true;
    bool is_decreasing = true;
    bool is_fixed_delta = true;
    bool deltas_overflow = false;
    Int64 initial_delta = 0;
    if (__builtin_sub_overflow(values[1], values[0], &initial_delta))
        deltas_overflow = true;

    /// Absolute values of the deltas, starting from the second one.
    Int64 abs_deltas[MAX_LITERAL_SIZE];
    UInt64 max_abs_delta = 0;

    for (size_t i = 1; i < count; ++i)
    {
        min = std::min(min, values[i]);
        max = std::max(max, values[i]);
        is_increasing &= values[i - 1] <= values[i];
        is_decreasing &= values[i - 1] >= values[i];

        Int64 delta = 0;
        if (__builtin_sub_overflow(values[i], values[i - 1], &delta))
        {
            deltas_overflow = true;
            is_fixed_delta = false;
            continue;
        }
        is_fixed_delta &= (delta == initial_delta);
        if (i > 1)
        {
            UInt64 abs_delta = delta < 0 ? (0 - static_cast<UInt64>(delta)) : static_cast<UInt64>(delta);
            abs_deltas[i - 2] = static_cast<Int64>(abs_delta);
            max_abs_delta = std::max(max_abs_delta, abs_delta);
        }
    }

    Int64 range = 0;
    if (__builtin_sub_overflow(max, min, &range))
    {
        writeDirect(values, count);
        return;
    }

    if (!deltas_overflow && is_fixed_delta)
    {
        size_t length = count - 1;
        stream.writeByte(static_cast<UInt8>((3 << 6) | ((length >> 8) & 1)));
        stream.writeByte(static_cast<UInt8>(length & 0xFF));
        if (is_signed)
            writeSignedVarint(values[0]);
        else
            writeVarint(static_cast<UInt64>(values[0]));
        writeSignedVarint(initial_delta);
        return;
    }

    if (!deltas_overflow && initial_delta != 0 && (is_increasing || is_decreasing)
        && max_abs_delta <= static_cast<UInt64>(std::numeric_limits<Int64>::max()))
    {
        UInt32 bit_width = findClosestNumBits(static_cast<Int64>(max_abs_delta));
        /// The bit width code 0 means a fixed delta, so the width 1 is not representable.
        if (bit_width == 1)
            bit_width = 2;
        size_t length = count - 1;
        stream.writeByte(static_cast<UInt8>((3 << 6) | (encodeBitWidth(bit_width) << 1) | ((length >> 8) & 1)));
        stream.writeByte(static_cast<UInt8>(length & 0xFF));
        if (is_signed)
            writeSignedVarint(values[0]);
        else
            writeVarint(static_cast<UInt64>(values[0]));
        writeSignedVarint(initial_delta);
        writeInts(abs_deltas, count - 2, bit_width);
        return;
    }

    /// DIRECT or PATCHED_BASE.
    Int64 encoded[MAX_LITERAL_SIZE];
    for (size_t i = 0; i < count; ++i)
        encoded[i] = is_signed ? static_cast<Int64>(zigZag(values[i])) : values[i];

    UInt32 bits_100p = percentileBits(encoded, count, 1.0);
    UInt32 bits_90p = percentileBits(encoded, count, 0.9);

    static constexpr Int64 BASE_VALUE_LIMIT = 1LL << 56;
    if (bits_100p - bits_90p <= 1 || min <= -BASE_VALUE_LIMIT || min >= BASE_VALUE_LIMIT)
    {
        writeDirect(values, count);
        return;
    }

    Int64 base_reduced[MAX_LITERAL_SIZE];
    for (size_t i = 0; i < count; ++i)
        base_reduced[i] = values[i] - min;

    UInt32 br_bits_95p = percentileBits(base_reduced, count, 0.95);
    UInt32 br_bits_100p = percentileBits(base_reduced, count, 1.0);
    if (br_bits_100p == br_bits_95p)
    {
        writeDirect(values, count);
        return;
    }

    UInt32 patch_width = getClosestFixedBits(br_bits_100p - br_bits_95p);
    if (patch_width == 64)
    {
        patch_width = 56;
        br_bits_95p = 8;
    }
    const Int64 mask = static_cast<Int64>((1ULL << br_bits_95p) - 1);

    size_t gaps[MAX_LITERAL_SIZE];
    Int64 patches[MAX_LITERAL_SIZE];
    size_t num_patches = 0;
    size_t previous = 0;
    size_t max_gap = 0;
    for (size_t i = 0; i < count; ++i)
    {
        if (base_reduced[i] > mask)
        {
            size_t gap = i - previous;
            max_gap = std::max(max_gap, gap);
            previous = i;
            gaps[num_patches] = gap;
            patches[num_patches] = base_reduced[i] >> br_bits_95p;
            ++num_patches;
            base_reduced[i] &= mask;
        }
    }

    UInt32 patch_gap_width = (max_gap == 0) ? 1 : findClosestNumBits(static_cast<Int64>(max_gap));
    if (patch_gap_width > 8)
        patch_gap_width = 8;

    Int64 patch_list[MAX_LITERAL_SIZE];
    size_t patch_list_length = 0;
    for (size_t p = 0; p < num_patches; ++p)
    {
        size_t gap = gaps[p];
        while (gap > 255)
        {
            patch_list[patch_list_length++] = static_cast<Int64>(255ULL << patch_width);
            gap -= 255;
        }
        patch_list[patch_list_length++] = static_cast<Int64>((static_cast<UInt64>(gap) << patch_width) | static_cast<UInt64>(patches[p]));
    }

    /// The length of the patch list is stored in 5 bits.
    if (patch_list_length > 31)
    {
        writeDirect(values, count);
        return;
    }

    bool is_negative = min < 0;
    UInt64 base = is_negative ? static_cast<UInt64>(-min) : static_cast<UInt64>(min);
    UInt32 base_width = findClosestNumBits(static_cast<Int64>(base)) + 1;
    UInt32 base_bytes = (base_width + 7) / 8;
    if (is_negative)
        base |= 1ULL << (base_bytes * 8 - 1);

    size_t length = count - 1;
    stream.writeByte(static_cast<UInt8>((2 << 6) | (encodeBitWidth(br_bits_95p) << 1) | ((length >> 8) & 1)));
    stream.writeByte(static_cast<UInt8>(length & 0xFF));
    stream.writeByte(static_cast<UInt8>(((base_bytes - 1) << 5) | encodeBitWidth(patch_width)));
    stream.writeByte(static_cast<UInt8>(((patch_gap_width - 1) << 5) | patch_list_length));
    for (UInt32 b = base_bytes; b > 0; --b)
        stream.writeByte(static_cast<UInt8>(base >> ((b - 1) * 8)));
    writeInts(base_reduced, count, getClosestFixedBits(br_bits_95p));
    writeInts(patch_list, patch_list_length, getClosestFixedBits(patch_gap_width + patch_width));
}

}
