#pragma once

#include <Processors/Formats/Impl/ORC/Stream.h>

#include <memory>
#include <vector>

/// Run-length encodings of ORC: https://orc.apache.org/specification/ORCv1/#run-length-encoding
///
/// All decoders fill the positions where `not_null` is non-zero (or all positions if it is
/// nullptr), consuming one value from the stream for each of them; the other positions are set to zero.

namespace DB::ORC
{

/// Byte run-length encoding, used for the bytes and for the booleans (packed into bytes).
class ByteRleDecoder
{
public:
    explicit ByteRleDecoder(std::unique_ptr<InputByteStream> stream_) : stream(std::move(stream_)) {}

    void next(char * data, size_t num_values, const char * not_null);
    void skip(size_t num_values);
    void seek(PositionProvider & positions);

private:
    void readHeader();

    std::unique_ptr<InputByteStream> stream;
    size_t remaining_values = 0;
    bool repeating = false;
    char value = 0;
};

/// Booleans packed into bytes, most significant bit first, encoded with the byte run-length encoding.
class BooleanRleDecoder
{
public:
    explicit BooleanRleDecoder(std::unique_ptr<InputByteStream> stream_) : byte_decoder(std::move(stream_)) {}

    /// Writes 0 or 1.
    void next(char * data, size_t num_values, const char * not_null);
    void skip(size_t num_values);
    void seek(PositionProvider & positions);

private:
    ByteRleDecoder byte_decoder;
    size_t remaining_bits = 0;
    UInt8 last_byte = 0;
};

/// Integer run-length encodings, version 1 and 2.
class IntRleDecoder
{
public:
    virtual ~IntRleDecoder() = default;
    /// The values are converted to the type of the output, which can be narrower than the encoded values.
    virtual void next(Int64 * data, size_t num_values, const char * not_null) = 0;
    virtual void next(UInt64 * data, size_t num_values, const char * not_null) = 0;
    virtual void next(Int32 * data, size_t num_values, const char * not_null) = 0;
    virtual void next(Int16 * data, size_t num_values, const char * not_null) = 0;
    virtual void skip(size_t num_values) = 0;
    virtual void seek(PositionProvider & positions) = 0;
};

enum class RleVersion : UInt8
{
    V1,
    V2,
};

std::unique_ptr<IntRleDecoder> createIntRleDecoder(std::unique_ptr<InputByteStream> stream, bool is_signed, RleVersion version);

/// Reads a base 128 varint, as used by the DECIMAL columns.
UInt64 readVarint(InputByteStream & stream);
Int64 readSignedVarint(InputByteStream & stream);


class ByteRleEncoder
{
public:
    explicit ByteRleEncoder(OutputByteStream & stream_) : stream(stream_) {}

    void write(char value);
    void recordPosition(std::vector<UInt64> & positions) const;
    /// Writes out the buffered values.
    void flush();

private:
    void writeValues();

    static constexpr size_t MIN_REPEAT = 3;
    static constexpr size_t MAX_LITERAL_SIZE = 128;
    static constexpr size_t MAX_REPEAT_SIZE = 127 + MIN_REPEAT;

    OutputByteStream & stream;
    char literals[MAX_LITERAL_SIZE]{};
    size_t num_literals = 0;
    size_t tail_run_length = 0;
    bool repeat = false;
};

class BooleanRleEncoder
{
public:
    explicit BooleanRleEncoder(OutputByteStream & stream_) : byte_encoder(stream_) {}

    void write(bool value);
    void recordPosition(std::vector<UInt64> & positions) const;
    void flush();

private:
    ByteRleEncoder byte_encoder;
    UInt8 current = 0;
    size_t bits_in_current = 0;
};

/// Integer run-length encoding, version 2.
class IntRleEncoderV2
{
public:
    IntRleEncoderV2(OutputByteStream & stream_, bool is_signed_) : stream(stream_), is_signed(is_signed_) {}

    void write(Int64 value)
    {
        literals[num_literals++] = value;
        if (num_literals == MAX_LITERAL_SIZE)
            flush();
    }

    void recordPosition(std::vector<UInt64> & positions) const;
    /// Writes out the buffered values.
    void flush();

    static constexpr size_t MAX_LITERAL_SIZE = 512;

private:
    void encodeLiterals(const Int64 * values, size_t count);
    void encodeRepeat(Int64 value, size_t count);
    void writeDirect(const Int64 * values, size_t count);
    void writeVarint(UInt64 value);
    void writeSignedVarint(Int64 value) { writeVarint((static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63)); }
    void writeInts(const Int64 * values, size_t count, UInt32 bit_width);

    OutputByteStream & stream;
    const bool is_signed;
    Int64 literals[MAX_LITERAL_SIZE]{};
    size_t num_literals = 0;
};

}
