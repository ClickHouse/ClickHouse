#include "ProtobufWriter.h"

#if USE_PROTOBUF
#   include <IO/WriteHelpers.h>


namespace DB
{
namespace
{
    constexpr size_t MAX_VARINT_SIZE = 10;
    constexpr size_t REPEATED_PACK_PADDING = 2 * MAX_VARINT_SIZE;
    constexpr size_t NESTED_MESSAGE_PADDING = 2 * MAX_VARINT_SIZE;

    // Note: There is a difference between this function and writeVarUInt() from IO/VarInt.h:
    // Google protobuf's representation of 64-bit integer contains from 1 to 10 bytes,
    // whileas writeVarUInt() writes from 1 to 9 bytes because it omits the tenth byte (which is not necessary to decode actually).
    void writeVarint(UInt64 value, WriteBuffer & out)
    {
        while (value >= 0x80)
        {
            out.write(static_cast<char>(value | 0x80));
            value >>= 7;
        }
        out.write(static_cast<char>(value));
    }

    UInt8 * writeVarint(UInt64 value, UInt8 * ptr)
    {
        while (value >= 0x80)
        {
            *ptr++ = static_cast<UInt8>(value | 0x80);
            value >>= 7;
        }
        *ptr++ = static_cast<UInt8>(value);
        return ptr;
    }

    void writeVarint(UInt64 value, PODArray<UInt8> & buf)
    {
        size_t old_size = buf.size();
        buf.reserve(old_size + MAX_VARINT_SIZE);
        UInt8 * ptr = buf.data() + old_size;
        ptr = writeVarint(value, ptr);
        buf.resize_assume_reserved(ptr - buf.data());
    }

    UInt64 encodeZigZag(Int64 value) { return (static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63); }

    enum WireType
    {
        VARINT = 0,
        BITS64 = 1,
        LENGTH_DELIMITED = 2,
        GROUP_START = 3,
        GROUP_END = 4,
        BITS32 = 5
    };

    UInt8 * writeFieldNumber(UInt32 field_number, WireType wire_type, UInt8 * ptr)
    {
        return writeVarint((field_number << 3) | wire_type, ptr);
    }

    void writeFieldNumber(UInt32 field_number, WireType wire_type, PODArray<UInt8> & buf) { writeVarint((field_number << 3) | wire_type, buf); }
}


ProtobufWriter::ProtobufWriter(WriteBuffer & out_)
    : out(out_)
{
}

ProtobufWriter::~ProtobufWriter() = default;

void ProtobufWriter::startMessage()
{
}

void ProtobufWriter::endMessage(bool with_length_delimiter)
{
    pieces.emplace_back(current_piece_start, buffer.size());
    if (with_length_delimiter)
    {
        size_t size_of_message = buffer.size() - num_bytes_skipped;
        writeVarint(size_of_message, out);
    }
    for (const auto & piece : pieces)
        if (piece.end > piece.start)
            out.write(reinterpret_cast<char *>(&buffer[piece.start]), piece.end - piece.start);
    buffer.clear();
    pieces.clear();
    num_bytes_skipped = 0;
    current_piece_start = 0;
}

void ProtobufWriter::startNestedMessage()
{
    nested_infos.emplace_back(pieces.size(), num_bytes_skipped);
    pieces.emplace_back(current_piece_start, buffer.size());

    // We skip enough bytes to have place for inserting the field number and the size of the nested message afterwards
    // when we finish writing the nested message itself. We don't know the size of the nested message at the point of
    // calling startNestedMessage(), that's why we have to do this skipping.
    current_piece_start = buffer.size() + NESTED_MESSAGE_PADDING;
    buffer.resize(current_piece_start);
    num_bytes_skipped = NESTED_MESSAGE_PADDING;
}

void ProtobufWriter::endNestedMessage(int field_number, bool is_group, bool skip_if_empty)
{
    const auto & nested_info = nested_infos.back();
    size_t num_pieces_at_start = nested_info.num_pieces_at_start;
    size_t num_bytes_skipped_at_start = nested_info.num_bytes_skipped_at_start;
    nested_infos.pop_back();
    auto & piece_before_message = pieces[num_pieces_at_start];
    size_t message_start = piece_before_message.end;
    size_t message_size = buffer.size() - message_start - num_bytes_skipped;
    if (!message_size && skip_if_empty)
    {
        current_piece_start = piece_before_message.start;
        buffer.resize(piece_before_message.end);
        pieces.resize(num_pieces_at_start);
        num_bytes_skipped = num_bytes_skipped_at_start;
        return;
    }
    size_t num_bytes_inserted;
    if (is_group)
    {
        writeFieldNumber(field_number, GROUP_END, buffer);
        UInt8 * ptr = &buffer[piece_before_message.end];
        UInt8 * endptr = writeFieldNumber(field_number, GROUP_START, ptr);
        num_bytes_inserted = endptr - ptr;
    }
    else
    {
        UInt8 * ptr = &buffer[piece_before_message.end];
        UInt8 * endptr = writeFieldNumber(field_number, LENGTH_DELIMITED, ptr);
        endptr = writeVarint(message_size, endptr);
        num_bytes_inserted = endptr - ptr;
    }
    piece_before_message.end += num_bytes_inserted;
    num_bytes_skipped += num_bytes_skipped_at_start - num_bytes_inserted;
}

void ProtobufWriter::writeUInt(int field_number, UInt64 value)
{
    if (in_repeated_pack)
    {
        writeVarint(value, buffer);
        return;
    }
    size_t old_size = buffer.size();
    buffer.reserve(old_size + 2 * MAX_VARINT_SIZE);
    UInt8 * ptr = buffer.data() + old_size;
    ptr = writeFieldNumber(field_number, VARINT, ptr);
    ptr = writeVarint(value, ptr);
    buffer.resize_assume_reserved(ptr - buffer.data());
}

void ProtobufWriter::writeInt(int field_number, Int64 value)
{
    writeUInt(field_number, static_cast<UInt64>(value));
}

void ProtobufWriter::writeSInt(int field_number, Int64 value)
{
    writeUInt(field_number, encodeZigZag(value));
}

template <typename T>
void ProtobufWriter::writeFixed(int field_number, T value)
{
    static_assert((sizeof(T) == 4) || (sizeof(T) == 8));
    if (in_repeated_pack)
    {
        size_t old_size = buffer.size();
        buffer.resize(old_size + sizeof(T));
        memcpy(buffer.data() + old_size, &value, sizeof(T));
        return;
    }
    constexpr WireType wire_type = (sizeof(T) == 4) ? BITS32 : BITS64;
    size_t old_size = buffer.size();
    buffer.reserve(old_size + MAX_VARINT_SIZE + sizeof(T));
    UInt8 * ptr = buffer.data() + old_size;
    ptr = writeFieldNumber(field_number, wire_type, ptr);
    memcpy(ptr, &value, sizeof(T));
    ptr += sizeof(T);
    buffer.resize_assume_reserved(ptr - buffer.data());
}

template void ProtobufWriter::writeFixed<Int32>(int field_number, Int32 value);
template void ProtobufWriter::writeFixed<UInt32>(int field_number, UInt32 value);
template void ProtobufWriter::writeFixed<Int64>(int field_number, Int64 value);
template void ProtobufWriter::writeFixed<UInt64>(int field_number, UInt64 value);
template void ProtobufWriter::writeFixed<Float32>(int field_number, Float32 value);
template void ProtobufWriter::writeFixed<Float64>(int field_number, Float64 value);

void ProtobufWriter::writeString(int field_number, std::string_view str)
{
    size_t length = str.length();
    size_t old_size = buffer.size();
    buffer.reserve(old_size + 2 * MAX_VARINT_SIZE + length);
    UInt8 * ptr = buffer.data() + old_size;
    ptr = writeFieldNumber(field_number, LENGTH_DELIMITED, ptr);
    ptr = writeVarint(length, ptr);
    memcpy(ptr, str.data(), length);
    ptr += length;
    buffer.resize_assume_reserved(ptr - buffer.data());
}

void ProtobufWriter::startRepeatedPack()
{
    pieces.emplace_back(current_piece_start, buffer.size());

    // We skip enough bytes to have place for inserting the field number and the size of the repeated pack afterwards
    // when we finish writing the repeated pack itself. We don't know the size of the repeated pack at the point of
    // calling startRepeatedPack(), that's why we have to do this skipping.
    current_piece_start = buffer.size() + REPEATED_PACK_PADDING;
    buffer.resize(current_piece_start);
    num_bytes_skipped += REPEATED_PACK_PADDING;
    in_repeated_pack = true;
}

void ProtobufWriter::endRepeatedPack(int field_number, bool skip_if_empty)
{
    size_t size = buffer.size() - current_piece_start;
    if (!size && skip_if_empty)
    {
        current_piece_start = pieces.back().start;
        buffer.resize(pieces.back().end);
        pieces.pop_back();
        num_bytes_skipped -= REPEATED_PACK_PADDING;
        in_repeated_pack = false;
        return;
    }
    UInt8 * ptr = &buffer[pieces.back().end];
    UInt8 * endptr = writeFieldNumber(field_number, LENGTH_DELIMITED, ptr);
    endptr = writeVarint(size, endptr);
    size_t num_bytes_inserted = endptr - ptr;
    pieces.back().end += num_bytes_inserted;
    num_bytes_skipped -= num_bytes_inserted;
    in_repeated_pack = false;
}

}

#endif
