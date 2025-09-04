#include "ProtobufReader.h"

#if USE_PROTOBUF
#   include <IO/ReadHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_PROTOBUF_FORMAT;
}


namespace
{
    enum WireType
    {
        VARINT = 0,
        BITS64 = 1,
        LENGTH_DELIMITED = 2,
        GROUP_START = 3,
        GROUP_END = 4,
        BITS32 = 5,
    };

    // The following conditions must always be true:
    // any_cursor_position > END_OF_VARINT
    // any_cursor_position > END_OF_GROUP
    // Those inequations helps checking conditions in ProtobufReader::SimpleReader.
    constexpr Int64 END_OF_VARINT = -1;
    constexpr Int64 END_OF_GROUP = -2;
    constexpr Int64 END_OF_FILE = -3;

    Int64 decodeZigZag(UInt64 n) { return static_cast<Int64>((n >> 1) ^ (~(n & 1) + 1)); }
}


ProtobufReader::ProtobufReader(ReadBuffer & in_)
    : in(&in_)
{
}

void ProtobufReader::startMessage(bool with_length_delimiter_)
{
    // Start reading a root message.
    assert(!current_message_level);

    root_message_has_length_delimiter = with_length_delimiter_;
    if (root_message_has_length_delimiter)
    {
        size_t size_of_message = readVarint();
        current_message_end = cursor + size_of_message;
    }
    else
    {
        current_message_end = END_OF_FILE;
    }
    ++current_message_level;
    field_number = next_field_number = 0;
    field_end = cursor;
}

void ProtobufReader::endMessage(bool ignore_errors)
{
    if (!current_message_level)
        return;

    Int64 root_message_end = (current_message_level == 1) ? current_message_end : parent_message_ends.front();
    if (cursor != root_message_end)
    {
        if (cursor < root_message_end)
            ignore(root_message_end - cursor);
        else if (root_message_end == END_OF_FILE)
            ignoreAll();
        else if (ignore_errors)
            moveCursorBackward(cursor - root_message_end);
        else
            throwUnknownFormat();
    }

    current_message_level = 0;
    parent_message_ends.clear();
}

void ProtobufReader::startNestedMessage()
{
    assert(current_message_level >= 1);
    if ((cursor > field_end) && (field_end != END_OF_GROUP))
        throwUnknownFormat();

    // Start reading a nested message which is located inside a length-delimited field
    // of another message.
    parent_message_ends.emplace_back(current_message_end);
    current_message_end = field_end;
    ++current_message_level;
    field_number = next_field_number = 0;
    field_end = cursor;
}

void ProtobufReader::endNestedMessage()
{
    assert(current_message_level >= 2);
    if (cursor != current_message_end)
    {
        if (current_message_end == END_OF_GROUP)
        {
            ignoreGroup();
            current_message_end = cursor;
        }
        else if (cursor < current_message_end)
            ignore(current_message_end - cursor);
        else
            throwUnknownFormat();
    }

    --current_message_level;
    current_message_end = parent_message_ends.back();
    parent_message_ends.pop_back();
    field_number = next_field_number = 0;
    field_end = cursor;
}

bool ProtobufReader::readFieldNumber(int & field_number_)
{
    assert(current_message_level);
    if (next_field_number)
    {
        field_number_ = field_number = next_field_number;
        next_field_number = 0;
        return true;
    }

    if (field_end != cursor)
    {
        if (field_end == END_OF_VARINT)
        {
            ignoreVarint();
            field_end = cursor;
        }
        else if (field_end == END_OF_GROUP)
        {
            ignoreGroup();
            field_end = cursor;
        }
        else if (cursor < field_end)
            ignore(field_end - cursor);
        else
            throwUnknownFormat();
    }

    if (cursor >= current_message_end)
    {
        if (current_message_end == END_OF_FILE)
        {
            if (unlikely(in->eof()))
            {
                current_message_end = cursor;
                return false;
            }
        }
        else if (current_message_end == END_OF_GROUP)
        {
            /// We'll check for the `GROUP_END` marker later.
        }
        else
            return false;
    }

    UInt64 varint = readVarint();
    if (unlikely(varint & (static_cast<UInt64>(0xFFFFFFFF) << 32)))
        throwUnknownFormat();
    UInt32 key = static_cast<UInt32>(varint);
    field_number_ = field_number = (key >> 3);
    next_field_number = 0;
    WireType wire_type = static_cast<WireType>(key & 0x07);
    switch (wire_type)
    {
        case BITS32:
        {
            field_end = cursor + 4;
            return true;
        }
        case BITS64:
        {
            field_end = cursor + 8;
            return true;
        }
        case LENGTH_DELIMITED:
        {
            size_t length = readVarint();
            field_end = cursor + length;
            return true;
        }
        case VARINT:
        {
            field_end = END_OF_VARINT;
            return true;
        }
        case GROUP_START:
        {
            field_end = END_OF_GROUP;
            return true;
        }
        case GROUP_END:
        {
            if (current_message_end != END_OF_GROUP)
                throwUnknownFormat();
            current_message_end = cursor;
            return false;
        }
    }
    throwUnknownFormat();
}

UInt64 ProtobufReader::readUInt()
{
    UInt64 value;
    if (field_end == END_OF_VARINT)
    {
        value = readVarint();
        field_end = cursor;
    }
    else
    {
        value = readVarint();
        if (cursor < field_end)
            next_field_number = field_number;
        else if (unlikely(cursor) > field_end)
            throwUnknownFormat();
    }
    return value;
}

Int64 ProtobufReader::readInt()
{
    return static_cast<Int64>(readUInt());
}

Int64 ProtobufReader::readSInt()
{
    return decodeZigZag(readUInt());
}

template<typename T>
T ProtobufReader::readFixed()
{
    if (unlikely(cursor + static_cast<Int64>(sizeof(T)) > field_end))
        throwUnknownFormat();
    T value;
    readBinary(&value, sizeof(T));
    if (cursor < field_end)
        next_field_number = field_number;
    return value;
}

template Int32 ProtobufReader::readFixed<Int32>();
template UInt32 ProtobufReader::readFixed<UInt32>();
template Int64 ProtobufReader::readFixed<Int64>();
template UInt64 ProtobufReader::readFixed<UInt64>();
template Float32 ProtobufReader::readFixed<Float32>();
template Float64 ProtobufReader::readFixed<Float64>();

void ProtobufReader::readString(String & str)
{
    if (unlikely(cursor > field_end))
        throwUnknownFormat();
    size_t length = field_end - cursor;
    str.resize(length);
    readBinary(reinterpret_cast<char*>(str.data()), length);
}

void ProtobufReader::readStringAndAppend(PaddedPODArray<UInt8> & str)
{
    if (unlikely(cursor > field_end))
        throwUnknownFormat();
    size_t length = field_end - cursor;
    size_t old_size = str.size();
    str.resize(old_size + length);
    readBinary(reinterpret_cast<char*>(str.data() + old_size), length);
}

void ProtobufReader::readBinary(void* data, size_t size)
{
    in->readStrict(reinterpret_cast<char*>(data), size);
    cursor += size;
}

void ProtobufReader::ignore(UInt64 num_bytes)
{
    in->ignore(num_bytes);
    cursor += num_bytes;
}

void ProtobufReader::ignoreAll()
{
    cursor += in->tryIgnore(std::numeric_limits<size_t>::max());
}

void ProtobufReader::moveCursorBackward(UInt64 num_bytes)
{
    if (in->offset() < num_bytes)
        throwUnknownFormat();
    in->position() -= num_bytes;
    cursor -= num_bytes;
}

UInt64 ProtobufReader::continueReadingVarint(UInt64 first_byte)
{
    UInt64 result = (first_byte & ~static_cast<UInt64>(0x80));
    char c;

#    define PROTOBUF_READER_READ_VARINT_BYTE(byteNo) \
        do \
        { \
            in->readStrict(c); \
            ++cursor; \
            if constexpr ((byteNo) < 10) \
            { \
                result |= static_cast<UInt64>(static_cast<UInt8>(c)) << (7 * ((byteNo)-1)); \
                if (likely(!(c & 0x80))) \
                    return result; \
            } \
            else \
            { \
                if (likely(c == 1)) \
                    return result; \
            } \
            if constexpr ((byteNo) < 9) \
                result &= ~(static_cast<UInt64>(0x80) << (7 * ((byteNo)-1))); \
        } while (false)

    PROTOBUF_READER_READ_VARINT_BYTE(2);
    PROTOBUF_READER_READ_VARINT_BYTE(3);
    PROTOBUF_READER_READ_VARINT_BYTE(4);
    PROTOBUF_READER_READ_VARINT_BYTE(5);
    PROTOBUF_READER_READ_VARINT_BYTE(6);
    PROTOBUF_READER_READ_VARINT_BYTE(7);
    PROTOBUF_READER_READ_VARINT_BYTE(8);
    PROTOBUF_READER_READ_VARINT_BYTE(9);
    PROTOBUF_READER_READ_VARINT_BYTE(10);

#    undef PROTOBUF_READER_READ_VARINT_BYTE

    throwUnknownFormat();
}

void ProtobufReader::ignoreVarint()
{
    char c;

#    define PROTOBUF_READER_IGNORE_VARINT_BYTE(byteNo) \
        do \
        { \
            in->readStrict(c); \
            ++cursor; \
            if constexpr ((byteNo) < 10) \
            { \
                if (likely(!(c & 0x80))) \
                    return; \
            } \
            else \
            { \
                if (likely(c == 1)) \
                    return; \
            } \
        } while (false)

    PROTOBUF_READER_IGNORE_VARINT_BYTE(1);
    PROTOBUF_READER_IGNORE_VARINT_BYTE(2);
    PROTOBUF_READER_IGNORE_VARINT_BYTE(3);
    PROTOBUF_READER_IGNORE_VARINT_BYTE(4);
    PROTOBUF_READER_IGNORE_VARINT_BYTE(5);
    PROTOBUF_READER_IGNORE_VARINT_BYTE(6);
    PROTOBUF_READER_IGNORE_VARINT_BYTE(7);
    PROTOBUF_READER_IGNORE_VARINT_BYTE(8);
    PROTOBUF_READER_IGNORE_VARINT_BYTE(9);
    PROTOBUF_READER_IGNORE_VARINT_BYTE(10);

#    undef PROTOBUF_READER_IGNORE_VARINT_BYTE

    throwUnknownFormat();
}

void ProtobufReader::ignoreGroup()
{
    size_t level = 1;
    while (true)
    {
        UInt64 varint = readVarint();
        WireType wire_type = static_cast<WireType>(varint & 0x07);
        switch (wire_type)
        {
            case VARINT:
            {
                ignoreVarint();
                break;
            }
            case BITS64:
            {
                ignore(8);
                break;
            }
            case LENGTH_DELIMITED:
            {
                ignore(readVarint());
                break;
            }
            case GROUP_START:
            {
                ++level;
                break;
            }
            case GROUP_END:
            {
                if (!--level)
                    return;
                break;
            }
            case BITS32:
            {
                ignore(4);
                break;
            }
        }
        throwUnknownFormat();
    }
}

[[noreturn]] void ProtobufReader::throwUnknownFormat() const
{
    throw Exception(ErrorCodes::UNKNOWN_PROTOBUF_FORMAT, "Protobuf messages are corrupted or don't match the provided schema.{}",
            root_message_has_length_delimiter
            ? " Please note that Protobuf stream is length-delimited: every message is prefixed by its length in varint."
            : "");
}
}

#endif
