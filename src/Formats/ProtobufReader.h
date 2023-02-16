#pragma once

#include "config_formats.h"

#if USE_PROTOBUF
#   include <Common/PODArray.h>
#   include <IO/ReadBuffer.h>


namespace DB
{
class ReadBuffer;

/// Utility class for reading in the Protobuf format.
/// Knows nothing about protobuf schemas, just provides useful functions to serialize data.
class ProtobufReader
{
public:
    explicit ProtobufReader(ReadBuffer & in_);

    void startMessage(bool with_length_delimiter_);
    void endMessage(bool ignore_errors);
    void startNestedMessage();
    void endNestedMessage();

    bool readFieldNumber(int & field_number);
    Int64 readInt();
    Int64 readSInt();
    UInt64 readUInt();
    template<typename T> T readFixed();

    void readString(String & str);
    void readStringAndAppend(PaddedPODArray<UInt8> & str);

    bool eof() const { return in.eof(); }

private:
    void readBinary(void * data, size_t size);
    void ignore(UInt64 num_bytes);
    void ignoreAll();
    void moveCursorBackward(UInt64 num_bytes);

    UInt64 ALWAYS_INLINE readVarint()
    {
        char c;
        in.readStrict(c);
        UInt64 first_byte = static_cast<UInt8>(c);
        ++cursor;
        if (likely(!(c & 0x80)))
            return first_byte;
        return continueReadingVarint(first_byte);
    }

    UInt64 continueReadingVarint(UInt64 first_byte);
    void ignoreVarint();
    void ignoreGroup();
    [[noreturn]] void throwUnknownFormat() const;

    ReadBuffer & in;
    Int64 cursor = 0;
    bool root_message_has_length_delimiter = false;
    size_t current_message_level = 0;
    Int64 current_message_end = 0;
    std::vector<Int64> parent_message_ends;
    int field_number = 0;
    int next_field_number = 0;
    Int64 field_end = 0;
};

}
#endif
