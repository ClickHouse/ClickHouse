#include <cassert>
#include <Formats/ProtobufSimpleWriter.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace
{
    // Note: We cannot simply use writeVarUInt() from IO/VarInt.h here because there is one small difference:
    // Google protobuf's representation of 64-bit integer contains from 1 to 10 bytes, whileas writeVarUInt() writes from 1 to 9 bytes
    // because it omits the tenth byte (which is not necessary to decode actually).
    void writePbVarUInt(UInt64 value, WriteBuffer & buf)
    {
        while (value >= 0x80)
        {
            buf.write(static_cast<char>(value | 0x80));
            value >>= 7;
        }
        buf.write(static_cast<char>(value));
    }

    void writePbVarInt(Int64 value, WriteBuffer & buf)
    {
        writePbVarUInt((static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63), buf);
    }

    void writePbVarIntNoZigZagEncoding(Int64 value, WriteBuffer & buf) { writePbVarUInt(static_cast<UInt64>(value), buf); }
}


enum ProtobufSimpleWriter::WireType : UInt32
{
    VARINT = 0,
    BITS64 = 1,
    LENGTH_DELIMITED = 2,
    BITS32 = 5
};

ProtobufSimpleWriter::ProtobufSimpleWriter(WriteBuffer & out_) : out(out_)
{
}

ProtobufSimpleWriter::~ProtobufSimpleWriter()
{
    finishCurrentMessage();
}

void ProtobufSimpleWriter::newMessage()
{
    finishCurrentMessage();
    were_messages = true;
}

void ProtobufSimpleWriter::finishCurrentMessage()
{
    if (!were_messages)
        return;
    finishCurrentField();
    current_field_number = 0;
    StringRef str = message_buffer.stringRef();
    writePbVarUInt(str.size, out);
    out.write(str.data, str.size);
    message_buffer.restart();
}

void ProtobufSimpleWriter::setCurrentField(UInt32 field_number)
{
    finishCurrentField();
    assert(current_field_number < field_number);
    current_field_number = field_number;
    num_normal_values = 0;
    num_packed_values = 0;
}

void ProtobufSimpleWriter::finishCurrentField()
{
    if (num_packed_values)
    {
        assert(!num_normal_values);
        StringRef str = repeated_packing_buffer.stringRef();
        if (str.size)
        {
            writeKey(LENGTH_DELIMITED, message_buffer);
            writePbVarUInt(str.size, message_buffer);
            message_buffer.write(str.data, str.size);
            repeated_packing_buffer.restart();
        }
    }
}

void ProtobufSimpleWriter::writeKey(WireType wire_type, WriteBuffer & buf)
{
    writePbVarUInt((current_field_number << 3) | wire_type, buf);
}

void ProtobufSimpleWriter::writeInt32(Int32 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarIntNoZigZagEncoding(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeUInt32(UInt32 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarUInt(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeSInt32(Int32 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarInt(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeInt64(Int64 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarIntNoZigZagEncoding(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeUInt64(UInt64 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarUInt(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeSInt64(Int64 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarInt(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeFixed32(UInt32 value)
{
    assert(current_field_number);
    writeKey(BITS32, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeSFixed32(Int32 value)
{
    assert(current_field_number);
    writeKey(BITS32, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeFloat(float value)
{
    assert(current_field_number);
    writeKey(BITS32, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeFixed64(UInt64 value)
{
    assert(current_field_number);
    writeKey(BITS64, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeSFixed64(Int64 value)
{
    assert(current_field_number);
    writeKey(BITS64, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeDouble(double value)
{
    assert(current_field_number);
    writeKey(BITS64, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeString(const StringRef & str)
{
    assert(current_field_number);
    ++num_normal_values;
    writeKey(LENGTH_DELIMITED, message_buffer);
    writePbVarUInt(str.size, message_buffer);
    message_buffer.write(str.data, str.size);
}

void ProtobufSimpleWriter::writeInt32IfNonZero(Int32 value)
{
    if (value)
        writeInt32(value);
}

void ProtobufSimpleWriter::writeUInt32IfNonZero(UInt32 value)
{
    if (value)
        writeUInt32(value);
}

void ProtobufSimpleWriter::writeSInt32IfNonZero(Int32 value)
{
    if (value)
        writeSInt32(value);
}

void ProtobufSimpleWriter::writeInt64IfNonZero(Int64 value)
{
    if (value)
        writeInt64(value);
}

void ProtobufSimpleWriter::writeUInt64IfNonZero(UInt64 value)
{
    if (value)
        writeUInt64(value);
}

void ProtobufSimpleWriter::writeSInt64IfNonZero(Int64 value)
{
    if (value)
        writeSInt64(value);
}

void ProtobufSimpleWriter::writeFixed32IfNonZero(UInt32 value)
{
    if (value)
        writeFixed32(value);
}

void ProtobufSimpleWriter::writeSFixed32IfNonZero(Int32 value)
{
    if (value)
        writeSFixed32(value);
}

void ProtobufSimpleWriter::writeFloatIfNonZero(float value)
{
    if (value != 0)
        writeFloat(value);
}

void ProtobufSimpleWriter::writeFixed64IfNonZero(UInt64 value)
{
    if (value)
        writeFixed64(value);
}

void ProtobufSimpleWriter::writeSFixed64IfNonZero(Int64 value)
{
    if (value)
        writeSFixed64(value);
}

void ProtobufSimpleWriter::writeDoubleIfNonZero(double value)
{
    if (value != 0)
        writeDouble(value);
}

void ProtobufSimpleWriter::writeStringIfNotEmpty(const StringRef & str)
{
    if (str.size)
        writeString(str);
}

void ProtobufSimpleWriter::packRepeatedInt32(Int32 value)
{
    assert(current_field_number);
    writePbVarIntNoZigZagEncoding(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedUInt32(UInt32 value)
{
    assert(current_field_number);
    writePbVarUInt(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedSInt32(Int32 value)
{
    assert(current_field_number);
    writePbVarInt(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedInt64(Int64 value)
{
    assert(current_field_number);
    writePbVarIntNoZigZagEncoding(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedUInt64(UInt64 value)
{
    assert(current_field_number);
    writePbVarUInt(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedSInt64(Int64 value)
{
    assert(current_field_number);
    writePbVarInt(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedFixed32(UInt32 value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedSFixed32(Int32 value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedFloat(float value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedFixed64(UInt64 value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedSFixed64(Int64 value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedDouble(double value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

}
