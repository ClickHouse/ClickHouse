#include <cassert>
#include <Formats/ProtobufSimpleWriter.h>
#include <Poco/ByteOrder.h>


namespace DB
{
namespace
{
    void writeBytes(WriteBuffer & buf, const void * data, size_t size) { buf.write(reinterpret_cast<const char *>(data), size); }

    void writeVariant(WriteBuffer & buf, UInt32 value)
    {
        while (value >= 0x80)
        {
            buf.write(static_cast<char>(value | 0x80));
            value >>= 7;
        }
        buf.write(static_cast<char>(value));
    }

    void writeVariant(WriteBuffer & buf, Int32 value) { writeVariant(buf, static_cast<UInt32>(value)); }

    void writeVariant(WriteBuffer & buf, UInt64 value)
    {
        while (value >= 0x80)
        {
            buf.write(static_cast<char>(value | 0x80));
            value >>= 7;
        }
        buf.write(static_cast<char>(value));
    }

    void writeVariant(WriteBuffer & buf, Int64 value) { writeVariant(buf, static_cast<UInt64>(value)); }

    void writeLittleEndian(WriteBuffer & buf, UInt32 value)
    {
        value = Poco::ByteOrder::toLittleEndian(value);
        writeBytes(buf, &value, sizeof(value));
    }

    void writeLittleEndian(WriteBuffer & buf, Int32 value) { writeLittleEndian(buf, static_cast<UInt32>(value)); }

    void writeLittleEndian(WriteBuffer & buf, float value)
    {
        union
        {
            Float32 f;
            UInt32 i;
        };
        f = value;
        writeLittleEndian(buf, i);
    }

    void writeLittleEndian(WriteBuffer & buf, UInt64 value)
    {
        value = Poco::ByteOrder::toLittleEndian(value);
        writeBytes(buf, &value, sizeof(value));
    }

    void writeLittleEndian(WriteBuffer & buf, Int64 value) { writeLittleEndian(buf, static_cast<UInt64>(value)); }

    void writeLittleEndian(WriteBuffer & buf, double value)
    {
        union
        {
            Float64 f;
            UInt64 i;
        };
        f = value;
        writeLittleEndian(buf, i);
    }

    UInt32 zigZag(Int32 value) { return (static_cast<UInt32>(value) << 1) ^ static_cast<UInt32>(value >> 31); }
    UInt64 zigZag(Int64 value) { return (static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63); }

}


enum ProtobufSimpleWriter::WireType : UInt32
{
    VARIANT = 0,
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
    writeVariant(out, str.size);
    writeBytes(out, str.data, str.size);
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
            writeKey(message_buffer, LENGTH_DELIMITED);
            writeVariant(message_buffer, str.size);
            writeBytes(message_buffer, str.data, str.size);
            repeated_packing_buffer.restart();
        }
    }
}

void ProtobufSimpleWriter::writeKey(WriteBuffer & buf, WireType wire_type)
{
    writeVariant(buf, (current_field_number << 3) | wire_type);
}

void ProtobufSimpleWriter::writeInt32(Int32 value)
{
    assert(current_field_number);
    writeKey(message_buffer, VARIANT);
    writeVariant(message_buffer, value);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeUInt32(UInt32 value)
{
    assert(current_field_number);
    writeKey(message_buffer, VARIANT);
    writeVariant(message_buffer, value);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeSInt32(Int32 value)
{
    assert(current_field_number);
    writeKey(message_buffer, VARIANT);
    writeVariant(message_buffer, zigZag(value));
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeInt64(Int64 value)
{
    assert(current_field_number);
    writeKey(message_buffer, VARIANT);
    writeVariant(message_buffer, value);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeUInt64(UInt64 value)
{
    assert(current_field_number);
    writeKey(message_buffer, VARIANT);
    writeVariant(message_buffer, value);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeSInt64(Int64 value)
{
    assert(current_field_number);
    writeKey(message_buffer, VARIANT);
    writeVariant(message_buffer, zigZag(value));
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeFixed32(UInt32 value)
{
    assert(current_field_number);
    writeKey(message_buffer, BITS32);
    writeLittleEndian(message_buffer, value);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeSFixed32(Int32 value)
{
    assert(current_field_number);
    writeKey(message_buffer, BITS32);
    writeLittleEndian(message_buffer, value);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeFloat(float value)
{
    assert(current_field_number);
    writeKey(message_buffer, BITS32);
    writeLittleEndian(message_buffer, value);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeFixed64(UInt64 value)
{
    assert(current_field_number);
    writeKey(message_buffer, BITS64);
    writeLittleEndian(message_buffer, value);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeSFixed64(Int64 value)
{
    assert(current_field_number);
    writeKey(message_buffer, BITS64);
    writeLittleEndian(message_buffer, value);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeDouble(double value)
{
    assert(current_field_number);
    writeKey(message_buffer, BITS64);
    writeLittleEndian(message_buffer, value);
    ++num_normal_values;
}

void ProtobufSimpleWriter::writeString(const StringRef & str)
{
    assert(current_field_number);
    ++num_normal_values;
    writeKey(message_buffer, LENGTH_DELIMITED);
    writeVariant(message_buffer, str.size);
    writeBytes(message_buffer, str.data, str.size);
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
    writeVariant(repeated_packing_buffer, value);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedUInt32(UInt32 value)
{
    assert(current_field_number);
    writeVariant(repeated_packing_buffer, value);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedSInt32(Int32 value)
{
    assert(current_field_number);
    writeVariant(repeated_packing_buffer, zigZag(value));
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedInt64(Int64 value)
{
    assert(current_field_number);
    writeVariant(repeated_packing_buffer, value);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedUInt64(UInt64 value)
{
    assert(current_field_number);
    writeVariant(repeated_packing_buffer, value);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedSInt64(Int64 value)
{
    assert(current_field_number);
    writeVariant(repeated_packing_buffer, zigZag(value));
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedFixed32(UInt32 value)
{
    assert(current_field_number);
    writeLittleEndian(repeated_packing_buffer, value);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedSFixed32(Int32 value)
{
    assert(current_field_number);
    writeLittleEndian(repeated_packing_buffer, value);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedFloat(float value)
{
    assert(current_field_number);
    writeLittleEndian(repeated_packing_buffer, value);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedFixed64(UInt64 value)
{
    assert(current_field_number);
    writeLittleEndian(repeated_packing_buffer, value);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedSFixed64(Int64 value)
{
    assert(current_field_number);
    writeLittleEndian(repeated_packing_buffer, value);
    ++num_packed_values;
}

void ProtobufSimpleWriter::packRepeatedDouble(double value)
{
    assert(current_field_number);
    writeLittleEndian(repeated_packing_buffer, value);
    ++num_packed_values;
}

}
