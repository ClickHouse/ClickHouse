#include <Common/config.h>
#if USE_PROTOBUF

#include <cassert>
#include <math.h>
#include <optional>
#include <AggregateFunctions/IAggregateFunction.h>
#include <boost/numeric/conversion/cast.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include "ProtobufWriter.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD;
    extern const int PROTOBUF_BAD_CAST;
    extern const int PROTOBUF_FIELD_NOT_REPEATED;
}


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


enum ProtobufWriter::SimpleWriter::WireType : UInt32
{
    VARINT = 0,
    BITS64 = 1,
    LENGTH_DELIMITED = 2,
    BITS32 = 5
};

ProtobufWriter::SimpleWriter::SimpleWriter(WriteBuffer & out_) : out(out_)
{
}

ProtobufWriter::SimpleWriter::~SimpleWriter()
{
    finishCurrentMessage();
}

void ProtobufWriter::SimpleWriter::newMessage()
{
    finishCurrentMessage();
    were_messages = true;
}

void ProtobufWriter::SimpleWriter::finishCurrentMessage()
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

void ProtobufWriter::SimpleWriter::setCurrentField(UInt32 field_number)
{
    finishCurrentField();
    assert(current_field_number < field_number);
    current_field_number = field_number;
    num_normal_values = 0;
    num_packed_values = 0;
}

void ProtobufWriter::SimpleWriter::finishCurrentField()
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

void ProtobufWriter::SimpleWriter::writeKey(WireType wire_type, WriteBuffer & buf)
{
    writePbVarUInt((current_field_number << 3) | wire_type, buf);
}

void ProtobufWriter::SimpleWriter::writeInt32(Int32 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarIntNoZigZagEncoding(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeUInt32(UInt32 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarUInt(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeSInt32(Int32 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarInt(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeInt64(Int64 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarIntNoZigZagEncoding(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeUInt64(UInt64 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarUInt(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeSInt64(Int64 value)
{
    assert(current_field_number);
    writeKey(VARINT, message_buffer);
    writePbVarInt(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeFixed32(UInt32 value)
{
    assert(current_field_number);
    writeKey(BITS32, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeSFixed32(Int32 value)
{
    assert(current_field_number);
    writeKey(BITS32, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeFloat(float value)
{
    assert(current_field_number);
    writeKey(BITS32, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeFixed64(UInt64 value)
{
    assert(current_field_number);
    writeKey(BITS64, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeSFixed64(Int64 value)
{
    assert(current_field_number);
    writeKey(BITS64, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeDouble(double value)
{
    assert(current_field_number);
    writeKey(BITS64, message_buffer);
    writePODBinary(value, message_buffer);
    ++num_normal_values;
}

void ProtobufWriter::SimpleWriter::writeString(const StringRef & str)
{
    assert(current_field_number);
    ++num_normal_values;
    writeKey(LENGTH_DELIMITED, message_buffer);
    writePbVarUInt(str.size, message_buffer);
    message_buffer.write(str.data, str.size);
}

void ProtobufWriter::SimpleWriter::writeInt32IfNonZero(Int32 value)
{
    if (value)
        writeInt32(value);
}

void ProtobufWriter::SimpleWriter::writeUInt32IfNonZero(UInt32 value)
{
    if (value)
        writeUInt32(value);
}

void ProtobufWriter::SimpleWriter::writeSInt32IfNonZero(Int32 value)
{
    if (value)
        writeSInt32(value);
}

void ProtobufWriter::SimpleWriter::writeInt64IfNonZero(Int64 value)
{
    if (value)
        writeInt64(value);
}

void ProtobufWriter::SimpleWriter::writeUInt64IfNonZero(UInt64 value)
{
    if (value)
        writeUInt64(value);
}

void ProtobufWriter::SimpleWriter::writeSInt64IfNonZero(Int64 value)
{
    if (value)
        writeSInt64(value);
}

void ProtobufWriter::SimpleWriter::writeFixed32IfNonZero(UInt32 value)
{
    if (value)
        writeFixed32(value);
}

void ProtobufWriter::SimpleWriter::writeSFixed32IfNonZero(Int32 value)
{
    if (value)
        writeSFixed32(value);
}

void ProtobufWriter::SimpleWriter::writeFloatIfNonZero(float value)
{
    if (value != 0)
        writeFloat(value);
}

void ProtobufWriter::SimpleWriter::writeFixed64IfNonZero(UInt64 value)
{
    if (value)
        writeFixed64(value);
}

void ProtobufWriter::SimpleWriter::writeSFixed64IfNonZero(Int64 value)
{
    if (value)
        writeSFixed64(value);
}

void ProtobufWriter::SimpleWriter::writeDoubleIfNonZero(double value)
{
    if (value != 0)
        writeDouble(value);
}

void ProtobufWriter::SimpleWriter::writeStringIfNotEmpty(const StringRef & str)
{
    if (str.size)
        writeString(str);
}

void ProtobufWriter::SimpleWriter::packRepeatedInt32(Int32 value)
{
    assert(current_field_number);
    writePbVarIntNoZigZagEncoding(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedUInt32(UInt32 value)
{
    assert(current_field_number);
    writePbVarUInt(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedSInt32(Int32 value)
{
    assert(current_field_number);
    writePbVarInt(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedInt64(Int64 value)
{
    assert(current_field_number);
    writePbVarIntNoZigZagEncoding(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedUInt64(UInt64 value)
{
    assert(current_field_number);
    writePbVarUInt(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedSInt64(Int64 value)
{
    assert(current_field_number);
    writePbVarInt(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedFixed32(UInt32 value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedSFixed32(Int32 value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedFloat(float value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedFixed64(UInt64 value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedSFixed64(Int64 value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}

void ProtobufWriter::SimpleWriter::packRepeatedDouble(double value)
{
    assert(current_field_number);
    writePODBinary(value, repeated_packing_buffer);
    ++num_packed_values;
}



class ProtobufWriter::ConverterBaseImpl : public IConverter
{
public:
    ConverterBaseImpl(SimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : simple_writer(simple_writer_), field(field_)
    {
    }

    virtual void writeString(const StringRef &) override { cannotConvertType("String"); }

    virtual void writeInt8(Int8) override { cannotConvertType("Int8"); }
    virtual void writeUInt8(UInt8) override { cannotConvertType("UInt8"); }
    virtual void writeInt16(Int16) override { cannotConvertType("Int16"); }
    virtual void writeUInt16(UInt16) override { cannotConvertType("UInt16"); }
    virtual void writeInt32(Int32) override { cannotConvertType("Int32"); }
    virtual void writeUInt32(UInt32) override { cannotConvertType("UInt32"); }
    virtual void writeInt64(Int64) override { cannotConvertType("Int64"); }
    virtual void writeUInt64(UInt64) override { cannotConvertType("UInt64"); }
    virtual void writeUInt128(const UInt128 &) override { cannotConvertType("UInt128"); }
    virtual void writeFloat32(Float32) override { cannotConvertType("Float32"); }
    virtual void writeFloat64(Float64) override { cannotConvertType("Float64"); }

    virtual void prepareEnumMappingInt8(const std::vector<std::pair<std::string, Int8>> &) override {}
    virtual void prepareEnumMappingInt16(const std::vector<std::pair<std::string, Int16>> &) override {}
    virtual void writeEnumInt8(Int8) override { cannotConvertType("Enum"); }
    virtual void writeEnumInt16(Int16) override { cannotConvertType("Enum"); }

    virtual void writeUUID(const UUID &) override { cannotConvertType("UUID"); }
    virtual void writeDate(DayNum) override { cannotConvertType("Date"); }
    virtual void writeDateTime(time_t) override { cannotConvertType("DateTime"); }

    virtual void writeDecimal32(Decimal32, UInt32) override { cannotConvertType("Decimal32"); }
    virtual void writeDecimal64(Decimal64, UInt32) override { cannotConvertType("Decimal64"); }
    virtual void writeDecimal128(const Decimal128 &, UInt32) override { cannotConvertType("Decimal128"); }

    virtual void writeAggregateFunction(const AggregateFunctionPtr &, ConstAggregateDataPtr) override { cannotConvertType("AggregateFunction"); }

protected:
    void cannotConvertType(const String & type_name)
    {
        throw Exception(
            "Could not convert data type '" + type_name + "' to protobuf type '" + field->type_name() + "' (field: " + field->name() + ")",
            ErrorCodes::PROTOBUF_BAD_CAST);
    }

    void cannotConvertValue(const String & value)
    {
        throw Exception(
            "Could not convert value '" + value + "' to protobuf type '" + field->type_name() + "' (field: " + field->name() + ")",
            ErrorCodes::PROTOBUF_BAD_CAST);
    }

    template <typename To, typename From>
    To numericCast(From value)
    {
        if constexpr (std::is_same_v<To, From>)
            return value;
        To result;
        try
        {
            result = boost::numeric_cast<To>(value);
        }
        catch (boost::numeric::bad_numeric_cast &)
        {
            cannotConvertValue(toString(value));
        }
        return result;
    }

    template <typename To>
    To parseFromString(const StringRef & str)
    {
        To result;
        try
        {
            result = ::DB::parse<To>(str.data, str.size);
        }
        catch (...)
        {
            cannotConvertValue(str.toString());
        }
        return result;
    }

    bool packRepeated() const
    {
        if (!field->is_repeated())
            return false;
        if (field->options().has_packed())
            return field->options().packed();
        return field->file()->syntax() == google::protobuf::FileDescriptor::SYNTAX_PROTO3;
    }

    bool skipNullValue() const
    {
        return field->is_optional() && (field->file()->syntax() == google::protobuf::FileDescriptor::SYNTAX_PROTO3);
    }

    SimpleWriter & simple_writer;
    const google::protobuf::FieldDescriptor * field;
};



class ProtobufWriter::ConverterToString : public ConverterBaseImpl
{
public:
    ConverterToString(SimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : ConverterBaseImpl(simple_writer_, field_)
    {
        initWriteFieldFunction();
    }

    void writeString(const StringRef & str) override { writeField(str); }

    void writeInt8(Int8 value) override { convertToStringAndWriteField(value); }
    void writeUInt8(UInt8 value) override { convertToStringAndWriteField(value); }
    void writeInt16(Int16 value) override { convertToStringAndWriteField(value); }
    void writeUInt16(UInt16 value) override { convertToStringAndWriteField(value); }
    void writeInt32(Int32 value) override { convertToStringAndWriteField(value); }
    void writeUInt32(UInt32 value) override { convertToStringAndWriteField(value); }
    void writeInt64(Int64 value) override { convertToStringAndWriteField(value); }
    void writeUInt64(UInt64 value) override { convertToStringAndWriteField(value); }
    void writeFloat32(Float32 value) override { convertToStringAndWriteField(value); }
    void writeFloat64(Float64 value) override { convertToStringAndWriteField(value); }

    void prepareEnumMappingInt8(const std::vector<std::pair<String, Int8>> & name_value_pairs) override
    {
        prepareEnumValueToNameMap(name_value_pairs);
    }
    void prepareEnumMappingInt16(const std::vector<std::pair<String, Int16>> & name_value_pairs) override
    {
        prepareEnumValueToNameMap(name_value_pairs);
    }

    void writeEnumInt8(Int8 value) override { writeEnumInt16(value); }

    void writeEnumInt16(Int16 value) override
    {
        auto it = enum_value_to_name_map->find(value);
        if (it == enum_value_to_name_map->end())
            cannotConvertValue(toString(value));
        writeField(it->second);
    }

    void writeUUID(const UUID & uuid) override { convertToStringAndWriteField(uuid); }
    void writeDate(DayNum date) override { convertToStringAndWriteField(date); }

    void writeDateTime(time_t tm) override
    {
        writeDateTimeText(tm, text_buffer);
        writeField(text_buffer.stringRef());
        text_buffer.restart();
    }

    void writeDecimal32(Decimal32 decimal, UInt32 scale) override { writeDecimal(decimal, scale); }
    void writeDecimal64(Decimal64 decimal, UInt32 scale) override { writeDecimal(decimal, scale); }
    void writeDecimal128(const Decimal128 & decimal, UInt32 scale) override { writeDecimal(decimal, scale); }

    void writeAggregateFunction(const AggregateFunctionPtr & function, ConstAggregateDataPtr place) override
    {
        function->serialize(place, text_buffer);
        writeField(text_buffer.stringRef());
        text_buffer.restart();
    }

private:
    template <typename T>
    void convertToStringAndWriteField(T value)
    {
        writeText(value, text_buffer);
        writeField(text_buffer.stringRef());
        text_buffer.restart();
    }

    template <typename T>
    void writeDecimal(const Decimal<T> & decimal, UInt32 scale)
    {
        writeText(decimal, scale, text_buffer);
        writeField(text_buffer.stringRef());
        text_buffer.restart();
    }

    template <typename T>
    void prepareEnumValueToNameMap(const std::vector<std::pair<String, T>> & name_value_pairs)
    {
        if (enum_value_to_name_map.has_value())
            return;
        enum_value_to_name_map.emplace();
        for (const auto & name_value_pair : name_value_pairs)
            enum_value_to_name_map->emplace(name_value_pair.second, name_value_pair.first);
    }

    void writeField(const StringRef & str) { (simple_writer.*write_field_function)(str); }

    void initWriteFieldFunction()
    {
        write_field_function = skipNullValue() ? &SimpleWriter::writeStringIfNotEmpty : &SimpleWriter::writeString;
    }

    void (SimpleWriter::*write_field_function)(const StringRef & str);
    WriteBufferFromOwnString text_buffer;
    std::optional<std::unordered_map<Int16, String>> enum_value_to_name_map;
};

#define PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_STRINGS(field_type_id) \
    template<> \
    class ProtobufWriter::ConverterImpl<field_type_id> : public ConverterToString \
    { \
        using ConverterToString::ConverterToString; \
    }
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_STRINGS(google::protobuf::FieldDescriptor::TYPE_STRING);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_STRINGS(google::protobuf::FieldDescriptor::TYPE_BYTES);
#undef PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_STRINGS



template <typename T>
class ProtobufWriter::ConverterToNumber : public ConverterBaseImpl
{
public:
    ConverterToNumber(SimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : ConverterBaseImpl(simple_writer_, field_)
    {
        initWriteFieldFunction();
    }

    void writeString(const StringRef & str) override { writeField(parseFromString<T>(str)); }

    void writeInt8(Int8 value) override { castNumericAndWriteField(value); }
    void writeUInt8(UInt8 value) override { castNumericAndWriteField(value); }
    void writeInt16(Int16 value) override { castNumericAndWriteField(value); }
    void writeUInt16(UInt16 value) override { castNumericAndWriteField(value); }
    void writeInt32(Int32 value) override { castNumericAndWriteField(value); }
    void writeUInt32(UInt32 value) override { castNumericAndWriteField(value); }
    void writeInt64(Int64 value) override { castNumericAndWriteField(value); }
    void writeUInt64(UInt64 value) override { castNumericAndWriteField(value); }
    void writeFloat32(Float32 value) override { castNumericAndWriteField(value); }
    void writeFloat64(Float64 value) override { castNumericAndWriteField(value); }

    void writeEnumInt8(Int8 value) override { writeEnumInt16(value); }

    void writeEnumInt16(Int16 value) override
    {
        if constexpr (!std::is_integral_v<T>)
            cannotConvertType("Enum"); // It's not correct to convert enum to floating point.
        castNumericAndWriteField(value);
    }

    void writeDate(DayNum date) override { castNumericAndWriteField(static_cast<UInt16>(date)); }
    void writeDateTime(time_t tm) override { castNumericAndWriteField(tm); }

    void writeDecimal32(Decimal32 decimal, UInt32 scale) override { writeDecimal(decimal, scale); }
    void writeDecimal64(Decimal64 decimal, UInt32 scale) override { writeDecimal(decimal, scale); }

private:
    template <typename From>
    void castNumericAndWriteField(From value)
    {
        writeField(numericCast<T>(value));
    }

    template <typename S>
    void writeDecimal(const Decimal<S> & decimal, UInt32 scale)
    {
        if constexpr (std::is_integral_v<T>)
            castNumericAndWriteField(decimal.value / decimalScaleMultiplier<S>(scale));
        else
            castNumericAndWriteField(double(decimal.value) * pow(10., -double(scale)));
    }

    void writeField(T value) { (simple_writer.*write_field_function)(value); }

    void initWriteFieldFunction()
    {
        if constexpr (std::is_same_v<T, Int32>)
        {
            switch (field->type())
            {
                case google::protobuf::FieldDescriptor::TYPE_INT32:
                    write_field_function = packRepeated()
                        ? &SimpleWriter::packRepeatedInt32
                        : (skipNullValue() ? &SimpleWriter::writeInt32IfNonZero : &SimpleWriter::writeInt32);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_SINT32:
                    write_field_function = packRepeated()
                        ? &SimpleWriter::packRepeatedSInt32
                        : (skipNullValue() ? &SimpleWriter::writeSInt32IfNonZero : &SimpleWriter::writeSInt32);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
                    write_field_function = packRepeated()
                        ? &SimpleWriter::packRepeatedSFixed32
                        : (skipNullValue() ? &SimpleWriter::writeSFixed32IfNonZero : &SimpleWriter::writeSFixed32);
                    break;
                default:
                    assert(false);
            }
        }
        else if constexpr (std::is_same_v<T, UInt32>)
        {
            switch (field->type())
            {
                case google::protobuf::FieldDescriptor::TYPE_UINT32:
                    write_field_function = packRepeated()
                        ? &SimpleWriter::packRepeatedUInt32
                        : (skipNullValue() ? &SimpleWriter::writeUInt32IfNonZero : &SimpleWriter::writeUInt32);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_FIXED32:
                    write_field_function = packRepeated()
                        ? &SimpleWriter::packRepeatedFixed32
                        : (skipNullValue() ? &SimpleWriter::writeFixed32IfNonZero : &SimpleWriter::writeFixed32);
                    break;
                default:
                    assert(false);
            }
        }
        else if constexpr (std::is_same_v<T, Int64>)
        {
            switch (field->type())
            {
                case google::protobuf::FieldDescriptor::TYPE_INT64:
                    write_field_function = packRepeated()
                        ? &SimpleWriter::packRepeatedInt64
                        : (skipNullValue() ? &SimpleWriter::writeInt64IfNonZero : &SimpleWriter::writeInt64);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_SINT64:
                    write_field_function = packRepeated()
                        ? &SimpleWriter::packRepeatedSInt64
                        : (skipNullValue() ? &SimpleWriter::writeSInt64IfNonZero : &SimpleWriter::writeSInt64);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
                    write_field_function = packRepeated()
                        ? &SimpleWriter::packRepeatedSFixed64
                        : (skipNullValue() ? &SimpleWriter::writeSFixed64IfNonZero : &SimpleWriter::writeSFixed64);
                    break;
                default:
                    assert(false);
            }
        }
        else if constexpr (std::is_same_v<T, UInt64>)
        {
            switch (field->type())
            {
                case google::protobuf::FieldDescriptor::TYPE_UINT64:
                    write_field_function = packRepeated()
                        ? &SimpleWriter::packRepeatedUInt64
                        : (skipNullValue() ? &SimpleWriter::writeUInt64IfNonZero : &SimpleWriter::writeUInt64);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_FIXED64:
                    write_field_function = packRepeated()
                        ? &SimpleWriter::packRepeatedFixed64
                        : (skipNullValue() ? &SimpleWriter::writeFixed64IfNonZero : &SimpleWriter::writeFixed64);
                    break;
                default:
                    assert(false);
            }
        }
        else if constexpr (std::is_same_v<T, float>)
        {
            write_field_function = packRepeated()
                ? &SimpleWriter::packRepeatedFloat
                : (skipNullValue() ? &SimpleWriter::writeFloatIfNonZero : &SimpleWriter::writeFloat);
        }
        else if constexpr (std::is_same_v<T, double>)
        {
            write_field_function = packRepeated()
                ? &SimpleWriter::packRepeatedDouble
                : (skipNullValue() ? &SimpleWriter::writeDoubleIfNonZero : &SimpleWriter::writeDouble);
        }
        else
        {
            assert(false);
        }
    }

    void (SimpleWriter::*write_field_function)(T value);
};

#define PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(field_type_id, field_type) \
    template<> \
    class ProtobufWriter::ConverterImpl<field_type_id> : public ConverterToNumber<field_type> \
    { \
        using ConverterToNumber::ConverterToNumber; \
    }
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_INT32, Int32);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SINT32, Int32);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_UINT32, UInt32);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_INT64, Int64);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SINT64, Int64);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_UINT64, UInt64);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_FIXED32, UInt32);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SFIXED32, Int32);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_FIXED64, UInt64);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SFIXED64, Int64);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_FLOAT, float);
PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_DOUBLE, double);
#undef PROTOBUF_WRITER_CONVERTER_IMPL_SPECIALIZATION_FOR_NUMBERS



template<>
class ProtobufWriter::ConverterImpl<google::protobuf::FieldDescriptor::TYPE_BOOL> : public ConverterBaseImpl
{
public:
    ConverterImpl(SimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : ConverterBaseImpl(simple_writer_, field_)
    {
        initWriteFieldFunction();
    }

    void writeString(const StringRef & str) override
    {
        if (str == "true")
            writeField(true);
        else if (str == "false")
            writeField(false);
        else
            cannotConvertValue(str.toString());
    }

    void writeInt8(Int8 value) override { convertToBoolAndWriteField(value); }
    void writeUInt8(UInt8 value) override { convertToBoolAndWriteField(value); }
    void writeInt16(Int16 value) override { convertToBoolAndWriteField(value); }
    void writeUInt16(UInt16 value) override { convertToBoolAndWriteField(value); }
    void writeInt32(Int32 value) override { convertToBoolAndWriteField(value); }
    void writeUInt32(UInt32 value) override { convertToBoolAndWriteField(value); }
    void writeInt64(Int64 value) override { convertToBoolAndWriteField(value); }
    void writeUInt64(UInt64 value) override { convertToBoolAndWriteField(value); }
    void writeFloat32(Float32 value) override { convertToBoolAndWriteField(value); }
    void writeFloat64(Float64 value) override { convertToBoolAndWriteField(value); }
    void writeDecimal32(Decimal32 decimal, UInt32) override { convertToBoolAndWriteField(decimal.value); }
    void writeDecimal64(Decimal64 decimal, UInt32) override { convertToBoolAndWriteField(decimal.value); }
    void writeDecimal128(const Decimal128 & decimal, UInt32) override { convertToBoolAndWriteField(decimal.value); }

private:
    template <typename T>
    void convertToBoolAndWriteField(T value)
    {
        writeField(static_cast<bool>(value));
    }

    void writeField(bool b) { (simple_writer.*write_field_function)(b); }

    void initWriteFieldFunction()
    {
        write_field_function = packRepeated()
            ? &SimpleWriter::packRepeatedUInt32
            : (skipNullValue() ? &SimpleWriter::writeUInt32IfNonZero : &SimpleWriter::writeUInt32);
    }

    void (SimpleWriter::*write_field_function)(UInt32 b);
};



template<>
class ProtobufWriter::ConverterImpl<google::protobuf::FieldDescriptor::TYPE_ENUM> : public ConverterBaseImpl
{
public:
    ConverterImpl(SimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : ConverterBaseImpl(simple_writer_, field_)
    {
        initWriteFieldFunction();
    }

    void writeString(const StringRef & str) override
    {
        prepareEnumNameToPbNumberMap();
        auto it = enum_name_to_pbnumber_map->find(str);
        if (it == enum_name_to_pbnumber_map->end())
            cannotConvertValue(str.toString());
        writeField(it->second);
    }

    void writeInt8(Int8 value) override { convertToEnumAndWriteField(value); }
    void writeUInt8(UInt8 value) override { convertToEnumAndWriteField(value); }
    void writeInt16(Int16 value) override { convertToEnumAndWriteField(value); }
    void writeUInt16(UInt16 value) override { convertToEnumAndWriteField(value); }
    void writeInt32(Int32 value) override { convertToEnumAndWriteField(value); }
    void writeUInt32(UInt32 value) override { convertToEnumAndWriteField(value); }
    void writeInt64(Int64 value) override { convertToEnumAndWriteField(value); }
    void writeUInt64(UInt64 value) override { convertToEnumAndWriteField(value); }

    void prepareEnumMappingInt8(const std::vector<std::pair<String, Int8>> & name_value_pairs) override
    {
        prepareEnumValueToPbNumberMap(name_value_pairs);
    }
    void prepareEnumMappingInt16(const std::vector<std::pair<String, Int16>> & name_value_pairs) override
    {
        prepareEnumValueToPbNumberMap(name_value_pairs);
    }

    void writeEnumInt8(Int8 value) override { writeEnumInt16(value); }

    void writeEnumInt16(Int16 value) override
    {
        auto it = enum_value_to_pbnumber_map->find(value);
        if (it == enum_value_to_pbnumber_map->end())
            cannotConvertValue(toString(value));
        writeField(it->second);
    }

private:
    template <typename T>
    void convertToEnumAndWriteField(T value)
    {
        const auto * enum_descriptor = field->enum_type()->FindValueByNumber(numericCast<int>(value));
        if (!enum_descriptor)
            cannotConvertValue(toString(value));
        writeField(enum_descriptor->number());
    }

    void prepareEnumNameToPbNumberMap()
    {
        if (enum_name_to_pbnumber_map.has_value())
            return;
        enum_name_to_pbnumber_map.emplace();
        const auto * enum_type = field->enum_type();
        for (int i = 0; i != enum_type->value_count(); ++i)
        {
            const auto * enum_value = enum_type->value(i);
            enum_name_to_pbnumber_map->emplace(enum_value->name(), enum_value->number());
        }
    }

    template <typename T>
    void prepareEnumValueToPbNumberMap(const std::vector<std::pair<String, T>> & name_value_pairs)
    {
        if (enum_value_to_pbnumber_map.has_value())
            return;
        enum_value_to_pbnumber_map.emplace();
        for (const auto & name_value_pair : name_value_pairs)
        {
            Int16 value = name_value_pair.second;
            const auto * enum_descriptor = field->enum_type()->FindValueByName(name_value_pair.first);
            if (enum_descriptor)
                enum_value_to_pbnumber_map->emplace(value, enum_descriptor->number());
        }
    }

    void writeField(int enum_number) { (simple_writer.*write_field_function)(enum_number); }

    void initWriteFieldFunction()
    {
        write_field_function = packRepeated()
            ? &SimpleWriter::packRepeatedUInt32
            : (skipNullValue() ? &SimpleWriter::writeUInt32IfNonZero : &SimpleWriter::writeUInt32);
    }

    void (SimpleWriter::*write_field_function)(UInt32 enum_number);
    std::optional<std::unordered_map<StringRef, int>> enum_name_to_pbnumber_map;
    std::optional<std::unordered_map<Int16, int>> enum_value_to_pbnumber_map;
};


ProtobufWriter::ProtobufWriter(WriteBuffer & out, const google::protobuf::Descriptor * message_type) : simple_writer(out)
{
    enumerateFieldsInWriteOrder(message_type);
    createConverters();
}

ProtobufWriter::~ProtobufWriter()
{
    finishCurrentMessage();
}

void ProtobufWriter::enumerateFieldsInWriteOrder(const google::protobuf::Descriptor * message_type)
{
    assert(fields_in_write_order.empty());
    fields_in_write_order.reserve(message_type->field_count());
    for (int i = 0; i < message_type->field_count(); ++i)
        fields_in_write_order.emplace_back(message_type->field(i));

    std::sort(
        fields_in_write_order.begin(),
        fields_in_write_order.end(),
        [](const google::protobuf::FieldDescriptor * left, const google::protobuf::FieldDescriptor * right)
        {
            return left->number() < right->number();
        });
}

void ProtobufWriter::createConverters()
{
    assert(converters.empty());
    converters.reserve(fields_in_write_order.size());
    for (size_t i = 0; i != fields_in_write_order.size(); ++i)
    {
        const auto * field = fields_in_write_order[i];
        std::unique_ptr<IConverter> converter;
        switch (field->type())
        {
#define PROTOBUF_WRITER_CONVERTER_CREATING_CASE(field_type_id) \
            case field_type_id: \
                converter = std::make_unique<ConverterImpl<field_type_id>>(simple_writer, field); \
                break
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_STRING);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_BYTES);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_INT32);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_SINT32);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_UINT32);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_FIXED32);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_SFIXED32);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_INT64);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_SINT64);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_UINT64);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_FIXED64);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_SFIXED64);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_FLOAT);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_DOUBLE);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_BOOL);
            PROTOBUF_WRITER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_ENUM);
#undef PROTOBUF_WRITER_CONVERTER_CREATING_CASE
            default:
                throw Exception(String("Protobuf type '") + field->type_name() + "' isn't supported", ErrorCodes::NOT_IMPLEMENTED);
        }
        converters.emplace_back(std::move(converter));
    }
}

const std::vector<const google::protobuf::FieldDescriptor *> & ProtobufWriter::fieldsInWriteOrder() const
{
    return fields_in_write_order;
}

void ProtobufWriter::newMessage()
{
    finishCurrentMessage();
    simple_writer.newMessage();
    if (fields_in_write_order.empty())
        return;

    current_field_index = 0;
    current_field = fields_in_write_order[current_field_index];
    current_converter = converters[current_field_index].get();
    simple_writer.setCurrentField(current_field->number());
}

void ProtobufWriter::finishCurrentMessage()
{
    if (current_field)
    {
        assert(current_field_index == fields_in_write_order.size() - 1);
        finishCurrentField();
    }
}

bool ProtobufWriter::nextField()
{
    if (current_field_index == fields_in_write_order.size() - 1)
        return false;

    finishCurrentField();

    ++current_field_index;
    current_field = fields_in_write_order[current_field_index];
    current_converter = converters[current_field_index].get();
    simple_writer.setCurrentField(current_field->number());
    return true;
}

void ProtobufWriter::finishCurrentField()
{
    assert(current_field);
    size_t num_values = simple_writer.numValues();
    if (num_values == 0)
    {
        if (current_field->is_required())
            throw Exception(
                "No data for the required field '" + current_field->name() + "'", ErrorCodes::NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD);
    }
    else if (num_values > 1 && !current_field->is_repeated())
    {
        throw Exception(
            "Cannot write more than single value to the non-repeated field '" + current_field->name() + "'",
            ErrorCodes::PROTOBUF_FIELD_NOT_REPEATED);
    }
}

}
#endif
