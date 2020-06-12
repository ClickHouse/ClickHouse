#include "ProtobufWriter.h"

#if USE_PROTOBUF
#    include <cassert>
#    include <optional>
#    include <math.h>
#    include <AggregateFunctions/IAggregateFunction.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <IO/ReadHelpers.h>
#    include <IO/WriteHelpers.h>
#    include <boost/numeric/conversion/cast.hpp>
#    include <google/protobuf/descriptor.h>
#    include <google/protobuf/descriptor.pb.h>


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

    // Should we pack repeated values while storing them.
    // It depends on type of the field in the protobuf schema and the syntax of that schema.
    bool shouldPackRepeated(const google::protobuf::FieldDescriptor * field)
    {
        if (!field->is_repeated())
            return false;
        switch (field->type())
        {
            case google::protobuf::FieldDescriptor::TYPE_INT32:
            case google::protobuf::FieldDescriptor::TYPE_UINT32:
            case google::protobuf::FieldDescriptor::TYPE_SINT32:
            case google::protobuf::FieldDescriptor::TYPE_INT64:
            case google::protobuf::FieldDescriptor::TYPE_UINT64:
            case google::protobuf::FieldDescriptor::TYPE_SINT64:
            case google::protobuf::FieldDescriptor::TYPE_FIXED32:
            case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
            case google::protobuf::FieldDescriptor::TYPE_FIXED64:
            case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
            case google::protobuf::FieldDescriptor::TYPE_FLOAT:
            case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
            case google::protobuf::FieldDescriptor::TYPE_BOOL:
            case google::protobuf::FieldDescriptor::TYPE_ENUM:
                break;
            default:
                return false;
        }
        if (field->options().has_packed())
            return field->options().packed();
        return field->file()->syntax() == google::protobuf::FileDescriptor::SYNTAX_PROTO3;
    }

    // Should we omit null values (zero for numbers / empty string for strings) while storing them.
    bool shouldSkipNullValue(const google::protobuf::FieldDescriptor * field)
    {
        return field->is_optional() && (field->file()->syntax() == google::protobuf::FileDescriptor::SYNTAX_PROTO3);
    }
}


// SimpleWriter is an utility class to serialize protobufs.
// Knows nothing about protobuf schemas, just provides useful functions to serialize data.
ProtobufWriter::SimpleWriter::SimpleWriter(WriteBuffer & out_) : out(out_), current_piece_start(0), num_bytes_skipped(0)
{
}

ProtobufWriter::SimpleWriter::~SimpleWriter() = default;

void ProtobufWriter::SimpleWriter::startMessage()
{
}

void ProtobufWriter::SimpleWriter::endMessage()
{
    pieces.emplace_back(current_piece_start, buffer.size());
    size_t size_of_message = buffer.size() - num_bytes_skipped;
    writeVarint(size_of_message, out);
    for (const auto & piece : pieces)
        if (piece.end > piece.start)
            out.write(reinterpret_cast<char *>(&buffer[piece.start]), piece.end - piece.start);
    buffer.clear();
    pieces.clear();
    num_bytes_skipped = 0;
    current_piece_start = 0;
}

void ProtobufWriter::SimpleWriter::startNestedMessage()
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

void ProtobufWriter::SimpleWriter::endNestedMessage(UInt32 field_number, bool is_group, bool skip_if_empty)
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

void ProtobufWriter::SimpleWriter::writeUInt(UInt32 field_number, UInt64 value)
{
    size_t old_size = buffer.size();
    buffer.reserve(old_size + 2 * MAX_VARINT_SIZE);
    UInt8 * ptr = buffer.data() + old_size;
    ptr = writeFieldNumber(field_number, VARINT, ptr);
    ptr = writeVarint(value, ptr);
    buffer.resize_assume_reserved(ptr - buffer.data());
}

void ProtobufWriter::SimpleWriter::writeInt(UInt32 field_number, Int64 value)
{
    writeUInt(field_number, static_cast<UInt64>(value));
}

void ProtobufWriter::SimpleWriter::writeSInt(UInt32 field_number, Int64 value)
{
    writeUInt(field_number, encodeZigZag(value));
}

template <typename T>
void ProtobufWriter::SimpleWriter::writeFixed(UInt32 field_number, T value)
{
    static_assert((sizeof(T) == 4) || (sizeof(T) == 8));
    constexpr WireType wire_type = (sizeof(T) == 4) ? BITS32 : BITS64;
    size_t old_size = buffer.size();
    buffer.reserve(old_size + MAX_VARINT_SIZE + sizeof(T));
    UInt8 * ptr = buffer.data() + old_size;
    ptr = writeFieldNumber(field_number, wire_type, ptr);
    memcpy(ptr, &value, sizeof(T));
    ptr += sizeof(T);
    buffer.resize_assume_reserved(ptr - buffer.data());
}

void ProtobufWriter::SimpleWriter::writeString(UInt32 field_number, const StringRef & str)
{
    size_t old_size = buffer.size();
    buffer.reserve(old_size + 2 * MAX_VARINT_SIZE + str.size);
    UInt8 * ptr = buffer.data() + old_size;
    ptr = writeFieldNumber(field_number, LENGTH_DELIMITED, ptr);
    ptr = writeVarint(str.size, ptr);
    memcpy(ptr, str.data, str.size);
    ptr += str.size;
    buffer.resize_assume_reserved(ptr - buffer.data());
}

void ProtobufWriter::SimpleWriter::startRepeatedPack()
{
    pieces.emplace_back(current_piece_start, buffer.size());

    // We skip enough bytes to have place for inserting the field number and the size of the repeated pack afterwards
    // when we finish writing the repeated pack itself. We don't know the size of the repeated pack at the point of
    // calling startRepeatedPack(), that's why we have to do this skipping.
    current_piece_start = buffer.size() + REPEATED_PACK_PADDING;
    buffer.resize(current_piece_start);
    num_bytes_skipped += REPEATED_PACK_PADDING;
}

void ProtobufWriter::SimpleWriter::endRepeatedPack(UInt32 field_number)
{
    size_t size = buffer.size() - current_piece_start;
    if (!size)
    {
        current_piece_start = pieces.back().start;
        buffer.resize(pieces.back().end);
        pieces.pop_back();
        num_bytes_skipped -= REPEATED_PACK_PADDING;
        return;
    }
    UInt8 * ptr = &buffer[pieces.back().end];
    UInt8 * endptr = writeFieldNumber(field_number, LENGTH_DELIMITED, ptr);
    endptr = writeVarint(size, endptr);
    size_t num_bytes_inserted = endptr - ptr;
    pieces.back().end += num_bytes_inserted;
    num_bytes_skipped -= num_bytes_inserted;
}

void ProtobufWriter::SimpleWriter::addUIntToRepeatedPack(UInt64 value)
{
    writeVarint(value, buffer);
}

void ProtobufWriter::SimpleWriter::addIntToRepeatedPack(Int64 value)
{
    writeVarint(static_cast<UInt64>(value), buffer);
}

void ProtobufWriter::SimpleWriter::addSIntToRepeatedPack(Int64 value)
{
    writeVarint(encodeZigZag(value), buffer);
}

template <typename T>
void ProtobufWriter::SimpleWriter::addFixedToRepeatedPack(T value)
{
    static_assert((sizeof(T) == 4) || (sizeof(T) == 8));
    size_t old_size = buffer.size();
    buffer.resize(old_size + sizeof(T));
    memcpy(buffer.data() + old_size, &value, sizeof(T));
}


// Implementation for a converter from any DB data type to any protobuf field type.
class ProtobufWriter::ConverterBaseImpl : public IConverter
{
public:
    ConverterBaseImpl(SimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : simple_writer(simple_writer_), field(field_)
    {
        field_number = field->number();
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
    virtual void prepareEnumMapping8(const std::vector<std::pair<std::string, Int8>> &) override {}
    virtual void prepareEnumMapping16(const std::vector<std::pair<std::string, Int16>> &) override {}
    virtual void writeEnum8(Int8) override { cannotConvertType("Enum"); }
    virtual void writeEnum16(Int16) override { cannotConvertType("Enum"); }
    virtual void writeUUID(const UUID &) override { cannotConvertType("UUID"); }
    virtual void writeDate(DayNum) override { cannotConvertType("Date"); }
    virtual void writeDateTime(time_t) override { cannotConvertType("DateTime"); }
    virtual void writeDateTime64(DateTime64, UInt32) override { cannotConvertType("DateTime64"); }
    virtual void writeDecimal32(Decimal32, UInt32) override { cannotConvertType("Decimal32"); }
    virtual void writeDecimal64(Decimal64, UInt32) override { cannotConvertType("Decimal64"); }
    virtual void writeDecimal128(const Decimal128 &, UInt32) override { cannotConvertType("Decimal128"); }

    virtual void writeAggregateFunction(const AggregateFunctionPtr &, ConstAggregateDataPtr) override { cannotConvertType("AggregateFunction"); }

protected:
    [[noreturn]] void cannotConvertType(const String & type_name)
    {
        throw Exception(
            "Could not convert data type '" + type_name + "' to protobuf type '" + field->type_name() + "' (field: " + field->name() + ")",
            ErrorCodes::PROTOBUF_BAD_CAST);
    }

    [[noreturn]] void cannotConvertValue(const String & value)
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

    SimpleWriter & simple_writer;
    const google::protobuf::FieldDescriptor * field;
    UInt32 field_number;
};


template <bool skip_null_value>
class ProtobufWriter::ConverterToString : public ConverterBaseImpl
{
public:
    using ConverterBaseImpl::ConverterBaseImpl;

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

    void prepareEnumMapping8(const std::vector<std::pair<String, Int8>> & name_value_pairs) override
    {
        prepareEnumValueToNameMap(name_value_pairs);
    }
    void prepareEnumMapping16(const std::vector<std::pair<String, Int16>> & name_value_pairs) override
    {
        prepareEnumValueToNameMap(name_value_pairs);
    }

    void writeEnum8(Int8 value) override { writeEnum16(value); }

    void writeEnum16(Int16 value) override
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

    void writeDateTime64(DateTime64 date_time, UInt32 scale) override
    {
        writeDateTimeText(date_time, scale, text_buffer);
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

    void writeField(const StringRef & str)
    {
        if constexpr (skip_null_value)
        {
            if (!str.size)
                return;
        }
        simple_writer.writeString(field_number, str);
    }

    WriteBufferFromOwnString text_buffer;
    std::optional<std::unordered_map<Int16, String>> enum_value_to_name_map;
};

#    define PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_STRINGS(field_type_id) \
        template <> \
        std::unique_ptr<ProtobufWriter::IConverter> ProtobufWriter::createConverter<field_type_id>( \
            const google::protobuf::FieldDescriptor * field) \
        { \
            if (shouldSkipNullValue(field)) \
                return std::make_unique<ConverterToString<true>>(simple_writer, field); \
            else \
                return std::make_unique<ConverterToString<false>>(simple_writer, field); \
        }
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_STRINGS(google::protobuf::FieldDescriptor::TYPE_STRING)
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_STRINGS(google::protobuf::FieldDescriptor::TYPE_BYTES)
#    undef PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_STRINGS


template <int field_type_id, typename ToType, bool skip_null_value, bool pack_repeated>
class ProtobufWriter::ConverterToNumber : public ConverterBaseImpl
{
public:
    using ConverterBaseImpl::ConverterBaseImpl;

    void writeString(const StringRef & str) override { writeField(parseFromString<ToType>(str)); }

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

    void writeEnum8(Int8 value) override { writeEnum16(value); }

    void writeEnum16(Int16 value) override
    {
        if constexpr (!is_integral_v<ToType>)
            cannotConvertType("Enum"); // It's not correct to convert enum to floating point.
        castNumericAndWriteField(value);
    }

    void writeDate(DayNum date) override { castNumericAndWriteField(static_cast<UInt16>(date)); }
    void writeDateTime(time_t tm) override { castNumericAndWriteField(tm); }
    void writeDateTime64(DateTime64 date_time, UInt32 scale) override { writeDecimal(date_time, scale); }
    void writeDecimal32(Decimal32 decimal, UInt32 scale) override { writeDecimal(decimal, scale); }
    void writeDecimal64(Decimal64 decimal, UInt32 scale) override { writeDecimal(decimal, scale); }
    void writeDecimal128(const Decimal128 & decimal, UInt32 scale) override { writeDecimal(decimal, scale); }

private:
    template <typename FromType>
    void castNumericAndWriteField(FromType value)
    {
        writeField(numericCast<ToType>(value));
    }

    template <typename S>
    void writeDecimal(const Decimal<S> & decimal, UInt32 scale)
    {
        castNumericAndWriteField(convertFromDecimal<DataTypeDecimal<Decimal<S>>, DataTypeNumber<ToType>>(decimal.value, scale));
    }

    void writeField(ToType value)
    {
        if constexpr (skip_null_value)
        {
            if (value == 0)
                return;
        }
        if constexpr (((field_type_id == google::protobuf::FieldDescriptor::TYPE_INT32) && std::is_same_v<ToType, Int32>)
                   || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_INT64) && std::is_same_v<ToType, Int64>))
        {
            if constexpr (pack_repeated)
                simple_writer.addIntToRepeatedPack(value);
            else
                simple_writer.writeInt(field_number, value);
        }
        else if constexpr (((field_type_id == google::protobuf::FieldDescriptor::TYPE_SINT32) && std::is_same_v<ToType, Int32>)
                        || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_SINT64) && std::is_same_v<ToType, Int64>))
        {
            if constexpr (pack_repeated)
                simple_writer.addSIntToRepeatedPack(value);
            else
                simple_writer.writeSInt(field_number, value);
        }
        else if constexpr (((field_type_id == google::protobuf::FieldDescriptor::TYPE_UINT32) && std::is_same_v<ToType, UInt32>)
                        || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_UINT64) && std::is_same_v<ToType, UInt64>))
        {
            if constexpr (pack_repeated)
                simple_writer.addUIntToRepeatedPack(value);
            else
                simple_writer.writeUInt(field_number, value);
        }
        else
        {
            static_assert(((field_type_id == google::protobuf::FieldDescriptor::TYPE_FIXED32) && std::is_same_v<ToType, UInt32>)
                       || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_SFIXED32) && std::is_same_v<ToType, Int32>)
                       || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_FIXED64) && std::is_same_v<ToType, UInt64>)
                       || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_SFIXED64) && std::is_same_v<ToType, Int64>)
                       || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_FLOAT) && std::is_same_v<ToType, float>)
                       || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_DOUBLE) && std::is_same_v<ToType, double>));
            if constexpr (pack_repeated)
                simple_writer.addFixedToRepeatedPack(value);
            else
                simple_writer.writeFixed(field_number, value);
        }
    }
};

#    define PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(field_type_id, field_type) \
        template <> \
        std::unique_ptr<ProtobufWriter::IConverter> ProtobufWriter::createConverter<field_type_id>( \
            const google::protobuf::FieldDescriptor * field) \
        { \
            if (shouldSkipNullValue(field)) \
                return std::make_unique<ConverterToNumber<field_type_id, field_type, true, false>>(simple_writer, field); \
            else if (shouldPackRepeated(field)) \
                return std::make_unique<ConverterToNumber<field_type_id, field_type, false, true>>(simple_writer, field); \
            else \
                return std::make_unique<ConverterToNumber<field_type_id, field_type, false, false>>(simple_writer, field); \
        }

PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_INT32, Int32);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SINT32, Int32);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_UINT32, UInt32);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_INT64, Int64);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SINT64, Int64);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_UINT64, UInt64);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_FIXED32, UInt32);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SFIXED32, Int32);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_FIXED64, UInt64);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SFIXED64, Int64);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_FLOAT, float);
PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_DOUBLE, double);
#    undef PROTOBUF_WRITER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS


template <bool skip_null_value, bool pack_repeated>
class ProtobufWriter::ConverterToBool : public ConverterBaseImpl
{
public:
    using ConverterBaseImpl::ConverterBaseImpl;

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

    void writeField(bool b)
    {
        if constexpr (skip_null_value)
        {
            if (!b)
                return;
        }
        if constexpr (pack_repeated)
            simple_writer.addUIntToRepeatedPack(b);
        else
            simple_writer.writeUInt(field_number, b);
    }
};

template <>
std::unique_ptr<ProtobufWriter::IConverter> ProtobufWriter::createConverter<google::protobuf::FieldDescriptor::TYPE_BOOL>(
    const google::protobuf::FieldDescriptor * field)
{
    if (shouldSkipNullValue(field))
        return std::make_unique<ConverterToBool<true, false>>(simple_writer, field);
    else if (shouldPackRepeated(field))
        return std::make_unique<ConverterToBool<false, true>>(simple_writer, field);
    else
        return std::make_unique<ConverterToBool<false, false>>(simple_writer, field);
}


template <bool skip_null_value, bool pack_repeated>
class ProtobufWriter::ConverterToEnum : public ConverterBaseImpl
{
public:
    using ConverterBaseImpl::ConverterBaseImpl;

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

    void prepareEnumMapping8(const std::vector<std::pair<String, Int8>> & name_value_pairs) override
    {
        prepareEnumValueToPbNumberMap(name_value_pairs);
    }
    void prepareEnumMapping16(const std::vector<std::pair<String, Int16>> & name_value_pairs) override
    {
        prepareEnumValueToPbNumberMap(name_value_pairs);
    }

    void writeEnum8(Int8 value) override { writeEnum16(value); }

    void writeEnum16(Int16 value) override
    {
        int pbnumber;
        if (enum_value_always_equals_pbnumber)
            pbnumber = value;
        else
        {
            auto it = enum_value_to_pbnumber_map->find(value);
            if (it == enum_value_to_pbnumber_map->end())
                cannotConvertValue(toString(value));
            pbnumber = it->second;
        }
        writeField(pbnumber);
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
        enum_value_always_equals_pbnumber = true;
        for (const auto & name_value_pair : name_value_pairs)
        {
            Int16 value = name_value_pair.second; // NOLINT
            const auto * enum_descriptor = field->enum_type()->FindValueByName(name_value_pair.first);
            if (enum_descriptor)
            {
                enum_value_to_pbnumber_map->emplace(value, enum_descriptor->number());
                if (value != enum_descriptor->number())
                    enum_value_always_equals_pbnumber = false;
            }
            else
                enum_value_always_equals_pbnumber = false;
        }
    }

    void writeField(int enum_pbnumber)
    {
        if constexpr (skip_null_value)
        {
            if (!enum_pbnumber)
                return;
        }
        if constexpr (pack_repeated)
            simple_writer.addUIntToRepeatedPack(enum_pbnumber);
        else
            simple_writer.writeUInt(field_number, enum_pbnumber);
    }

    std::optional<std::unordered_map<StringRef, int>> enum_name_to_pbnumber_map;
    std::optional<std::unordered_map<Int16, int>> enum_value_to_pbnumber_map;
    bool enum_value_always_equals_pbnumber;
};

template <>
std::unique_ptr<ProtobufWriter::IConverter> ProtobufWriter::createConverter<google::protobuf::FieldDescriptor::TYPE_ENUM>(
    const google::protobuf::FieldDescriptor * field)
{
    if (shouldSkipNullValue(field))
        return std::make_unique<ConverterToEnum<true, false>>(simple_writer, field);
    else if (shouldPackRepeated(field))
        return std::make_unique<ConverterToEnum<false, true>>(simple_writer, field);
    else
        return std::make_unique<ConverterToEnum<false, false>>(simple_writer, field);
}


ProtobufWriter::ProtobufWriter(
    WriteBuffer & out, const google::protobuf::Descriptor * message_type, const std::vector<String> & column_names)
    : simple_writer(out)
{
    std::vector<const google::protobuf::FieldDescriptor *> field_descriptors_without_match;
    root_message = ProtobufColumnMatcher::matchColumns<ColumnMatcherTraits>(column_names, message_type, field_descriptors_without_match);
    for (const auto * field_descriptor_without_match : field_descriptors_without_match)
    {
        if (field_descriptor_without_match->is_required())
            throw Exception(
                "Output doesn't have a column named '" + field_descriptor_without_match->name()
                    + "' which is required to write the output in the protobuf format.",
                ErrorCodes::NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD);
    }
    setTraitsDataAfterMatchingColumns(root_message.get());
}

ProtobufWriter::~ProtobufWriter() = default;

void ProtobufWriter::setTraitsDataAfterMatchingColumns(Message * message)
{
    Field * parent_field = message->parent ? &message->parent->fields[message->index_in_parent] : nullptr;
    message->data.parent_field_number = parent_field ? parent_field->field_number : 0;
    message->data.is_required = parent_field && parent_field->data.is_required;

    if (parent_field && parent_field->data.is_repeatable)
        message->data.repeatable_container_message = message;
    else if (message->parent)
        message->data.repeatable_container_message = message->parent->data.repeatable_container_message;
    else
        message->data.repeatable_container_message = nullptr;

    message->data.is_group = parent_field && (parent_field->field_descriptor->type() == google::protobuf::FieldDescriptor::TYPE_GROUP);

    for (auto & field : message->fields)
    {
        field.data.is_repeatable = field.field_descriptor->is_repeated();
        field.data.is_required = field.field_descriptor->is_required();
        field.data.repeatable_container_message = message->data.repeatable_container_message;
        field.data.should_pack_repeated = shouldPackRepeated(field.field_descriptor);

        if (field.nested_message)
        {
            setTraitsDataAfterMatchingColumns(field.nested_message.get());
            continue;
        }
        switch (field.field_descriptor->type())
        {
#    define PROTOBUF_WRITER_CONVERTER_CREATING_CASE(field_type_id) \
        case field_type_id: \
            field.data.converter = createConverter<field_type_id>(field.field_descriptor); \
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
#    undef PROTOBUF_WRITER_CONVERTER_CREATING_CASE
            default:
                throw Exception(
                    String("Protobuf type '") + field.field_descriptor->type_name() + "' isn't supported", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
}

void ProtobufWriter::startMessage()
{
    current_message = root_message.get();
    current_field_index = 0;
    simple_writer.startMessage();
}

void ProtobufWriter::endMessage()
{
    if (!current_message)
        return;
    endWritingField();
    while (current_message->parent)
    {
        simple_writer.endNestedMessage(
            current_message->data.parent_field_number, current_message->data.is_group, !current_message->data.is_required);
        current_message = current_message->parent;
    }
    simple_writer.endMessage();
    current_message = nullptr;
}

bool ProtobufWriter::writeField(size_t & column_index)
{
    endWritingField();
    while (true)
    {
        if (current_field_index < current_message->fields.size())
        {
            Field & field = current_message->fields[current_field_index];
            if (!field.nested_message)
            {
                current_field = &current_message->fields[current_field_index];
                current_converter = current_field->data.converter.get();
                column_index = current_field->column_index;
                if (current_field->data.should_pack_repeated)
                    simple_writer.startRepeatedPack();
                return true;
            }
            simple_writer.startNestedMessage();
            current_message = field.nested_message.get();
            current_message->data.need_repeat = false;
            current_field_index = 0;
            continue;
        }
        if (current_message->parent)
        {
            simple_writer.endNestedMessage(
                current_message->data.parent_field_number, current_message->data.is_group, !current_message->data.is_required);
            if (current_message->data.need_repeat)
            {
                simple_writer.startNestedMessage();
                current_message->data.need_repeat = false;
                current_field_index = 0;
                continue;
            }
            current_field_index = current_message->index_in_parent + 1;
            current_message = current_message->parent;
            continue;
        }
        return false;
    }
}

void ProtobufWriter::endWritingField()
{
    if (!current_field)
        return;
    if (current_field->data.should_pack_repeated)
        simple_writer.endRepeatedPack(current_field->field_number);
    else if ((num_values == 0) && current_field->data.is_required)
        throw Exception(
            "No data for the required field '" + current_field->field_descriptor->name() + "'",
            ErrorCodes::NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD);

    current_field = nullptr;
    current_converter = nullptr;
    num_values = 0;
    ++current_field_index;
}

void ProtobufWriter::setNestedMessageNeedsRepeat()
{
    if (current_field->data.repeatable_container_message)
        current_field->data.repeatable_container_message->data.need_repeat = true;
    else
        throw Exception(
            "Cannot write more than single value to the non-repeated field '" + current_field->field_descriptor->name() + "'",
            ErrorCodes::PROTOBUF_FIELD_NOT_REPEATED);
}

}

#endif
