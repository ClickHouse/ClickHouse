#include "ProtobufWriter.h"
#if USE_PROTOBUF

#include <cassert>
#include <optional>
#include <math.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <boost/numeric/conversion/cast.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD;
    extern const int CANNOT_CONVERT_TO_PROTOBUF_TYPE;
    extern const int PROTOBUF_FIELD_NOT_REPEATED;
}


class ProtobufWriter::Converter : private boost::noncopyable
{
public:
    Converter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : simple_writer(simple_writer_), field(field_)
    {
    }

    virtual ~Converter() = default;

    virtual void writeString(const StringRef &) { cannotConvertType("String"); }

    virtual void writeInt8(Int8) { cannotConvertType("Int8"); }
    virtual void writeUInt8(UInt8) { cannotConvertType("UInt8"); }
    virtual void writeInt16(Int16) { cannotConvertType("Int16"); }
    virtual void writeUInt16(UInt16) { cannotConvertType("UInt16"); }
    virtual void writeInt32(Int32) { cannotConvertType("Int32"); }
    virtual void writeUInt32(UInt32) { cannotConvertType("UInt32"); }
    virtual void writeInt64(Int64) { cannotConvertType("Int64"); }
    virtual void writeUInt64(UInt64) { cannotConvertType("UInt64"); }
    virtual void writeUInt128(const UInt128 &) { cannotConvertType("UInt128"); }
    virtual void writeFloat32(Float32) { cannotConvertType("Float32"); }
    virtual void writeFloat64(Float64) { cannotConvertType("Float64"); }

    virtual void prepareEnumMappingInt8(const std::vector<std::pair<std::string, Int8>> &) {}
    virtual void prepareEnumMappingInt16(const std::vector<std::pair<std::string, Int16>> &) {}
    virtual void writeEnumInt8(Int8) { cannotConvertType("Enum"); }
    virtual void writeEnumInt16(Int16) { cannotConvertType("Enum"); }

    virtual void writeUUID(const UUID &) { cannotConvertType("UUID"); }
    virtual void writeDate(DayNum) { cannotConvertType("Date"); }
    virtual void writeDateTime(time_t) { cannotConvertType("DateTime"); }

    virtual void writeDecimal32(Decimal32, UInt32) { cannotConvertType("Decimal32"); }
    virtual void writeDecimal64(Decimal64, UInt32) { cannotConvertType("Decimal64"); }
    virtual void writeDecimal128(const Decimal128 &, UInt32) { cannotConvertType("Decimal128"); }

    virtual void writeAggregateFunction(const AggregateFunctionPtr &, ConstAggregateDataPtr) { cannotConvertType("AggregateFunction"); }

protected:
    void cannotConvertType(const String & type_name)
    {
        throw Exception(
            "Could not convert data type '" + type_name + "' to protobuf type '" + field->type_name() + "' (field: " + field->name() + ")",
            ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
    }

    void cannotConvertValue(const String & value)
    {
        throw Exception(
            "Could not convert value '" + value + "' to protobuf type '" + field->type_name() + "' (field: " + field->name() + ")",
            ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
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

    ProtobufSimpleWriter & simple_writer;
    const google::protobuf::FieldDescriptor * field;
};


class ProtobufWriter::ToStringConverter : public Converter
{
public:
    ToStringConverter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : Converter(simple_writer_, field_)
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
        write_field_function = skipNullValue() ? &ProtobufSimpleWriter::writeStringIfNotEmpty : &ProtobufSimpleWriter::writeString;
    }

    void (ProtobufSimpleWriter::*write_field_function)(const StringRef & str);
    WriteBufferFromOwnString text_buffer;
    std::optional<std::unordered_map<Int16, String>> enum_value_to_name_map;
};


template <typename T>
class ProtobufWriter::ToNumberConverter : public Converter
{
public:
    ToNumberConverter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : Converter(simple_writer_, field_)
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
                        ? &ProtobufSimpleWriter::packRepeatedInt32
                        : (skipNullValue() ? &ProtobufSimpleWriter::writeInt32IfNonZero : &ProtobufSimpleWriter::writeInt32);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_SINT32:
                    write_field_function = packRepeated()
                        ? &ProtobufSimpleWriter::packRepeatedSInt32
                        : (skipNullValue() ? &ProtobufSimpleWriter::writeSInt32IfNonZero : &ProtobufSimpleWriter::writeSInt32);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
                    write_field_function = packRepeated()
                        ? &ProtobufSimpleWriter::packRepeatedSFixed32
                        : (skipNullValue() ? &ProtobufSimpleWriter::writeSFixed32IfNonZero : &ProtobufSimpleWriter::writeSFixed32);
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
                        ? &ProtobufSimpleWriter::packRepeatedUInt32
                        : (skipNullValue() ? &ProtobufSimpleWriter::writeUInt32IfNonZero : &ProtobufSimpleWriter::writeUInt32);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_FIXED32:
                    write_field_function = packRepeated()
                        ? &ProtobufSimpleWriter::packRepeatedFixed32
                        : (skipNullValue() ? &ProtobufSimpleWriter::writeFixed32IfNonZero : &ProtobufSimpleWriter::writeFixed32);
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
                        ? &ProtobufSimpleWriter::packRepeatedInt64
                        : (skipNullValue() ? &ProtobufSimpleWriter::writeInt64IfNonZero : &ProtobufSimpleWriter::writeInt64);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_SINT64:
                    write_field_function = packRepeated()
                        ? &ProtobufSimpleWriter::packRepeatedSInt64
                        : (skipNullValue() ? &ProtobufSimpleWriter::writeSInt64IfNonZero : &ProtobufSimpleWriter::writeSInt64);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
                    write_field_function = packRepeated()
                        ? &ProtobufSimpleWriter::packRepeatedSFixed64
                        : (skipNullValue() ? &ProtobufSimpleWriter::writeSFixed64IfNonZero : &ProtobufSimpleWriter::writeSFixed64);
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
                        ? &ProtobufSimpleWriter::packRepeatedUInt64
                        : (skipNullValue() ? &ProtobufSimpleWriter::writeUInt64IfNonZero : &ProtobufSimpleWriter::writeUInt64);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_FIXED64:
                    write_field_function = packRepeated()
                        ? &ProtobufSimpleWriter::packRepeatedFixed64
                        : (skipNullValue() ? &ProtobufSimpleWriter::writeFixed64IfNonZero : &ProtobufSimpleWriter::writeFixed64);
                    break;
                default:
                    assert(false);
            }
        }
        else if constexpr (std::is_same_v<T, float>)
        {
            write_field_function = packRepeated()
                ? &ProtobufSimpleWriter::packRepeatedFloat
                : (skipNullValue() ? &ProtobufSimpleWriter::writeFloatIfNonZero : &ProtobufSimpleWriter::writeFloat);
        }
        else if constexpr (std::is_same_v<T, double>)
        {
            write_field_function = packRepeated()
                ? &ProtobufSimpleWriter::packRepeatedDouble
                : (skipNullValue() ? &ProtobufSimpleWriter::writeDoubleIfNonZero : &ProtobufSimpleWriter::writeDouble);
        }
        else
        {
            assert(false);
        }
    }

    void (ProtobufSimpleWriter::*write_field_function)(T value);
};


class ProtobufWriter::ToBoolConverter : public Converter
{
public:
    ToBoolConverter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : Converter(simple_writer_, field_)
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
            ? &ProtobufSimpleWriter::packRepeatedUInt32
            : (skipNullValue() ? &ProtobufSimpleWriter::writeUInt32IfNonZero : &ProtobufSimpleWriter::writeUInt32);
    }

    void (ProtobufSimpleWriter::*write_field_function)(UInt32 b);
};


class ProtobufWriter::ToEnumConverter : public Converter
{
public:
    ToEnumConverter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor * field_)
        : Converter(simple_writer_, field_)
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
            ? &ProtobufSimpleWriter::packRepeatedUInt32
            : (skipNullValue() ? &ProtobufSimpleWriter::writeUInt32IfNonZero : &ProtobufSimpleWriter::writeUInt32);
    }

    void (ProtobufSimpleWriter::*write_field_function)(UInt32 enum_number);
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
        std::unique_ptr<Converter> converter;
        switch (field->type())
        {
            case google::protobuf::FieldDescriptor::TYPE_INT32:
            case google::protobuf::FieldDescriptor::TYPE_SINT32:
            case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
                converter = std::make_unique<ToNumberConverter<Int32>>(simple_writer, field);
                break;
            case google::protobuf::FieldDescriptor::TYPE_UINT32:
            case google::protobuf::FieldDescriptor::TYPE_FIXED32:
                converter = std::make_unique<ToNumberConverter<UInt32>>(simple_writer, field);
                break;
            case google::protobuf::FieldDescriptor::TYPE_INT64:
            case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
            case google::protobuf::FieldDescriptor::TYPE_SINT64:
                converter = std::make_unique<ToNumberConverter<Int64>>(simple_writer, field);
                break;
            case google::protobuf::FieldDescriptor::TYPE_UINT64:
            case google::protobuf::FieldDescriptor::TYPE_FIXED64:
                converter = std::make_unique<ToNumberConverter<UInt64>>(simple_writer, field);
                break;
            case google::protobuf::FieldDescriptor::TYPE_FLOAT:
                converter = std::make_unique<ToNumberConverter<float>>(simple_writer, field);
                break;
            case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
                converter = std::make_unique<ToNumberConverter<double>>(simple_writer, field);
                break;
            case google::protobuf::FieldDescriptor::TYPE_BOOL:
                converter = std::make_unique<ToBoolConverter>(simple_writer, field);
                break;
            case google::protobuf::FieldDescriptor::TYPE_ENUM:
                converter = std::make_unique<ToEnumConverter>(simple_writer, field);
                break;
            case google::protobuf::FieldDescriptor::TYPE_STRING:
            case google::protobuf::FieldDescriptor::TYPE_BYTES:
                converter = std::make_unique<ToStringConverter>(simple_writer, field);
                break;
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

void ProtobufWriter::writeNumber(Int8 value)
{
    current_converter->writeInt8(value);
}

void ProtobufWriter::writeNumber(UInt8 value)
{
    current_converter->writeUInt8(value);
}

void ProtobufWriter::writeNumber(Int16 value)
{
    current_converter->writeInt16(value);
}

void ProtobufWriter::writeNumber(UInt16 value)
{
    current_converter->writeUInt16(value);
}

void ProtobufWriter::writeNumber(Int32 value)
{
    current_converter->writeInt32(value);
}

void ProtobufWriter::writeNumber(UInt32 value)
{
    current_converter->writeUInt32(value);
}

void ProtobufWriter::writeNumber(Int64 value)
{
    current_converter->writeInt64(value);
}

void ProtobufWriter::writeNumber(UInt64 value)
{
    current_converter->writeUInt64(value);
}

void ProtobufWriter::writeNumber(UInt128 value)
{
    current_converter->writeUInt128(value);
}

void ProtobufWriter::writeNumber(Float32 value)
{
    current_converter->writeFloat32(value);
}

void ProtobufWriter::writeNumber(Float64 value)
{
    current_converter->writeFloat64(value);
}

void ProtobufWriter::writeString(const StringRef & str)
{
    current_converter->writeString(str);
}

void ProtobufWriter::prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & enum_values)
{
    current_converter->prepareEnumMappingInt8(enum_values);
}

void ProtobufWriter::prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & enum_values)
{
    current_converter->prepareEnumMappingInt16(enum_values);
}

void ProtobufWriter::writeEnum(Int8 value)
{
    current_converter->writeEnumInt8(value);
}

void ProtobufWriter::writeEnum(Int16 value)
{
    current_converter->writeEnumInt16(value);
}

void ProtobufWriter::writeUUID(const UUID & uuid)
{
    current_converter->writeUUID(uuid);
}

void ProtobufWriter::writeDate(DayNum date)
{
    current_converter->writeDate(date);
}

void ProtobufWriter::writeDateTime(time_t tm)
{
    current_converter->writeDateTime(tm);
}

void ProtobufWriter::writeDecimal(Decimal32 decimal, UInt32 scale)
{
    current_converter->writeDecimal32(decimal, scale);
}

void ProtobufWriter::writeDecimal(Decimal64 decimal, UInt32 scale)
{
    current_converter->writeDecimal64(decimal, scale);
}

void ProtobufWriter::writeDecimal(const Decimal128 & decimal, UInt32 scale)
{
    current_converter->writeDecimal128(decimal, scale);
}

void ProtobufWriter::writeAggregateFunction(const AggregateFunctionPtr & function, ConstAggregateDataPtr place)
{
    current_converter->writeAggregateFunction(function, place);
}

}

#endif
