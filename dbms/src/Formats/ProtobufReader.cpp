#include "config_formats.h"
#if USE_PROTOBUF

#include "ProtobufReader.h"

#include <AggregateFunctions/IAggregateFunction.h>
#include <boost/numeric/conversion/cast.hpp>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <optional>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_PROTOBUF_FORMAT;
    extern const int PROTOBUF_BAD_CAST;
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

    // The following condition must always be true:
    // any_cursor_position < min(END_OF_VARINT, END_OF_GROUP)
    // This inequation helps to check conditions in SimpleReader.
    constexpr UInt64 END_OF_VARINT = static_cast<UInt64>(-1);
    constexpr UInt64 END_OF_GROUP = static_cast<UInt64>(-2);

    Int64 decodeZigZag(UInt64 n) { return static_cast<Int64>((n >> 1) ^ (~(n & 1) + 1)); }

    [[noreturn]] void throwUnknownFormat()
    {
        throw Exception("Protobuf messages are corrupted or don't match the provided schema. Please note that Protobuf stream is length-delimited: every message is prefixed by its length in varint.", ErrorCodes::UNKNOWN_PROTOBUF_FORMAT);
    }
}


// SimpleReader is an utility class to deserialize protobufs.
// Knows nothing about protobuf schemas, just provides useful functions to deserialize data.
ProtobufReader::SimpleReader::SimpleReader(ReadBuffer & in_)
    : in(in_)
    , cursor(0)
    , current_message_level(0)
    , current_message_end(0)
    , field_end(0)
    , last_string_pos(-1)
{
}

bool ProtobufReader::SimpleReader::startMessage()
{
    // Start reading a root message.
    assert(!current_message_level);
    if (unlikely(in.eof()))
        return false;
    size_t size_of_message = readVarint();
    current_message_end = cursor + size_of_message;
    ++current_message_level;
    field_end = cursor;
    return true;
}

void ProtobufReader::SimpleReader::endMessage(bool ignore_errors)
{
    if (!current_message_level)
        return;

    UInt64 root_message_end = (current_message_level == 1) ? current_message_end : parent_message_ends.front();
    if (cursor != root_message_end)
    {
        if (cursor < root_message_end)
            ignore(root_message_end - cursor);
        else if (ignore_errors)
            moveCursorBackward(cursor - root_message_end);
        else
            throwUnknownFormat();
    }

    current_message_level = 0;
    parent_message_ends.clear();
}

void ProtobufReader::SimpleReader::startNestedMessage()
{
    assert(current_message_level >= 1);
    // Start reading a nested message which is located inside a length-delimited field
    // of another message.
    parent_message_ends.emplace_back(current_message_end);
    current_message_end = field_end;
    ++current_message_level;
    field_end = cursor;
}

void ProtobufReader::SimpleReader::endNestedMessage()
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
    field_end = cursor;
}

bool ProtobufReader::SimpleReader::readFieldNumber(UInt32 & field_number)
{
    assert(current_message_level);
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
        return false;

    UInt64 varint = readVarint();
    if (unlikely(varint & (static_cast<UInt64>(0xFFFFFFFF) << 32)))
        throwUnknownFormat();
    UInt32 key = static_cast<UInt32>(varint);
    field_number = (key >> 3);
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

bool ProtobufReader::SimpleReader::readUInt(UInt64 & value)
{
    if (unlikely(cursor >= field_end))
        return false;
    value = readVarint();
    if (field_end == END_OF_VARINT)
        field_end = cursor;
    return true;
}

bool ProtobufReader::SimpleReader::readInt(Int64 & value)
{
    UInt64 varint;
    if (!readUInt(varint))
        return false;
    value = static_cast<Int64>(varint);
    return true;
}

bool ProtobufReader::SimpleReader::readSInt(Int64 & value)
{
    UInt64 varint;
    if (!readUInt(varint))
        return false;
    value = decodeZigZag(varint);
    return true;
}

template<typename T>
bool ProtobufReader::SimpleReader::readFixed(T & value)
{
    if (unlikely(cursor >= field_end))
        return false;
    readBinary(&value, sizeof(T));
    return true;
}

bool ProtobufReader::SimpleReader::readStringInto(PaddedPODArray<UInt8> & str)
{
    if (unlikely(cursor == last_string_pos))
        return false; /// We don't want to read the same empty string again.
    last_string_pos = cursor;
    if (unlikely(cursor > field_end))
        throwUnknownFormat();
    size_t length = field_end - cursor;
    size_t old_size = str.size();
    str.resize(old_size + length);
    readBinary(reinterpret_cast<char*>(str.data() + old_size), length);
    return true;
}

void ProtobufReader::SimpleReader::readBinary(void* data, size_t size)
{
    in.readStrict(reinterpret_cast<char*>(data), size);
    cursor += size;
}

void ProtobufReader::SimpleReader::ignore(UInt64 num_bytes)
{
    in.ignore(num_bytes);
    cursor += num_bytes;
}

void ProtobufReader::SimpleReader::moveCursorBackward(UInt64 num_bytes)
{
    if (in.offset() < num_bytes)
        throwUnknownFormat();
    in.position() -= num_bytes;
    cursor -= num_bytes;
}

UInt64 ProtobufReader::SimpleReader::continueReadingVarint(UInt64 first_byte)
{
    UInt64 result = (first_byte & ~static_cast<UInt64>(0x80));
    char c;

#define PROTOBUF_READER_READ_VARINT_BYTE(byteNo) \
    in.readStrict(c); \
    ++cursor; \
    if constexpr (byteNo < 10) \
    { \
        result |= static_cast<UInt64>(static_cast<UInt8>(c)) << (7 * (byteNo - 1)); \
        if (likely(!(c & 0x80))) \
            return result; \
    } \
    else \
    { \
        if (likely(c == 1)) \
            return result; \
    } \
    if constexpr (byteNo < 9) \
        result &= ~(static_cast<UInt64>(0x80) << (7 * (byteNo - 1)));
    PROTOBUF_READER_READ_VARINT_BYTE(2)
    PROTOBUF_READER_READ_VARINT_BYTE(3)
    PROTOBUF_READER_READ_VARINT_BYTE(4)
    PROTOBUF_READER_READ_VARINT_BYTE(5)
    PROTOBUF_READER_READ_VARINT_BYTE(6)
    PROTOBUF_READER_READ_VARINT_BYTE(7)
    PROTOBUF_READER_READ_VARINT_BYTE(8)
    PROTOBUF_READER_READ_VARINT_BYTE(9)
    PROTOBUF_READER_READ_VARINT_BYTE(10)
#undef PROTOBUF_READER_READ_VARINT_BYTE

    throwUnknownFormat();
}

void ProtobufReader::SimpleReader::ignoreVarint()
{
    char c;

#define PROTOBUF_READER_IGNORE_VARINT_BYTE(byteNo) \
    in.readStrict(c); \
    ++cursor; \
    if constexpr (byteNo < 10) \
    { \
        if (likely(!(c & 0x80))) \
            return; \
    } \
    else \
    { \
        if (likely(c == 1)) \
            return; \
    }
    PROTOBUF_READER_IGNORE_VARINT_BYTE(1)
    PROTOBUF_READER_IGNORE_VARINT_BYTE(2)
    PROTOBUF_READER_IGNORE_VARINT_BYTE(3)
    PROTOBUF_READER_IGNORE_VARINT_BYTE(4)
    PROTOBUF_READER_IGNORE_VARINT_BYTE(5)
    PROTOBUF_READER_IGNORE_VARINT_BYTE(6)
    PROTOBUF_READER_IGNORE_VARINT_BYTE(7)
    PROTOBUF_READER_IGNORE_VARINT_BYTE(8)
    PROTOBUF_READER_IGNORE_VARINT_BYTE(9)
    PROTOBUF_READER_IGNORE_VARINT_BYTE(10)
#undef PROTOBUF_READER_IGNORE_VARINT_BYTE

    throwUnknownFormat();
}

void ProtobufReader::SimpleReader::ignoreGroup()
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


namespace
{
    [[noreturn]] void throwUnknownFormat()
    {
        throw Exception("Protobuf messages are corrupted or don't match the provided schema. Please note that Protobuf stream is length-delimited: every message is prefixed by its length in varint.", ErrorCodes::UNKNOWN_PROTOBUF_FORMAT);
    }

    UInt64 continueReadingVarint_2(UInt64 first_byte, size_t & cursor)
    {
        UInt64 result = (first_byte & ~static_cast<UInt64>(0x80));
        char c;

    #define PROTOBUF_READER_READ_VARINT_BYTE(byteNo) \
        in.readStrict(c); \
        ++cursor; \
        if constexpr (byteNo < 10) \
        { \
            result |= static_cast<UInt64>(static_cast<UInt8>(c)) << (7 * (byteNo - 1)); \
            if (likely(!(c & 0x80))) \
                return result; \
        } \
        else \
        { \
            if (likely(c == 1)) \
                return result; \
        } \
        if constexpr (byteNo < 9) \
            result &= ~(static_cast<UInt64>(0x80) << (7 * (byteNo - 1)));
        PROTOBUF_READER_READ_VARINT_BYTE(2)
        PROTOBUF_READER_READ_VARINT_BYTE(3)
        PROTOBUF_READER_READ_VARINT_BYTE(4)
        PROTOBUF_READER_READ_VARINT_BYTE(5)
        PROTOBUF_READER_READ_VARINT_BYTE(6)
        PROTOBUF_READER_READ_VARINT_BYTE(7)
        PROTOBUF_READER_READ_VARINT_BYTE(8)
        PROTOBUF_READER_READ_VARINT_BYTE(9)
        PROTOBUF_READER_READ_VARINT_BYTE(10)
    #undef PROTOBUF_READER_READ_VARINT_BYTE

        throwUnknownFormat();
    }

    UInt64 ALWAYS_INLINE readVarint_2(ReadBuffer & in, size_t & cursor)
    {
        char c;
        in.readStrict(c);
        ++cursor;
        UInt64 first_byte = static_cast<UInt8>(c);
        if (likely(!(c & 0x80)))
            return first_byte;
        return continueReadingVarint_2(first_byte, cursor);
    }

    Int64 decodeZigZag_2(UInt64 n)
    {
        return static_cast<Int64>((n >> 1) ^ (~(n & 1) + 1));
    }

    bool doesColumnNameCorrespondProtobufFieldName(const String & column_name, const String & field_name)
    {
        size_t sz = column_name.size();
        if (field_name.size() != sz)
            return false;
        for (size_t i = 0; i != sz; ++i)
        {
            char c1 = tolower(column_name[i]);
            char c2 = tolower(field_name[i]);
            if (c1 != c2)
            {
                if (c1 == '.')
                    c1 = '_';
                if (c2 == '.')
                    c2 = '_';
                if (c1 != c2)
                    return false;
            }
        }
        return true;
    }
}


class ProtobufReader::IColumnsFiller
{
public:
    IColumnsFiller(size_t field_number_) : field_number(field_number_) {}
    virtual ~IColumnFiller() = default;
    virtual void setDestinationColumns(const ColumnsWithTypeAndName & columns) = 0;
    virtual void readValue(ReadBuffer & in, size_t & cursor, const size_t * row_index, size_t row_index_size) = 0;
    static std::unique_ptr<IColumnFiller> create(const google::protobuf::FieldDescriptor & field, const ColumnsWithTypeAndName & header);
};


class ProtobufReader::ColumnsFillerFromMessage : public IColumnsFiller
{
public:
    ColumnsFillerFromMessage(const google::protobuf::Descriptor & message, const google::protobuf::FieldDescriptor * containing_field, ColumnsWithTypeAndName & header)
    {
        //for (int i = 0; i !=
    }

    void setDestinationColumns(const ColumnsWithTypeAndName & columns) override
    {
        for (auto & columns_filler : columns_fillers)
            columns_filler->setDestinationColumns(columns);
    }

    void readAndPushValue(ReadBuffer & in, size_t & cursor, std::vector<UInt8> & row) override
    {
        size_t message_size = readVarint_2(in, cursor);
        size_t message_end = cursor + message_size;
        size_t current_field_index = 0;
        field_used.clear();
        field_used.resize(field_number_to_column_filler_pairs.size(), 0);

        while (cursor < message_end)
        {
            UInt64 varint = readVarint_2(in, cursor);
            if (unlikely(varint & (static_cast<UInt64>(0xFFFFFFFF) << 32)))
                throwUnknownFormat();
            UInt32 key = static_cast<UInt32>(varint);
            size_t field_number = (key >> 3);
            WireType wire_type = static_cast<WireType>(key & 0x07);

            IColumnFiller * column = nullptr;
            while (current_field_index < field_number_to_column_filler_pairs.size())
            {
                size_t current_field_number = field_number_to_column_filler_pairs[current_field_index].first;
                if (current_field_number == field_number)
                {
                    column = field_number_to_column_filler_pairs[current_field_index].second.get();
                    break;
                }
                if (current_field_number > field_number)
                {
                    auto it = field_number_to_index_map.find(field_number);
                    if (it != field_number_to_index_map.end())
                    {
                        current_field_index = it->second;
                        column = field_number_to_column_filler_pairs[current_field_index].second.get();
                    }
                    break;
                }
                ++current_field_index;
            }

            if (!column)
            {
                /// ignore
            }

            column->readAndPushValue(in, cursor, allow_add_row);
            field_used[field_index] = 1;
        }

        for (field_index = 0; i != row_added.size(); ++field_index)
        {
            if (!field_used[field_index])
                field_number_to_column_filler_pairs.second->pushDefault();
        }
    }

    virtual void pushDefault() = 0;

private:
    std::vector<std::unique_ptr<IColumnsFiller>> column_fillers;
    std::unordered_map<size_t, size_t> field_number_to_index_map;
    std::vector<UInt8> row_added;
}


std::unique_ptr<ProtobufReader::IColumnFiller>
IColumnFiller::create(const google::protobuf::FieldDescriptor & field, ColumnsWithTypeAndName & columns)
{
    auto it = boost::find_if(columns, [&field](const ColumnWithTypeAndName & column)
    {
        return doesColumnNameCorrespondProtobufFieldName(column.name, field.name());
    });

    if (it == columns.end())
        return nullptr;

    auto column = *it;
    columns.erase(it);

    switch (column.type->getTypeId())
    {
        case TypeIndex::UInt8: return std::make_unique<NumberColumnFiller<UInt8>>(field, column.name);
        case TypeIndex::UInt16: return std::make_unique<NumberColumnFiller<UInt16>>(field, column.name);
        case TypeIndex::UInt32: return std::make_unique<NumberColumnFiller<UInt32>>(column.name, *field);
        case TypeIndex::UInt64: return std::make_unique<NumberColumnFiller<UInt64>>(column_name, *field);
        case TypeIndex::Int8: return std::make_unique<NumberColumnFiller<Int8>>(column_name, *field);
        case TypeIndex::Int16: return std::make_unique<NumberColumnFiller<Int16>>(column_name, *field);
        case TypeIndex::Int32: return std::make_unique<NumberColumnFiller<Int32>>(column_name, *field);
        case TypeIndex::Int64: return std::make_unique<NumberColumnFiller<Int64>>(column_name, *field);
        case TypeIndex::Float32: return std::make_unique<NumberColumnFiller<Float32>>(column_name, *field);
        case TypeIndex::Float64: return std::make_unique<NumberColumnFiller<Float64>>(column_name, *field);
        case TypeIndex::String: return std::make_unique<StringColumnFiller>(column_name, *field);
        case TypeIndex::FixedString: return std::make_unique<FixedColumnStringFiller>(column_name, *field);
        case TypeIndex::Enum8:
            return std::make_unique<EnumNode<Int8>>(static_cast<const DataTypeEnum8 &>(*type).getValues());
        case TypeIndex::Enum16:
            return std::make_unique<EnumNode<Int16>>(static_cast<const DataTypeEnum16 &>(*type).getValues());
        case TypeIndex::Nullable:
        {
            return std::make_unique<NullableColumnFiller>(column, message);
                        createColumnFiller(column_name, static_cast<const DataTypeNullable &>(*column_type).getNestedType()), message);
        }
        case TypeIndex::Array:
        {
            return std::make_unique<ArrayNode>(build(function_name, static_cast<const DataTypeArray &>(*type).getNestedType()));
        }
        case TypeIndex::Tuple:
        {
            const auto & tuple = static_cast<const DataTypeTuple &>(*type);
            const auto & tuple_elements = tuple.getElements();
            std::vector<std::unique_ptr<Node>> elements;
            for (const auto & tuple_element : tuple_elements)
                elements.emplace_back(build(function_name, tuple_element));
            return std::make_unique<TupleNode>(std::move(elements), tuple.haveExplicitNames() ? tuple.getElementNames() : Strings{});
        }
        default:
            throw Exception{"Function " + String(function_name) + " doesn't support the return type schema: " + type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }
}


class ProtobufReader::ColumnsFillerForMessage : public ProtobufReader::IColumnFiller
{
public:
    ColumnsFillerForMessage(const std::vector<std::pair<IColumnFiller *, const google::protobuf::FieldDescriptor *>> & fields);

    void readValueAndPush(ReadBuffer & in) override
    {
        try
        {

        }
    }

    void pushDefault() override
    {

    }

    std::vector<
};


class ProtobufReader::DefaultColumnFiller : public ProtobufReader::IColumnFiller
{
public:
    DefaultColumnFiller(const String & dest_column_name_) : default_column_name(default_column_name_) {}

    void setDestinationColumns(ColumnsWithTypeAndName & columns) override
    {
        auto it = boost::range::find_if(columns, [](const ColumnsWithTypeAndName & column) { return column.name == dest_column_name; });
        dest_column = it->column;
    }

    void readAndPushValue(ReadBuffer & in)
    {
        throw Exception{"DefaultColumnFiller::readAndPushValue() shouldn't be invoked", ErrorCodes::LOGICAL_ERROR};
    }
    void pushDefault() { dest_column->insertDefault(); }

private:
    String dest_column_name;
    ColumnPtr dest_column;
};


template <typename NumberType>
class ProtobufReader::NumberColumnFiller : public ProtobufReader::IColumnFiller
{
public:
    NumberColumnFiller(const String & dest_column_name_, const google::protobuf::FieldDescriptor & field_)
        : dest_column_name(dest_column_name_), field(field_)
    {
        switch (field.cpp_type())
        {
            case google::protobuf::FieldDescriptor::CPPTYPE_INT32: default_value = castNumber(field.default_value_int32()); break;
            case google::protobuf::FieldDescriptor::CPPTYPE_UINT32: default_value = castNumber(field.default_value_uint32()); break;
            case google::protobuf::FieldDescriptor::CPPTYPE_INT64: default_value = castNumber(field.default_value_int64()); break;
            case google::protobuf::FieldDescriptor::CPPTYPE_UINT64: default_value = castNumber(field.default_value_uint64()); break;
            case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT: default_value = castNumber(field.default_value_float()); break;
            case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE: default_value = castNumber(field.default_value_double()); break;
            case google::protobuf::FieldDescriptor::CPPTYPE_STRING: default_value = parseFromString(field.default_value_string()); break;
            default:
                throw Exception("Unexpected field type: " + std::to_string(static_cast<int>(field.type())), ErrorCodes::LOGICAL_ERROR);
        }
    }

    void setDestinationColumns(const ColumnsWithTypeAndName & columns) override
    {
        auto it = boost::range::find_if(columns, [](const ColumnsWithTypeAndName & column) { return column.name == dest_column_name; });
        dest_column = checkAndGetColumn<ColumnVector<NumberType>>(it->column);
    }

    void readAndPushValue(ReadBuffer & in, IColumn & out)
    {
        switch (field.type())
        {
            case google::protobuf::FieldDescriptor::TYPE_INT32: pushNumber(readVarint(in); break;
            case google::protobuf::FieldDescriptor::TYPE_UINT32: pushNumber(readVarint(in)); break;
            case google::protobuf::FieldDescriptor::TYPE_SINT32: pushNumber(decodeZigZag(readVarint(in))); break;
            case google::protobuf::FieldDescriptor::TYPE_FIXED32: pushNumber(readFixed<UInt32>(in)); break;
            case google::protobuf::FieldDescriptor::TYPE_SFIXED32: pushNumber(readFixed<Int32>(in)); break;
            case google::protobuf::FieldDescriptor::TYPE_INT64: pushNumber(readVarint(in)); break;
            case google::protobuf::FieldDescriptor::TYPE_INT64: pushNumber(readVarint(in)); break;
            case google::protobuf::FieldDescriptor::TYPE_SINT64: pushNumber(decodeZigZag(readVarint(in))); break;
            case google::protobuf::FieldDescriptor::TYPE_FIXED64: pushNumber(readFixed<UInt64>(in)); break;
            case google::protobuf::FieldDescriptor::TYPE_SFIXED64: pushNumber(readFixed<Int64>(in)); break;
            case google::protobuf::FieldDescriptor::TYPE_FLOAT: pushNumber(readFixed<float>(in)); break;
            case google::protobuf::FieldDescriptor::TYPE_DOUBLE: pushNumber(readFixed<double>(in)); break;
            case google::protobuf::FieldDescriptor::TYPE_BOOL: pushNumber(readVarint(in) ? NumberType{1} : NumberType{0}); break;
            case google::protobuf::FieldDescriptor::TYPE_STRING:
            case google::protobuf::FieldDescriptor::TYPE_BYTES:
            {
                readString(tmp_string, in);
                pushNumber(parseFromString<NumberType>(tmp_string));
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_ENUM: pushNumber(readVarint(in)); break;
            default:
                throw Exception("Unexpected field type: " + std::to_string(static_cast<int>(field.type())), ErrorCodes::LOGICAL_ERROR);
        }
    }

    void pushDefault(IColumn & out) override { pushNumber(default_value); }

private:
    template <typename T>
    void pushNumber(T value)
    {
        dest_column.insertValue(castNumber(value));
    }

    template <typename From>
    NumberType castNumber(From value)
    {
        NumberType casted;
        if (accurate::convertNumeric(value, &casted))
            return casted;
        throw Exception(
            String("Could not convert type '") + field.type_name() + "' from protobuf field '" + field.name() + "' to data type '"
                + type_name + "'",
            ErrorCodes::PROTOBUF_BAD_CAST);
    }

    const google::protobuf::FieldDescriptor & field;
    ColumnVector<NumberType> * dest_column = nullptr;
    NumberType default_value;
};


template <bool is_fixed>
class ProtobufReader::StringColumnFiller : public ProtobufReader::IColumnFiller
{
public:
    void readAndPushValue(ReadBuffer & in) override
    {

    }

    void pushDefault() override
    {

    }
};


class ProtobufReader::NullableColumnFiller : public ProtobufReader::IColumnFiller
{
public:
    NullableColumnFiller(const ColumnWithTypeAndName & column_, const google::protobuf::FieldDescriptor & field_)
        : dest_column_name(column_.name),
          nested_type(typeid_cast<DataTypeNullable *>(column.type))
          nested_filler(create({nested_type, column_.name}, field_)) {}

    void setDestinationColumns(const ColumnsWithTypeAndName & columns)
    {
        auto it = boost::range::find_if(columns, [](const ColumnsWithTypeAndName & column) { return column.name == dest_column_name; });
        dest_column = typeid_cast<ColumnNullable *>(it->column);
        nested_filler.setDestinationColumns({{dest_column->getNestedColumn(), nested_type, column_name}});
    }

    void readAndPushValue(ReadBuffer & in, bool allow_add_row) override
    {
        auto & nested_column = dest_column->nested_column;
        size_t old_size = nested_column.size();
        try
        {
            nested_column_filler.readAndPushValue(in, allow_add_row);
            if (allow_add_row)
                col.getNullMapData().push_back(0);
            else
                col.getNullMapData().back() = 0;
        }
        catch (...)
        {
            nested_column.popBack(nested_column.size() - old_size);
            col.getNullMapData().resize_assume_reserved(old_size);
            throw;
        }
    }

    void pushDefault() override
    {
        column->insertDefault();
    }

private:
    String column_name;
    NullableColumn * column = nullptr;
    DataTypePtr nested_type;
    std::unique_ptr<IColumnFiller> nested_filler;
};


class ProtobufReader::ArrayColumnFiller : public ProtobufReader::IColumnFiller
{
public:
    ArrayColumnFiller(const ColumnWithTypeAndName & column_, const google::protobuf::FieldDescriptor & field_)
        : dest_column_name(column_.name),
          nested_type(typeid_cast<DataTypeNullable *>(column.type))
          nested_filler(create({nested_type, column_.name}, field_)) {}

    void setDestinationColumns(const ColumnsWithTypeAndName & columns)
    {
        auto it = boost::range::find_if(columns, [](const ColumnsWithTypeAndName & column) { return column.name == dest_column_name; });
        dest_column = typeid_cast<ColumnNullable *>(it->column);
        nested_filler.setDestinationColumns({{dest_column->getNestedColumn(), nested_type, column_name}});
    }

    void readAndPushValue(ReadBuffer & in) override
    {
        nested_filler.readAndPushValue(in);
        dest_column.getNullMapColumn().push_back(0);
    }

    void pushDefault() override
    {
        dest_column->insertDefault();
    }

private:
    NullableColumn * dest_column = nullptr;
    DataTypePtr nested_type;
    std::unique_ptr<IColumnFiller> nested_filler;
};


// Implementation for a converter from any protobuf field type to any DB data type.
class ProtobufReader::ConverterBaseImpl : public ProtobufReader::IConverter
{
public:
    ConverterBaseImpl(SimpleReader & simple_reader_, const google::protobuf::FieldDescriptor * field_)
        : simple_reader(simple_reader_), field(field_) {}

    bool readStringInto(PaddedPODArray<UInt8> &) override
    {
        cannotConvertType("String");
    }

    bool readInt8(Int8 &) override
    {
        cannotConvertType("Int8");
    }

    bool readUInt8(UInt8 &) override
    {
        cannotConvertType("UInt8");
    }

    bool readInt16(Int16 &) override
    {
        cannotConvertType("Int16");
    }

    bool readUInt16(UInt16 &) override
    {
        cannotConvertType("UInt16");
    }

    bool readInt32(Int32 &) override
    {
        cannotConvertType("Int32");
    }

    bool readUInt32(UInt32 &) override
    {
        cannotConvertType("UInt32");
    }

    bool readInt64(Int64 &) override
    {
        cannotConvertType("Int64");
    }

    bool readUInt64(UInt64 &) override
    {
        cannotConvertType("UInt64");
    }

    bool readUInt128(UInt128 &) override
    {
        cannotConvertType("UInt128");
    }

    bool readFloat32(Float32 &) override
    {
        cannotConvertType("Float32");
    }

    bool readFloat64(Float64 &) override
    {
        cannotConvertType("Float64");
    }

    void prepareEnumMapping8(const std::vector<std::pair<std::string, Int8>> &) override {}
    void prepareEnumMapping16(const std::vector<std::pair<std::string, Int16>> &) override {}

    bool readEnum8(Int8 &) override
    {
        cannotConvertType("Enum");
    }

    bool readEnum16(Int16 &) override
    {
        cannotConvertType("Enum");
    }

    bool readUUID(UUID &) override
    {
        cannotConvertType("UUID");
    }

    bool readDate(DayNum &) override
    {
        cannotConvertType("Date");
    }

    bool readDateTime(time_t &) override
    {
        cannotConvertType("DateTime");
    }

    bool readDecimal32(Decimal32 &, UInt32, UInt32) override
    {
        cannotConvertType("Decimal32");
    }

    bool readDecimal64(Decimal64 &, UInt32, UInt32) override
    {
        cannotConvertType("Decimal64");
    }

    bool readDecimal128(Decimal128 &, UInt32, UInt32) override
    {
        cannotConvertType("Decimal128");
    }

    bool readAggregateFunction(const AggregateFunctionPtr &, AggregateDataPtr, Arena &) override
    {
        cannotConvertType("AggregateFunction");
    }

protected:
    [[noreturn]] void cannotConvertType(const String & type_name)
    {
        throw Exception(
            String("Could not convert type '") + field->type_name() + "' from protobuf field '" + field->name() + "' to data type '"
                + type_name + "'",
            ErrorCodes::PROTOBUF_BAD_CAST);
    }

    [[noreturn]] void cannotConvertValue(const String & value, const String & type_name)
    {
        throw Exception(
            "Could not convert value '" + value + "' from protobuf field '" + field->name() + "' to data type '" + type_name + "'",
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
            cannotConvertValue(toString(value), TypeName<To>::get());
        }
        return result;
    }

    template <typename To>
    To parseFromString(const PaddedPODArray<UInt8> & str)
    {
        try
        {
            To result;
            ReadBufferFromString buf(str);
            readText(result, buf);
            return result;
        }
        catch (...)
        {
            cannotConvertValue(StringRef(str.data(), str.size()).toString(), TypeName<To>::get());
        }
    }

    SimpleReader & simple_reader;
    const google::protobuf::FieldDescriptor * field;
};


class ProtobufReader::ConverterFromString : public ConverterBaseImpl
{
public:
    using ConverterBaseImpl::ConverterBaseImpl;

    bool readStringInto(PaddedPODArray<UInt8> & str) override { return simple_reader.readStringInto(str); }

    bool readInt8(Int8 & value) override { return readNumeric(value); }
    bool readUInt8(UInt8 & value) override { return readNumeric(value); }
    bool readInt16(Int16 & value) override { return readNumeric(value); }
    bool readUInt16(UInt16 & value) override { return readNumeric(value); }
    bool readInt32(Int32 & value) override { return readNumeric(value); }
    bool readUInt32(UInt32 & value) override { return readNumeric(value); }
    bool readInt64(Int64 & value) override { return readNumeric(value); }
    bool readUInt64(UInt64 & value) override { return readNumeric(value); }
    bool readFloat32(Float32 & value) override { return readNumeric(value); }
    bool readFloat64(Float64 & value) override { return readNumeric(value); }

    void prepareEnumMapping8(const std::vector<std::pair<String, Int8>> & name_value_pairs) override
    {
        prepareEnumNameToValueMap(name_value_pairs);
    }
    void prepareEnumMapping16(const std::vector<std::pair<String, Int16>> & name_value_pairs) override
    {
        prepareEnumNameToValueMap(name_value_pairs);
    }

    bool readEnum8(Int8 & value) override { return readEnum(value); }
    bool readEnum16(Int16 & value) override { return readEnum(value); }

    bool readUUID(UUID & uuid) override
    {
        if (!readTempString())
            return false;
        ReadBufferFromString buf(temp_string);
        readUUIDText(uuid, buf);
        return true;
    }

    bool readDate(DayNum & date) override
    {
        if (!readTempString())
            return false;
        ReadBufferFromString buf(temp_string);
        readDateText(date, buf);
        return true;
    }

    bool readDateTime(time_t & tm) override
    {
        if (!readTempString())
            return false;
        ReadBufferFromString buf(temp_string);
        readDateTimeText(tm, buf);
        return true;
    }

    bool readDecimal32(Decimal32 & decimal, UInt32 precision, UInt32 scale) override { return readDecimal(decimal, precision, scale); }
    bool readDecimal64(Decimal64 & decimal, UInt32 precision, UInt32 scale) override { return readDecimal(decimal, precision, scale); }
    bool readDecimal128(Decimal128 & decimal, UInt32 precision, UInt32 scale) override { return readDecimal(decimal, precision, scale); }

    bool readAggregateFunction(const AggregateFunctionPtr & function, AggregateDataPtr place, Arena & arena) override
    {
        if (!readTempString())
            return false;
        ReadBufferFromString buf(temp_string);
        function->deserialize(place, buf, &arena);
        return true;
    }

private:
    bool readTempString()
    {
        temp_string.clear();
        return simple_reader.readStringInto(temp_string);
    }

    template <typename T>
    bool readNumeric(T & value)
    {
        if (!readTempString())
            return false;
        value = parseFromString<T>(temp_string);
        return true;
    }

    template<typename T>
    bool readEnum(T & value)
    {
        if (!readTempString())
            return false;
        StringRef ref(temp_string.data(), temp_string.size());
        auto it = enum_name_to_value_map->find(ref);
        if (it == enum_name_to_value_map->end())
            cannotConvertValue(ref.toString(), "Enum");
        value = static_cast<T>(it->second);
        return true;
    }

    template <typename T>
    bool readDecimal(Decimal<T> & decimal, UInt32 precision, UInt32 scale)
    {
        if (!readTempString())
            return false;
        ReadBufferFromString buf(temp_string);
        DataTypeDecimal<Decimal<T>>::readText(decimal, buf, precision, scale);
        return true;
    }

    template <typename T>
    void prepareEnumNameToValueMap(const std::vector<std::pair<String, T>> & name_value_pairs)
    {
        if (likely(enum_name_to_value_map.has_value()))
            return;
        enum_name_to_value_map.emplace();
        for (const auto & name_value_pair : name_value_pairs)
            enum_name_to_value_map->emplace(name_value_pair.first, name_value_pair.second);
    }

    PaddedPODArray<UInt8> temp_string;
    std::optional<std::unordered_map<StringRef, Int16>> enum_name_to_value_map;
};

#define PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_STRINGS(field_type_id) \
    template <> \
    std::unique_ptr<ProtobufReader::IConverter> ProtobufReader::createConverter<field_type_id>( \
        const google::protobuf::FieldDescriptor * field) \
    { \
        return std::make_unique<ConverterFromString>(simple_reader, field); \
    }
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_STRINGS(google::protobuf::FieldDescriptor::TYPE_STRING)
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_STRINGS(google::protobuf::FieldDescriptor::TYPE_BYTES)
#undef PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_STRINGS


template <int field_type_id, typename FromType>
class ProtobufReader::ConverterFromNumber : public ConverterBaseImpl
{
public:
    using ConverterBaseImpl::ConverterBaseImpl;

    bool readStringInto(PaddedPODArray<UInt8> & str) override
    {
        FromType number;
        if (!readField(number))
            return false;
        WriteBufferFromVector<PaddedPODArray<UInt8>> buf(str);
        writeText(number, buf);
        return true;
    }

    bool readInt8(Int8 & value) override { return readNumeric(value); }
    bool readUInt8(UInt8 & value) override { return readNumeric(value); }
    bool readInt16(Int16 & value) override { return readNumeric(value); }
    bool readUInt16(UInt16 & value) override { return readNumeric(value); }
    bool readInt32(Int32 & value) override { return readNumeric(value); }
    bool readUInt32(UInt32 & value) override { return readNumeric(value); }
    bool readInt64(Int64 & value) override { return readNumeric(value); }
    bool readUInt64(UInt64 & value) override { return readNumeric(value); }
    bool readFloat32(Float32 & value) override { return readNumeric(value); }
    bool readFloat64(Float64 & value) override { return readNumeric(value); }

    bool readEnum8(Int8 & value) override { return readEnum(value); }
    bool readEnum16(Int16 & value) override { return readEnum(value); }

    void prepareEnumMapping8(const std::vector<std::pair<String, Int8>> & name_value_pairs) override
    {
        prepareSetOfEnumValues(name_value_pairs);
    }
    void prepareEnumMapping16(const std::vector<std::pair<String, Int16>> & name_value_pairs) override
    {
        prepareSetOfEnumValues(name_value_pairs);
    }

    bool readDate(DayNum & date) override
    {
        UInt16 number;
        if (!readNumeric(number))
            return false;
        date = DayNum(number);
        return true;
    }

    bool readDateTime(time_t & tm) override
    {
        UInt32 number;
        if (!readNumeric(number))
            return false;
        tm = number;
        return true;
    }

    bool readDecimal32(Decimal32 & decimal, UInt32, UInt32 scale) override { return readDecimal(decimal, scale); }
    bool readDecimal64(Decimal64 & decimal, UInt32, UInt32 scale) override { return readDecimal(decimal, scale); }
    bool readDecimal128(Decimal128 & decimal, UInt32, UInt32 scale) override { return readDecimal(decimal, scale); }

private:
    template <typename To>
    bool readNumeric(To & value)
    {
        FromType number;
        if (!readField(number))
            return false;
        value = numericCast<To>(number);
        return true;
    }

    template<typename EnumType>
    bool readEnum(EnumType & value)
    {
        if constexpr (!std::is_integral_v<FromType>)
            cannotConvertType("Enum"); // It's not correct to convert floating point to enum.
        FromType number;
        if (!readField(number))
            return false;
        value = numericCast<EnumType>(number);
        if (set_of_enum_values->find(value) == set_of_enum_values->end())
            cannotConvertValue(toString(value), "Enum");
        return true;
    }

    template<typename EnumType>
    void prepareSetOfEnumValues(const std::vector<std::pair<String, EnumType>> & name_value_pairs)
    {
        if (likely(set_of_enum_values.has_value()))
            return;
        set_of_enum_values.emplace();
        for (const auto & name_value_pair : name_value_pairs)
            set_of_enum_values->emplace(name_value_pair.second);
    }

    template <typename S>
    bool readDecimal(Decimal<S> & decimal, UInt32 scale)
    {
        FromType number;
        if (!readField(number))
            return false;
        decimal.value = convertToDecimal<DataTypeNumber<FromType>, DataTypeDecimal<Decimal<S>>>(number, scale);
        return true;
    }

    bool readField(FromType & value)
    {
        if constexpr (((field_type_id == google::protobuf::FieldDescriptor::TYPE_INT32) && std::is_same_v<FromType, Int64>)
                   || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_INT64) && std::is_same_v<FromType, Int64>))
        {
            return simple_reader.readInt(value);
        }
        else if constexpr (((field_type_id == google::protobuf::FieldDescriptor::TYPE_UINT32) && std::is_same_v<FromType, UInt64>)
                        || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_UINT64) && std::is_same_v<FromType, UInt64>))
        {
            return simple_reader.readUInt(value);
        }

        else if constexpr (((field_type_id == google::protobuf::FieldDescriptor::TYPE_SINT32) && std::is_same_v<FromType, Int64>)
                        || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_SINT64) && std::is_same_v<FromType, Int64>))
        {
            return simple_reader.readSInt(value);
        }
        else
        {
            static_assert(((field_type_id == google::protobuf::FieldDescriptor::TYPE_FIXED32) && std::is_same_v<FromType, UInt32>)
                       || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_SFIXED32) && std::is_same_v<FromType, Int32>)
                       || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_FIXED64) && std::is_same_v<FromType, UInt64>)
                       || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_SFIXED64) && std::is_same_v<FromType, Int64>)
                       || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_FLOAT) && std::is_same_v<FromType, float>)
                       || ((field_type_id == google::protobuf::FieldDescriptor::TYPE_DOUBLE) && std::is_same_v<FromType, double>));
            return simple_reader.readFixed(value);
        }
    }

    std::optional<std::unordered_set<Int16>> set_of_enum_values;
};

#define PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(field_type_id, field_type) \
    template <> \
    std::unique_ptr<ProtobufReader::IConverter> ProtobufReader::createConverter<field_type_id>( \
        const google::protobuf::FieldDescriptor * field) \
    { \
        return std::make_unique<ConverterFromNumber<field_type_id, field_type>>(simple_reader, field); \
    }
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_INT32, Int64);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SINT32, Int64);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_UINT32, UInt64);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_INT64, Int64);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SINT64, Int64);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_UINT64, UInt64);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_FIXED32, UInt32);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SFIXED32, Int32);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_FIXED64, UInt64);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_SFIXED64, Int64);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_FLOAT, float);
PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS(google::protobuf::FieldDescriptor::TYPE_DOUBLE, double);
#undef PROTOBUF_READER_CREATE_CONVERTER_SPECIALIZATION_FOR_NUMBERS



class ProtobufReader::ConverterFromBool : public ConverterBaseImpl
{
public:
    using ConverterBaseImpl::ConverterBaseImpl;

    bool readStringInto(PaddedPODArray<UInt8> & str) override
    {
        bool b;
        if (!readField(b))
            return false;
        StringRef ref(b ? "true" : "false");
        str.insert(ref.data, ref.data + ref.size);
        return true;
    }

    bool readInt8(Int8 & value) override { return readNumeric(value); }
    bool readUInt8(UInt8 & value) override { return readNumeric(value); }
    bool readInt16(Int16 & value) override { return readNumeric(value); }
    bool readUInt16(UInt16 & value) override { return readNumeric(value); }
    bool readInt32(Int32 & value) override { return readNumeric(value); }
    bool readUInt32(UInt32 & value) override { return readNumeric(value); }
    bool readInt64(Int64 & value) override { return readNumeric(value); }
    bool readUInt64(UInt64 & value) override { return readNumeric(value); }
    bool readFloat32(Float32 & value) override { return readNumeric(value); }
    bool readFloat64(Float64 & value) override { return readNumeric(value); }
    bool readDecimal32(Decimal32 & decimal, UInt32, UInt32) override { return readNumeric(decimal.value); }
    bool readDecimal64(Decimal64 & decimal, UInt32, UInt32) override { return readNumeric(decimal.value); }
    bool readDecimal128(Decimal128 & decimal, UInt32, UInt32) override { return readNumeric(decimal.value); }

private:
    template<typename T>
    bool readNumeric(T & value)
    {
        bool b;
        if (!readField(b))
            return false;
        value = b ? 1 : 0;
        return true;
    }

    bool readField(bool & b)
    {
        UInt64 number;
        if (!simple_reader.readUInt(number))
            return false;
        b = static_cast<bool>(number);
        return true;
    }
};

template <>
std::unique_ptr<ProtobufReader::IConverter> ProtobufReader::createConverter<google::protobuf::FieldDescriptor::TYPE_BOOL>(
    const google::protobuf::FieldDescriptor * field)
{
    return std::make_unique<ConverterFromBool>(simple_reader, field);
}


class ProtobufReader::ConverterFromEnum : public ConverterBaseImpl
{
public:
    using ConverterBaseImpl::ConverterBaseImpl;

    bool readStringInto(PaddedPODArray<UInt8> & str) override
    {
        prepareEnumPbNumberToNameMap();
        Int64 pbnumber;
        if (!readField(pbnumber))
            return false;
        auto it = enum_pbnumber_to_name_map->find(pbnumber);
        if (it == enum_pbnumber_to_name_map->end())
            cannotConvertValue(toString(pbnumber), "Enum");
        const auto & ref = it->second;
        str.insert(ref.data, ref.data + ref.size);
        return true;
    }

    bool readInt8(Int8 & value) override { return readNumeric(value); }
    bool readUInt8(UInt8 & value) override { return readNumeric(value); }
    bool readInt16(Int16 & value) override { return readNumeric(value); }
    bool readUInt16(UInt16 & value) override { return readNumeric(value); }
    bool readInt32(Int32 & value) override { return readNumeric(value); }
    bool readUInt32(UInt32 & value) override { return readNumeric(value); }
    bool readInt64(Int64 & value) override { return readNumeric(value); }
    bool readUInt64(UInt64 & value) override { return readNumeric(value); }

    void prepareEnumMapping8(const std::vector<std::pair<String, Int8>> & name_value_pairs) override
    {
        prepareEnumPbNumberToValueMap(name_value_pairs);
    }
    void prepareEnumMapping16(const std::vector<std::pair<String, Int16>> & name_value_pairs) override
    {
        prepareEnumPbNumberToValueMap(name_value_pairs);
    }

    bool readEnum8(Int8 & value) override { return readEnum(value); }
    bool readEnum16(Int16 & value) override { return readEnum(value); }

private:
    template <typename T>
    bool readNumeric(T & value)
    {
        Int64 pbnumber;
        if (!readField(pbnumber))
            return false;
        value = numericCast<T>(pbnumber);
        return true;
    }

    template<typename T>
    bool readEnum(T & value)
    {
        Int64 pbnumber;
        if (!readField(pbnumber))
            return false;
        if (enum_pbnumber_always_equals_value)
            value = static_cast<T>(pbnumber);
        else
        {
            auto it = enum_pbnumber_to_value_map->find(pbnumber);
            if (it == enum_pbnumber_to_value_map->end())
                cannotConvertValue(toString(pbnumber), "Enum");
            value = static_cast<T>(it->second);
        }
        return true;
    }

    void prepareEnumPbNumberToNameMap()
    {
        if (likely(enum_pbnumber_to_name_map.has_value()))
            return;
        enum_pbnumber_to_name_map.emplace();
        const auto * enum_type = field->enum_type();
        for (int i = 0; i != enum_type->value_count(); ++i)
        {
            const auto * enum_value = enum_type->value(i);
            enum_pbnumber_to_name_map->emplace(enum_value->number(), enum_value->name());
        }
    }

    template <typename T>
    void prepareEnumPbNumberToValueMap(const std::vector<std::pair<String, T>> & name_value_pairs)
    {
        if (likely(enum_pbnumber_to_value_map.has_value()))
            return;
        enum_pbnumber_to_value_map.emplace();
        enum_pbnumber_always_equals_value = true;
        for (const auto & name_value_pair : name_value_pairs)
        {
            Int16 value = name_value_pair.second;
            const auto * enum_descriptor = field->enum_type()->FindValueByName(name_value_pair.first);
            if (enum_descriptor)
            {
                enum_pbnumber_to_value_map->emplace(enum_descriptor->number(), value);
                if (enum_descriptor->number() != value)
                    enum_pbnumber_always_equals_value = false;
            }
            else
                enum_pbnumber_always_equals_value = false;
        }
    }

    bool readField(Int64 & enum_pbnumber)
    {
        return simple_reader.readInt(enum_pbnumber);
    }

    std::optional<std::unordered_map<Int64, StringRef>> enum_pbnumber_to_name_map;
    std::optional<std::unordered_map<Int64, Int16>> enum_pbnumber_to_value_map;
    bool enum_pbnumber_always_equals_value;
};

template <>
std::unique_ptr<ProtobufReader::IConverter> ProtobufReader::createConverter<google::protobuf::FieldDescriptor::TYPE_ENUM>(
    const google::protobuf::FieldDescriptor * field)
{
    return std::make_unique<ConverterFromEnum>(simple_reader, field);
}


ProtobufReader::ProtobufReader(
    ReadBuffer & in_, const google::protobuf::Descriptor * message_type, const std::vector<String> & column_names)
    : simple_reader(in_)
{
    root_message = ProtobufColumnMatcher::matchColumns<ColumnMatcherTraits>(column_names, message_type);
    setTraitsDataAfterMatchingColumns(root_message.get());
}

ProtobufReader::~ProtobufReader() = default;

void ProtobufReader::setTraitsDataAfterMatchingColumns(Message * message)
{
    for (Field & field : message->fields)
    {
        if (field.nested_message)
        {
            setTraitsDataAfterMatchingColumns(field.nested_message.get());
            continue;
        }
        switch (field.field_descriptor->type())
        {
#define PROTOBUF_READER_CONVERTER_CREATING_CASE(field_type_id) \
            case field_type_id: \
                field.data.converter = createConverter<field_type_id>(field.field_descriptor); \
                break
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_STRING);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_BYTES);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_INT32);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_SINT32);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_UINT32);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_FIXED32);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_SFIXED32);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_INT64);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_SINT64);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_UINT64);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_FIXED64);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_SFIXED64);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_FLOAT);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_DOUBLE);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_BOOL);
            PROTOBUF_READER_CONVERTER_CREATING_CASE(google::protobuf::FieldDescriptor::TYPE_ENUM);
#undef PROTOBUF_READER_CONVERTER_CREATING_CASE
            default: __builtin_unreachable();
        }
        message->data.field_number_to_field_map.emplace(field.field_number, &field);
    }
}

bool ProtobufReader::startMessage()
{
    if (!simple_reader.startMessage())
        return false;
    current_message = root_message.get();
    current_field_index = 0;
    return true;
}

void ProtobufReader::endMessage(bool try_ignore_errors)
{
    simple_reader.endMessage(try_ignore_errors);
    current_message = nullptr;
    current_converter = nullptr;
}

bool ProtobufReader::readColumnIndex(size_t & column_index)
{
    while (true)
    {
        UInt32 field_number;
        if (!simple_reader.readFieldNumber(field_number))
        {
            if (!current_message->parent)
            {
                current_converter = nullptr;
                return false;
            }
            simple_reader.endNestedMessage();
            current_field_index = current_message->index_in_parent;
            current_message = current_message->parent;
            continue;
        }

        const Field * field = nullptr;
        for (; current_field_index < current_message->fields.size(); ++current_field_index)
        {
            const Field & f = current_message->fields[current_field_index];
            if (f.field_number == field_number)
            {
                field = &f;
                break;
            }
            if (f.field_number > field_number)
                break;
        }

        if (!field)
        {
            const auto & field_number_to_field_map = current_message->data.field_number_to_field_map;
            auto it = field_number_to_field_map.find(field_number);
            if (it == field_number_to_field_map.end())
                continue;
            field = it->second;
        }

        if (field->nested_message)
        {
            simple_reader.startNestedMessage();
            current_message = field->nested_message.get();
            current_field_index = 0;
            continue;
        }

        column_index = field->column_index;
        current_converter = field->data.converter.get();
        return true;
    }
}

}
#endif
