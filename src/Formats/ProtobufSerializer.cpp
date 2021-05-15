#include <Formats/ProtobufSerializer.h>

#if USE_PROTOBUF
#   include <Columns/ColumnAggregateFunction.h>
#   include <Columns/ColumnArray.h>
#   include <Columns/ColumnDecimal.h>
#   include <Columns/ColumnLowCardinality.h>
#   include <Columns/ColumnMap.h>
#   include <Columns/ColumnNullable.h>
#   include <Columns/ColumnFixedString.h>
#   include <Columns/ColumnString.h>
#   include <Columns/ColumnTuple.h>
#   include <Columns/ColumnVector.h>
#   include <Common/PODArray.h>
#   include <Common/quoteString.h>
#   include <Core/DecimalComparison.h>
#   include <DataTypes/DataTypeAggregateFunction.h>
#   include <DataTypes/DataTypeArray.h>
#   include <DataTypes/DataTypesDecimal.h>
#   include <DataTypes/DataTypeDateTime64.h>
#   include <DataTypes/DataTypeEnum.h>
#   include <DataTypes/DataTypeFixedString.h>
#   include <DataTypes/DataTypeLowCardinality.h>
#   include <DataTypes/DataTypeMap.h>
#   include <DataTypes/DataTypeNullable.h>
#   include <DataTypes/DataTypeTuple.h>
#   include <DataTypes/Serializations/SerializationDecimal.h>
#   include <DataTypes/Serializations/SerializationFixedString.h>
#   include <Formats/ProtobufReader.h>
#   include <Formats/ProtobufWriter.h>
#   include <IO/ReadBufferFromString.h>
#   include <IO/ReadHelpers.h>
#   include <IO/WriteBufferFromString.h>
#   include <IO/WriteHelpers.h>
#   include <ext/range.h>
#   include <google/protobuf/descriptor.h>
#   include <google/protobuf/descriptor.pb.h>
#   include <boost/algorithm/string.hpp>
#   include <boost/container/flat_map.hpp>
#   include <boost/container/flat_set.hpp>
#   include <boost/numeric/conversion/cast.hpp>
#   include <boost/range/algorithm.hpp>

#   include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_COLUMNS_SERIALIZED_TO_PROTOBUF_FIELDS;
    extern const int MULTIPLE_COLUMNS_SERIALIZED_TO_SAME_PROTOBUF_FIELD;
    extern const int NO_COLUMN_SERIALIZED_TO_REQUIRED_PROTOBUF_FIELD;
    extern const int DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD;
    extern const int PROTOBUF_FIELD_NOT_REPEATED;
    extern const int PROTOBUF_BAD_CAST;
    extern const int LOGICAL_ERROR;
}

namespace
{
    using FieldDescriptor = google::protobuf::FieldDescriptor;
    using MessageDescriptor = google::protobuf::Descriptor;
    using FieldTypeId = google::protobuf::FieldDescriptor::Type;


    /// Compares column's name with protobuf field's name.
    /// This comparison is case-insensitive and ignores the difference between '.' and '_'
    struct ColumnNameWithProtobufFieldNameComparator
    {
        static bool equals(char c1, char c2)
        {
            return convertChar(c1) == convertChar(c2);
        }

        static bool equals(const std::string_view & s1, const std::string_view & s2)
        {
            return (s1.length() == s2.length())
                && std::equal(s1.begin(), s1.end(), s2.begin(), [](char c1, char c2) { return convertChar(c1) == convertChar(c2); });
        }

        static bool less(const std::string_view & s1, const std::string_view & s2)
        {
            return std::lexicographical_compare(s1.begin(), s1.end(), s2.begin(), s2.end(), [](char c1, char c2) { return convertChar(c1) < convertChar(c2); });
        }

        static bool startsWith(const std::string_view & s1, const std::string_view & s2)
        {
            return (s1.length() >= s2.length()) && equals(s1.substr(0, s2.length()), s2);
        }

        static char convertChar(char c)
        {
            c = tolower(c);
            if (c == '.')
                c = '_';
            return c;
        }
    };


    // Should we omit null values (zero for numbers / empty string for strings) while storing them.
    bool shouldSkipZeroOrEmpty(const FieldDescriptor & field_descriptor)
    {
        if (!field_descriptor.is_optional())
            return false;
        if (field_descriptor.containing_type()->options().map_entry())
            return false;
        return field_descriptor.message_type() || (field_descriptor.file()->syntax() == google::protobuf::FileDescriptor::SYNTAX_PROTO3);
    }

    // Should we pack repeated values while storing them.
    bool shouldPackRepeated(const FieldDescriptor & field_descriptor)
    {
        if (!field_descriptor.is_repeated())
            return false;
        switch (field_descriptor.type())
        {
            case FieldTypeId::TYPE_INT32:
            case FieldTypeId::TYPE_UINT32:
            case FieldTypeId::TYPE_SINT32:
            case FieldTypeId::TYPE_INT64:
            case FieldTypeId::TYPE_UINT64:
            case FieldTypeId::TYPE_SINT64:
            case FieldTypeId::TYPE_FIXED32:
            case FieldTypeId::TYPE_SFIXED32:
            case FieldTypeId::TYPE_FIXED64:
            case FieldTypeId::TYPE_SFIXED64:
            case FieldTypeId::TYPE_FLOAT:
            case FieldTypeId::TYPE_DOUBLE:
            case FieldTypeId::TYPE_BOOL:
            case FieldTypeId::TYPE_ENUM:
                break;
            default:
                return false;
        }
        if (field_descriptor.options().has_packed())
            return field_descriptor.options().packed();
        return field_descriptor.file()->syntax() == google::protobuf::FileDescriptor::SYNTAX_PROTO3;
    }


    struct ProtobufReaderOrWriter
    {
        ProtobufReaderOrWriter(ProtobufReader & reader_) : reader(&reader_) {} // NOLINT(google-explicit-constructor)
        ProtobufReaderOrWriter(ProtobufWriter & writer_) : writer(&writer_) {} // NOLINT(google-explicit-constructor)
        ProtobufReader * const reader = nullptr;
        ProtobufWriter * const writer = nullptr;
    };


    /// Base class for all serializers which serialize a single value.
    class ProtobufSerializerSingleValue : public ProtobufSerializer
    {
    protected:
        ProtobufSerializerSingleValue(const FieldDescriptor & field_descriptor_, const ProtobufReaderOrWriter & reader_or_writer_)
            : field_descriptor(field_descriptor_)
            , field_typeid(field_descriptor_.type())
            , field_tag(field_descriptor.number())
            , reader(reader_or_writer_.reader)
            , writer(reader_or_writer_.writer)
            , skip_zero_or_empty(shouldSkipZeroOrEmpty(field_descriptor))
        {
        }

        void setColumns(const ColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            column = columns[0];
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            column = columns[0]->getPtr();
        }

        template <typename NumberType>
        void writeInt(NumberType value)
        {
            auto casted = castNumber<Int64>(value);
            if (casted || !skip_zero_or_empty)
                writer->writeInt(field_tag, casted);
        }

        template <typename NumberType>
        void writeSInt(NumberType value)
        {
            auto casted = castNumber<Int64>(value);
            if (casted || !skip_zero_or_empty)
                writer->writeSInt(field_tag, casted);
        }

        template <typename NumberType>
        void writeUInt(NumberType value)
        {
            auto casted = castNumber<UInt64>(value);
            if (casted || !skip_zero_or_empty)
                writer->writeUInt(field_tag, casted);
        }

        template <typename FieldType, typename NumberType>
        void writeFixed(NumberType value)
        {
            auto casted = castNumber<FieldType>(value);
            if (casted || !skip_zero_or_empty)
                writer->writeFixed(field_tag, casted);
        }

        Int64 readInt() { return reader->readInt(); }
        Int64 readSInt() { return reader->readSInt(); }
        UInt64 readUInt() { return reader->readUInt(); }

        template <typename FieldType>
        FieldType readFixed()
        {
            return reader->readFixed<FieldType>();
        }

        void writeStr(const std::string_view & str)
        {
            if (!str.empty() || !skip_zero_or_empty)
                writer->writeString(field_tag, str);
        }

        void readStr(String & str) { reader->readString(str); }
        void readStrAndAppend(PaddedPODArray<UInt8> & str) { reader->readStringAndAppend(str); }

        template <typename DestType>
        DestType parseFromStr(const std::string_view & str) const
        {
            try
            {
                DestType result;
                ReadBufferFromMemory buf(str.data(), str.length());
                readText(result, buf);
                return result;
            }
            catch (...)
            {
                cannotConvertValue(str, "String", TypeName<DestType>);
            }
        }

        template <typename DestType, typename SrcType>
        DestType castNumber(SrcType value) const
        {
            if constexpr (std::is_same_v<DestType, SrcType>)
                return value;
            DestType result;
            try
            {
                /// TODO: use accurate::convertNumeric() maybe?
                result = boost::numeric_cast<DestType>(value);
            }
            catch (boost::numeric::bad_numeric_cast &)
            {
                cannotConvertValue(toString(value), TypeName<SrcType>, TypeName<DestType>);
            }
            return result;
        }

        [[noreturn]] void cannotConvertValue(const std::string_view & src_value, const std::string_view & src_type_name, const std::string_view & dest_type_name) const
        {
            throw Exception(
                "Could not convert value '" + String{src_value} + "' from type " + String{src_type_name} + " to type " + String{dest_type_name} +
                    " while " + (reader ? "reading" : "writing") + " field " + field_descriptor.name(),
                ErrorCodes::PROTOBUF_BAD_CAST);
        }

        const FieldDescriptor & field_descriptor;
        const FieldTypeId field_typeid;
        const int field_tag;
        ProtobufReader * const reader;
        ProtobufWriter * const writer;
        ColumnPtr column;

    private:
        const bool skip_zero_or_empty;
    };


    /// Serializes any ColumnVector<NumberType> to a field of any type except TYPE_MESSAGE, TYPE_GROUP.
    /// NumberType must be one of the following types: Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64,
    /// Int128, UInt128, Int256, UInt256, Float32, Float64.
    /// And the field's type cannot be TYPE_ENUM if NumberType is Float32 or Float64.
    template <typename NumberType>
    class ProtobufSerializerNumber : public ProtobufSerializerSingleValue
    {
    public:
        using ColumnType = ColumnVector<NumberType>;

        ProtobufSerializerNumber(const FieldDescriptor & field_descriptor_, const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(field_descriptor_, reader_or_writer_)
        {
            setFunctions();
        }

        void writeRow(size_t row_num) override
        {
            const auto & column_vector = assert_cast<const ColumnType &>(*column);
            write_function(column_vector.getElement(row_num));
        }

        void readRow(size_t row_num) override
        {
            NumberType value = read_function();
            auto & column_vector = assert_cast<ColumnType &>(column->assumeMutableRef());
            if (row_num < column_vector.size())
                column_vector.getElement(row_num) = value;
            else
                column_vector.insertValue(value);
        }

        void insertDefaults(size_t row_num) override
        {
            auto & column_vector = assert_cast<ColumnType &>(column->assumeMutableRef());
            if (row_num < column_vector.size())
                return;
            column_vector.insertValue(getDefaultNumber());
        }

    private:
        void setFunctions()
        {
            switch (field_typeid)
            {
                case FieldTypeId::TYPE_INT32:
                {
                    write_function = [this](NumberType value) { writeInt(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readInt()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_int32()); };
                    break;
                }

                case FieldTypeId::TYPE_SINT32:
                {
                    write_function = [this](NumberType value) { writeSInt(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readSInt()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_int32()); };
                    break;
                }

                case FieldTypeId::TYPE_UINT32:
                {
                    write_function = [this](NumberType value) { writeUInt(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readUInt()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_uint32()); };
                    break;
                }

                case FieldTypeId::TYPE_INT64:
                {
                    write_function = [this](NumberType value) { writeInt(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readInt()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_int64()); };
                    break;
                }

                case FieldTypeId::TYPE_SINT64:
                {
                    write_function = [this](NumberType value) { writeSInt(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readSInt()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_int64()); };
                    break;
                }

                case FieldTypeId::TYPE_UINT64:
                {
                    write_function = [this](NumberType value) { writeUInt(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readUInt()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_uint64()); };
                    break;
                }

                case FieldTypeId::TYPE_FIXED32:
                {
                    write_function = [this](NumberType value) { writeFixed<UInt32>(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readFixed<UInt32>()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_uint32()); };
                    break;
                }

                case FieldTypeId::TYPE_SFIXED32:
                {
                    write_function = [this](NumberType value) { writeFixed<Int32>(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readFixed<Int32>()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_int32()); };
                    break;
                }

                case FieldTypeId::TYPE_FIXED64:
                {
                    write_function = [this](NumberType value) { writeFixed<UInt64>(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readFixed<UInt64>()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_uint64()); };
                    break;
                }

                case FieldTypeId::TYPE_SFIXED64:
                {
                    write_function = [this](NumberType value) { writeFixed<Int64>(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readFixed<Int64>()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_int64()); };
                    break;
                }

                case FieldTypeId::TYPE_FLOAT:
                {
                    write_function = [this](NumberType value) { writeFixed<Float32>(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readFixed<Float32>()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_float()); };
                    break;
                }

                case FieldTypeId::TYPE_DOUBLE:
                {
                    write_function = [this](NumberType value) { writeFixed<Float64>(value); };
                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readFixed<Float64>()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_double()); };
                    break;
                }

                case FieldTypeId::TYPE_BOOL:
                {
                    write_function = [this](NumberType value)
                    {
                        if (value == 0)
                            writeUInt(0);
                        else if (value == 1)
                            writeUInt(1);
                        else
                            cannotConvertValue(toString(value), TypeName<NumberType>, field_descriptor.type_name());
                    };

                    read_function = [this]() -> NumberType
                    {
                        UInt64 u64 = readUInt();
                        if (u64 < 2)
                            return static_cast<NumberType>(u64);
                        else
                            cannotConvertValue(toString(u64), field_descriptor.type_name(), TypeName<NumberType>);
                    };

                    default_function = [this]() -> NumberType { return static_cast<NumberType>(field_descriptor.default_value_bool()); };
                    break;
                }

                case FieldTypeId::TYPE_STRING:
                case FieldTypeId::TYPE_BYTES:
                {
                    write_function = [this](NumberType value)
                    {
                        WriteBufferFromString buf{text_buffer};
                        writeText(value, buf);
                        buf.finalize();
                        writeStr(text_buffer);
                    };

                    read_function = [this]() -> NumberType
                    {
                        readStr(text_buffer);
                        return parseFromStr<NumberType>(text_buffer);
                    };

                    default_function = [this]() -> NumberType { return parseFromStr<NumberType>(field_descriptor.default_value_string()); };
                    break;
                }

                case FieldTypeId::TYPE_ENUM:
                {
                    if (std::is_floating_point_v<NumberType>)
                        failedToSetFunctions();

                    write_function = [this](NumberType value)
                    {
                        int number = castNumber<int>(value);
                        checkProtobufEnumValue(number);
                        writeInt(number);
                    };

                    read_function = [this]() -> NumberType { return castNumber<NumberType>(readInt()); };
                    default_function = [this]() -> NumberType { return castNumber<NumberType>(field_descriptor.default_value_enum()->number()); };
                    break;
                }

                default:
                    failedToSetFunctions();
            }
        }

        [[noreturn]] void failedToSetFunctions() const
        {
            throw Exception(
                "The field " + quoteString(field_descriptor.full_name()) + " has an incompatible type " + field_descriptor.type_name()
                    + " for serialization of the data type " + quoteString(TypeName<NumberType>),
                ErrorCodes::DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD);
        }

        NumberType getDefaultNumber()
        {
            if (!default_number)
                default_number = default_function();
            return *default_number;
        }

        void checkProtobufEnumValue(int value) const
        {
            const auto * enum_value_descriptor = field_descriptor.enum_type()->FindValueByNumber(value);
            if (!enum_value_descriptor)
                cannotConvertValue(toString(value), TypeName<NumberType>, field_descriptor.type_name());
        }

    protected:
        std::function<void(NumberType)> write_function;
        std::function<NumberType()> read_function;
        std::function<NumberType()> default_function;
        String text_buffer;

    private:
        std::optional<NumberType> default_number;
    };


    /// Serializes ColumnString or ColumnFixedString to a field of any type except TYPE_MESSAGE, TYPE_GROUP.
    template <bool is_fixed_string>
    class ProtobufSerializerString : public ProtobufSerializerSingleValue
    {
    public:
        using ColumnType = std::conditional_t<is_fixed_string, ColumnFixedString, ColumnString>;

        ProtobufSerializerString(
            const std::shared_ptr<const DataTypeFixedString> & fixed_string_data_type_,
            const google::protobuf::FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(field_descriptor_, reader_or_writer_)
            , fixed_string_data_type(fixed_string_data_type_)
            , n(fixed_string_data_type->getN())
        {
            static_assert(is_fixed_string, "This constructor for FixedString only");
            setFunctions();
            prepareEnumMapping();
        }

        ProtobufSerializerString(
            const google::protobuf::FieldDescriptor & field_descriptor_, const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(field_descriptor_, reader_or_writer_)
        {
            static_assert(!is_fixed_string, "This constructor for String only");
            setFunctions();
            prepareEnumMapping();
        }

        void writeRow(size_t row_num) override
        {
            const auto & column_string = assert_cast<const ColumnType &>(*column);
            write_function(std::string_view{column_string.getDataAt(row_num)});
        }

        void readRow(size_t row_num) override
        {
            auto & column_string = assert_cast<ColumnType &>(column->assumeMutableRef());
            const size_t old_size = column_string.size();
            typename ColumnType::Chars & data = column_string.getChars();
            const size_t old_data_size = data.size();

            if (row_num < old_size)
            {
                text_buffer.clear();
                read_function(text_buffer);
            }
            else
            {
                try
                {
                    read_function(data);
                }
                catch (...)
                {
                    data.resize_assume_reserved(old_data_size);
                    throw;
                }
            }

            if constexpr (is_fixed_string)
            {
                if (row_num < old_size)
                {
                    SerializationFixedString::alignStringLength(n, text_buffer, 0);
                    memcpy(data.data() + row_num * n, text_buffer.data(), n);
                }
                else
                    SerializationFixedString::alignStringLength(n, data, old_data_size);
            }
            else
            {
                if (row_num < old_size)
                {
                    if (row_num != old_size - 1)
                        throw Exception("Cannot replace a string in the middle of ColumnString", ErrorCodes::LOGICAL_ERROR);
                    column_string.popBack(1);
                }
                try
                {
                    data.push_back(0 /* terminating zero */);
                    column_string.getOffsets().push_back(data.size());
                }
                catch (...)
                {
                    data.resize_assume_reserved(old_data_size);
                    column_string.getOffsets().resize_assume_reserved(old_size);
                    throw;
                }
            }
        }

        void insertDefaults(size_t row_num) override
        {
            auto & column_string = assert_cast<ColumnType &>(column->assumeMutableRef());
            const size_t old_size = column_string.size();
            if (row_num < old_size)
                return;

            const auto & default_str = getDefaultString();
            typename ColumnType::Chars & data = column_string.getChars();
            const size_t old_data_size = data.size();
            try
            {
                data.insert(default_str.data(), default_str.data() + default_str.size());
            }
            catch (...)
            {
                data.resize_assume_reserved(old_data_size);
                throw;
            }

            if constexpr (!is_fixed_string)
            {
                try
                {
                    data.push_back(0 /* terminating zero */);
                    column_string.getOffsets().push_back(data.size());
                }
                catch (...)
                {
                    data.resize_assume_reserved(old_data_size);
                    column_string.getOffsets().resize_assume_reserved(old_size);
                    throw;
                }
            }
        }

    private:
        void setFunctions()
        {
            switch (field_typeid)
            {
                case FieldTypeId::TYPE_INT32:
                {
                    write_function = [this](const std::string_view & str) { writeInt(parseFromStr<Int32>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readInt(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_int32()); };
                    break;
                }

                case FieldTypeId::TYPE_SINT32:
                {
                    write_function = [this](const std::string_view & str) { writeSInt(parseFromStr<Int32>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readSInt(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_int32()); };
                    break;
                }

                case FieldTypeId::TYPE_UINT32:
                {
                    write_function = [this](const std::string_view & str) { writeUInt(parseFromStr<UInt32>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readUInt(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_uint32()); };
                    break;
                }

                case FieldTypeId::TYPE_INT64:
                {
                    write_function = [this](const std::string_view & str) { writeInt(parseFromStr<Int64>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readInt(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_int64()); };
                    break;
                }

                case FieldTypeId::TYPE_SINT64:
                {
                    write_function = [this](const std::string_view & str) { writeSInt(parseFromStr<Int64>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readSInt(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_int64()); };
                    break;
                }

                case FieldTypeId::TYPE_UINT64:
                {
                    write_function = [this](const std::string_view & str) { writeUInt(parseFromStr<UInt64>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readUInt(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_uint64()); };
                    break;
                }

                case FieldTypeId::TYPE_FIXED32:
                {
                    write_function = [this](const std::string_view & str) { writeFixed<UInt32>(parseFromStr<UInt32>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readFixed<UInt32>(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_uint32()); };
                    break;
                }

                case FieldTypeId::TYPE_SFIXED32:
                {
                    write_function = [this](const std::string_view & str) { writeFixed<Int32>(parseFromStr<Int32>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readFixed<Int32>(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_int32()); };
                    break;
                }

                case FieldTypeId::TYPE_FIXED64:
                {
                    write_function = [this](const std::string_view & str) { writeFixed<UInt64>(parseFromStr<UInt64>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readFixed<UInt64>(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_uint64()); };
                    break;
                }

                case FieldTypeId::TYPE_SFIXED64:
                {
                    write_function = [this](const std::string_view & str) { writeFixed<Int64>(parseFromStr<Int64>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readFixed<Int64>(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_int64()); };
                    break;
                }

                case FieldTypeId::TYPE_FLOAT:
                {
                    write_function = [this](const std::string_view & str) { writeFixed<Float32>(parseFromStr<Float32>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readFixed<Float32>(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_float()); };
                    break;
                }

                case FieldTypeId::TYPE_DOUBLE:
                {
                    write_function = [this](const std::string_view & str) { writeFixed<Float64>(parseFromStr<Float64>(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { toStringAppend(readFixed<Float64>(), str); };
                    default_function = [this]() -> String { return toString(field_descriptor.default_value_double()); };
                    break;
                }

                case FieldTypeId::TYPE_BOOL:
                {
                    write_function = [this](const std::string_view & str)
                    {
                        if (str == "true")
                            writeUInt(1);
                        else if (str == "false")
                            writeUInt(0);
                        else
                            cannotConvertValue(str, "String", field_descriptor.type_name());
                    };

                    read_function = [this](PaddedPODArray<UInt8> & str)
                    {
                        UInt64 u64 = readUInt();
                        if (u64 < 2)
                        {
                            std::string_view ref(u64 ? "true" : "false");
                            str.insert(ref.data(), ref.data() + ref.length());
                        }
                        else
                            cannotConvertValue(toString(u64), field_descriptor.type_name(), "String");
                    };

                    default_function = [this]() -> String
                    {
                        return field_descriptor.default_value_bool() ? "true" : "false";
                    };
                    break;
                }

                case FieldTypeId::TYPE_STRING:
                case FieldTypeId::TYPE_BYTES:
                {
                    write_function = [this](const std::string_view & str) { writeStr(str); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { readStrAndAppend(str); };
                    default_function = [this]() -> String { return field_descriptor.default_value_string(); };
                    break;
                }

                case FieldTypeId::TYPE_ENUM:
                {
                    write_function = [this](const std::string_view & str) { writeInt(stringToProtobufEnumValue(str)); };
                    read_function = [this](PaddedPODArray<UInt8> & str) { protobufEnumValueToStringAppend(readInt(), str); };
                    default_function = [this]() -> String { return field_descriptor.default_value_enum()->name(); };
                    break;
                }

                default:
                    failedToSetFunctions();
            }
        }

        [[noreturn]] void failedToSetFunctions()
        {
            throw Exception(
                "The field " + quoteString(field_descriptor.full_name()) + " has an incompatible type " + field_descriptor.type_name()
                    + " for serialization of the data type " + quoteString(is_fixed_string ? "FixedString" : "String"),
                ErrorCodes::DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD);
        }

        const PaddedPODArray<UInt8> & getDefaultString()
        {
            if (!default_string)
            {
                PaddedPODArray<UInt8> arr;
                auto str = default_function();
                arr.insert(str.data(), str.data() + str.size());
                if constexpr (is_fixed_string)
                    SerializationFixedString::alignStringLength(n, arr, 0);
                default_string = std::move(arr);
            }
            return *default_string;
        }

        template <typename NumberType>
        void toStringAppend(NumberType value, PaddedPODArray<UInt8> & str)
        {
            WriteBufferFromVector buf{str, WriteBufferFromVector<PaddedPODArray<UInt8>>::AppendModeTag{}};
            writeText(value, buf);
        }

        void prepareEnumMapping()
        {
            if ((field_typeid == google::protobuf::FieldDescriptor::TYPE_ENUM) && writer)
            {
                const auto & enum_descriptor = *field_descriptor.enum_type();
                for (int i = 0; i != enum_descriptor.value_count(); ++i)
                {
                    const auto & enum_value_descriptor = *enum_descriptor.value(i);
                    string_to_protobuf_enum_value_map.emplace(enum_value_descriptor.name(), enum_value_descriptor.number());
                }
            }
        }

        int stringToProtobufEnumValue(const std::string_view & str) const
        {
            auto it = string_to_protobuf_enum_value_map.find(str);
            if (it == string_to_protobuf_enum_value_map.end())
                cannotConvertValue(str, "String", field_descriptor.type_name());
            return it->second;
        }

        std::string_view protobufEnumValueToString(int value) const
        {
            const auto * enum_value_descriptor = field_descriptor.enum_type()->FindValueByNumber(value);
            if (!enum_value_descriptor)
                cannotConvertValue(toString(value), field_descriptor.type_name(), "String");
            return enum_value_descriptor->name();
        }

        void protobufEnumValueToStringAppend(int value, PaddedPODArray<UInt8> & str) const
        {
            auto name = protobufEnumValueToString(value);
            str.insert(name.data(), name.data() + name.length());
        }

        const std::shared_ptr<const DataTypeFixedString> fixed_string_data_type;
        const size_t n = 0;
        std::function<void(const std::string_view &)> write_function;
        std::function<void(PaddedPODArray<UInt8> &)> read_function;
        std::function<String()> default_function;
        std::unordered_map<std::string_view, int> string_to_protobuf_enum_value_map;
        PaddedPODArray<UInt8> text_buffer;
        std::optional<PaddedPODArray<UInt8>> default_string;
    };


    /// Serializes ColumnVector<NumberType> containing enum values to a field of any type
    /// except TYPE_MESSAGE, TYPE_GROUP, TYPE_FLOAT, TYPE_DOUBLE, TYPE_BOOL.
    /// NumberType can be either Int8 or Int16.
    template <typename NumberType>
    class ProtobufSerializerEnum : public ProtobufSerializerNumber<NumberType>
    {
    public:
        using ColumnType = ColumnVector<NumberType>;
        using EnumDataType = DataTypeEnum<NumberType>;
        using BaseClass = ProtobufSerializerNumber<NumberType>;

        ProtobufSerializerEnum(
            const std::shared_ptr<const EnumDataType> & enum_data_type_,
            const FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : BaseClass(field_descriptor_, reader_or_writer_), enum_data_type(enum_data_type_)
        {
            assert(enum_data_type);
            setFunctions();
            prepareEnumMapping();
        }

    private:
        void setFunctions()
        {
            switch (this->field_typeid)
            {
                case FieldTypeId::TYPE_INT32:
                case FieldTypeId::TYPE_SINT32:
                case FieldTypeId::TYPE_UINT32:
                case FieldTypeId::TYPE_INT64:
                case FieldTypeId::TYPE_SINT64:
                case FieldTypeId::TYPE_UINT64:
                case FieldTypeId::TYPE_FIXED32:
                case FieldTypeId::TYPE_SFIXED32:
                case FieldTypeId::TYPE_FIXED64:
                case FieldTypeId::TYPE_SFIXED64:
                {
                    auto base_read_function = this->read_function;
                    this->read_function = [this, base_read_function]() -> NumberType
                    {
                        NumberType value = base_read_function();
                        checkEnumDataTypeValue(value);
                        return value;
                    };

                    auto base_default_function = this->default_function;
                    this->default_function = [this, base_default_function]() -> NumberType
                    {
                        auto value = base_default_function();
                        checkEnumDataTypeValue(value);
                        return value;
                    };
                    break;
                }

                case FieldTypeId::TYPE_STRING:
                case FieldTypeId::TYPE_BYTES:
                {
                    this->write_function = [this](NumberType value)
                    {
                        writeStr(enumDataTypeValueToString(value));
                    };

                    this->read_function = [this]() -> NumberType
                    {
                        readStr(this->text_buffer);
                        return stringToEnumDataTypeValue(this->text_buffer);
                    };

                    this->default_function = [this]() -> NumberType
                    {
                        return stringToEnumDataTypeValue(this->field_descriptor.default_value_string());
                    };
                    break;
                }

                case FieldTypeId::TYPE_ENUM:
                {
                    this->write_function = [this](NumberType value) { writeInt(enumDataTypeValueToProtobufEnumValue(value)); };
                    this->read_function = [this]() -> NumberType { return protobufEnumValueToEnumDataTypeValue(readInt()); };
                    this->default_function = [this]() -> NumberType { return protobufEnumValueToEnumDataTypeValue(this->field_descriptor.default_value_enum()->number()); };
                    break;
                }

                default:
                    failedToSetFunctions();
            }
        }

        [[noreturn]] void failedToSetFunctions()
        {
            throw Exception(
                "The field " + quoteString(this->field_descriptor.full_name()) + " has an incompatible type " + this->field_descriptor.type_name()
                    + " for serialization of the data type " + quoteString(enum_data_type->getName()),
                ErrorCodes::DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD);
        }

        void checkEnumDataTypeValue(NumberType value)
        {
            enum_data_type->findByValue(value); /// Throws an exception if the value isn't defined in the DataTypeEnum.
        }

        std::string_view enumDataTypeValueToString(NumberType value) const { return std::string_view{enum_data_type->getNameForValue(value)}; }
        NumberType stringToEnumDataTypeValue(const String & str) const { return enum_data_type->getValue(str); }

        void prepareEnumMapping()
        {
            if (this->field_typeid != FieldTypeId::TYPE_ENUM)
                return;

            const auto & enum_descriptor = *this->field_descriptor.enum_type();

            /// We have two mappings:
            /// enum_data_type: "string->NumberType" and protobuf_enum: string->int".
            /// And here we want to make from those two mapping a new mapping "NumberType->int" (if we're writing protobuf data),
            /// or "int->NumberType" (if we're reading protobuf data).

            auto add_to_mapping = [&](NumberType enum_data_type_value, int protobuf_enum_value)
            {
                if (this->writer)
                    enum_data_type_value_to_protobuf_enum_value_map.emplace(enum_data_type_value, protobuf_enum_value);
                else
                    protobuf_enum_value_to_enum_data_type_value_map.emplace(protobuf_enum_value, enum_data_type_value);
            };

            auto iless = [](const std::string_view & s1, const std::string_view & s2) { return ColumnNameWithProtobufFieldNameComparator::less(s1, s2); };
            boost::container::flat_map<std::string_view, int, decltype(iless)> string_to_protobuf_enum_value_map;
            typename decltype(string_to_protobuf_enum_value_map)::sequence_type string_to_protobuf_enum_value_seq;
            for (int i : ext::range(enum_descriptor.value_count()))
                string_to_protobuf_enum_value_seq.emplace_back(enum_descriptor.value(i)->name(), enum_descriptor.value(i)->number());
            string_to_protobuf_enum_value_map.adopt_sequence(std::move(string_to_protobuf_enum_value_seq));

            std::vector<NumberType> not_found_by_name_values;
            not_found_by_name_values.reserve(enum_data_type->getValues().size());

            /// Find mapping between enum_data_type and protobuf_enum by name (case insensitively),
            /// i.e. we add to the mapping
            /// NumberType(enum_data_type) -> "NAME"(enum_data_type) ->
            /// -> "NAME"(protobuf_enum, same name) -> int(protobuf_enum)
            for (const auto & [name, value] : enum_data_type->getValues())
            {
                auto it = string_to_protobuf_enum_value_map.find(name);
                if (it != string_to_protobuf_enum_value_map.end())
                    add_to_mapping(value, it->second);
                else
                    not_found_by_name_values.push_back(value);
            }

            if (!not_found_by_name_values.empty())
            {
                /// Find mapping between two enum_data_type and protobuf_enum by value.
                /// If the same value has different names in enum_data_type and protobuf_enum
                /// we can still add it to our mapping, i.e. we add to the mapping
                /// NumberType(enum_data_type) -> int(protobuf_enum, same value)
                for (NumberType value : not_found_by_name_values)
                {
                    if (enum_descriptor.FindValueByNumber(value))
                        add_to_mapping(value, value);
                }
            }

            size_t num_mapped_values = this->writer ? enum_data_type_value_to_protobuf_enum_value_map.size()
                                                    : protobuf_enum_value_to_enum_data_type_value_map.size();

            if (!num_mapped_values && !enum_data_type->getValues().empty() && enum_descriptor.value_count())
            {
                throw Exception(
                    "Couldn't find mapping between data type " + enum_data_type->getName() + " and the enum " + quoteString(enum_descriptor.full_name())
                        + " in the protobuf schema",
                    ErrorCodes::DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD);
            }
        }

        int enumDataTypeValueToProtobufEnumValue(NumberType value) const
        {
            auto it = enum_data_type_value_to_protobuf_enum_value_map.find(value);
            if (it == enum_data_type_value_to_protobuf_enum_value_map.end())
                cannotConvertValue(toString(value), enum_data_type->getName(), this->field_descriptor.type_name());
            return it->second;
        }

        NumberType protobufEnumValueToEnumDataTypeValue(int value) const
        {
            auto it = protobuf_enum_value_to_enum_data_type_value_map.find(value);
            if (it == protobuf_enum_value_to_enum_data_type_value_map.end())
               cannotConvertValue(toString(value), this->field_descriptor.type_name(), enum_data_type->getName());
            return it->second;
        }

        Int64 readInt() { return ProtobufSerializerSingleValue::readInt(); }
        void writeInt(Int64 value) { ProtobufSerializerSingleValue::writeInt(value); }
        void writeStr(const std::string_view & str) { ProtobufSerializerSingleValue::writeStr(str); }
        void readStr(String & str) { ProtobufSerializerSingleValue::readStr(str); }
        [[noreturn]] void cannotConvertValue(const std::string_view & src_value, const std::string_view & src_type_name, const std::string_view & dest_type_name) const { ProtobufSerializerSingleValue::cannotConvertValue(src_value, src_type_name, dest_type_name); }

        const std::shared_ptr<const EnumDataType> enum_data_type;
        std::unordered_map<NumberType, int> enum_data_type_value_to_protobuf_enum_value_map;
        std::unordered_map<int, NumberType> protobuf_enum_value_to_enum_data_type_value_map;
    };


    /// Serializes a ColumnDecimal<DecimalType> to any field except TYPE_MESSAGE, TYPE_GROUP, TYPE_ENUM.
    /// DecimalType must be one of the following types: Decimal32, Decimal64, Decimal128, Decimal256, DateTime64.
    template <typename DecimalType>
    class ProtobufSerializerDecimal : public ProtobufSerializerSingleValue
    {
    public:
        using ColumnType = ColumnDecimal<DecimalType>;

        ProtobufSerializerDecimal(
            const DataTypeDecimalBase<DecimalType> & decimal_data_type_,
            const FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(field_descriptor_, reader_or_writer_)
            , precision(decimal_data_type_.getPrecision())
            , scale(decimal_data_type_.getScale())
        {
            setFunctions();
        }

        void writeRow(size_t row_num) override
        {
            const auto & column_decimal = assert_cast<const ColumnType &>(*column);
            write_function(column_decimal.getElement(row_num));
        }

        void readRow(size_t row_num) override
        {
            DecimalType decimal = read_function();
            auto & column_decimal = assert_cast<ColumnType &>(column->assumeMutableRef());
            if (row_num < column_decimal.size())
                column_decimal.getElement(row_num) = decimal;
            else
                column_decimal.insertValue(decimal);
        }

        void insertDefaults(size_t row_num) override
        {
            auto & column_decimal = assert_cast<ColumnType &>(column->assumeMutableRef());
            if (row_num < column_decimal.size())
                return;
            column_decimal.insertValue(getDefaultDecimal());
        }

    private:
        void setFunctions()
        {
            switch (field_typeid)
            {
                case FieldTypeId::TYPE_INT32:
                {
                    write_function = [this](const DecimalType & decimal) { writeInt(decimalToNumber<Int32>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readInt()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_int32()); };
                    break;
                }

                case FieldTypeId::TYPE_SINT32:
                {
                    write_function = [this](const DecimalType & decimal) { writeSInt(decimalToNumber<Int32>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readSInt()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_int32()); };
                    break;
                }

                case FieldTypeId::TYPE_UINT32:
                {
                    write_function = [this](const DecimalType & decimal) { writeUInt(decimalToNumber<UInt32>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readUInt()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_uint32()); };
                    break;
                }

                case FieldTypeId::TYPE_INT64:
                {
                    write_function = [this](const DecimalType & decimal) { writeInt(decimalToNumber<Int64>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readInt()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_int64()); };
                    break;
                }

                case FieldTypeId::TYPE_SINT64:
                {
                    write_function = [this](const DecimalType & decimal) { writeSInt(decimalToNumber<Int64>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readSInt()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_int64()); };
                    break;
                }

                case FieldTypeId::TYPE_UINT64:
                {
                    write_function = [this](const DecimalType & decimal) { writeUInt(decimalToNumber<UInt64>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readUInt()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_uint64()); };
                    break;
                }

                case FieldTypeId::TYPE_FIXED32:
                {
                    write_function = [this](const DecimalType & decimal) { writeFixed<UInt32>(decimalToNumber<UInt32>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readFixed<UInt32>()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_uint32()); };
                    break;
                }

                case FieldTypeId::TYPE_SFIXED32:
                {
                    write_function = [this](const DecimalType & decimal) { writeFixed<Int32>(decimalToNumber<Int32>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readFixed<Int32>()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_int32()); };
                    break;
                }

                case FieldTypeId::TYPE_FIXED64:
                {
                    write_function = [this](const DecimalType & decimal) { writeFixed<UInt64>(decimalToNumber<UInt64>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readFixed<UInt64>()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_uint64()); };
                    break;
                }

                case FieldTypeId::TYPE_SFIXED64:
                {
                    write_function = [this](const DecimalType & decimal) { writeFixed<Int64>(decimalToNumber<Int64>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readFixed<Int64>()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_int64()); };
                    break;
                }

                case FieldTypeId::TYPE_FLOAT:
                {
                    write_function = [this](const DecimalType & decimal) { writeFixed<Float32>(decimalToNumber<Float32>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readFixed<Float32>()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_float()); };
                    break;
                }

                case FieldTypeId::TYPE_DOUBLE:
                {
                    write_function = [this](const DecimalType & decimal) { writeFixed<Float64>(decimalToNumber<Float64>(decimal)); };
                    read_function = [this]() -> DecimalType { return numberToDecimal(readFixed<Float64>()); };
                    default_function = [this]() -> DecimalType { return numberToDecimal(field_descriptor.default_value_double()); };
                    break;
                }

                case FieldTypeId::TYPE_BOOL:
                {
                    if (std::is_same_v<DecimalType, DateTime64>)
                        failedToSetFunctions();
                    else
                    {
                        write_function = [this](const DecimalType & decimal)
                        {
                            if (decimal.value == 0)
                                writeInt(0);
                            else if (DecimalComparison<DecimalType, int, EqualsOp>::compare(decimal, 1, scale, 0))
                                writeInt(1);
                            else
                            {
                                WriteBufferFromOwnString buf;
                                writeText(decimal, scale, buf);
                                cannotConvertValue(buf.str(), TypeName<DecimalType>, field_descriptor.type_name());
                            }
                        };

                        read_function = [this]() -> DecimalType
                        {
                            UInt64 u64 = readUInt();
                            if (u64 < 2)
                                return numberToDecimal(static_cast<UInt64>(u64 != 0));
                            else
                                cannotConvertValue(toString(u64), field_descriptor.type_name(), TypeName<DecimalType>);
                        };

                        default_function = [this]() -> DecimalType
                        {
                            return numberToDecimal(static_cast<Int64>(field_descriptor.default_value_bool()));
                        };
                    }
                    break;
                }

                case FieldTypeId::TYPE_STRING:
                case FieldTypeId::TYPE_BYTES:
                {
                    write_function = [this](const DecimalType & decimal)
                    {
                        decimalToString(decimal, text_buffer);
                        writeStr(text_buffer);
                    };

                    read_function = [this]() -> DecimalType
                    {
                        readStr(text_buffer);
                        return stringToDecimal(text_buffer);
                    };

                    default_function = [this]() -> DecimalType { return stringToDecimal(field_descriptor.default_value_string()); };
                    break;
                }

                default:
                    failedToSetFunctions();
            }
        }

        [[noreturn]] void failedToSetFunctions()
        {
            throw Exception(
                "The field " + quoteString(field_descriptor.full_name()) + " has an incompatible type " + field_descriptor.type_name()
                    + " for serialization of the data type " + quoteString(TypeName<DecimalType>),
                ErrorCodes::DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD);
        }

        DecimalType getDefaultDecimal()
        {
            if (!default_decimal)
                default_decimal = default_function();
            return *default_decimal;
        }

        template <typename NumberType>
        DecimalType numberToDecimal(NumberType value) const
        {
            return convertToDecimal<DataTypeNumber<NumberType>, DataTypeDecimal<DecimalType>>(value, scale);
        }

        template <typename NumberType>
        NumberType decimalToNumber(const DecimalType & decimal) const
        {
            return DecimalUtils::convertTo<NumberType>(decimal, scale);
        }

        void decimalToString(const DecimalType & decimal, String & str) const
        {
            WriteBufferFromString buf{str};
            if constexpr (std::is_same_v<DecimalType, DateTime64>)
               writeDateTimeText(decimal, scale, buf);
            else
                writeText(decimal, scale, buf);
        }

        DecimalType stringToDecimal(const String & str) const
        {
            ReadBufferFromString buf(str);
            DecimalType decimal{0};
            if constexpr (std::is_same_v<DecimalType, DateTime64>)
                readDateTime64Text(decimal, scale, buf);
            else
                SerializationDecimal<DecimalType>::readText(decimal, buf, precision, scale);
            return decimal;
        }

        const UInt32 precision;
        const UInt32 scale;
        std::function<void(const DecimalType &)> write_function;
        std::function<DecimalType()> read_function;
        std::function<DecimalType()> default_function;
        std::optional<DecimalType> default_decimal;
        String text_buffer;
    };

    using ProtobufSerializerDateTime64 = ProtobufSerializerDecimal<DateTime64>;


    /// Serializes a ColumnVector<UInt16> containing dates to a field of any type except TYPE_MESSAGE, TYPE_GROUP, TYPE_BOOL, TYPE_ENUM.
    class ProtobufSerializerDate : public ProtobufSerializerNumber<UInt16>
    {
    public:
        ProtobufSerializerDate(
            const FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerNumber<UInt16>(field_descriptor_, reader_or_writer_)
        {
            setFunctions();
        }

    private:
        void setFunctions()
        {
            switch (field_typeid)
            {
                case FieldTypeId::TYPE_INT32:
                case FieldTypeId::TYPE_SINT32:
                case FieldTypeId::TYPE_UINT32:
                case FieldTypeId::TYPE_INT64:
                case FieldTypeId::TYPE_SINT64:
                case FieldTypeId::TYPE_UINT64:
                case FieldTypeId::TYPE_FIXED32:
                case FieldTypeId::TYPE_SFIXED32:
                case FieldTypeId::TYPE_FIXED64:
                case FieldTypeId::TYPE_SFIXED64:
                case FieldTypeId::TYPE_FLOAT:
                case FieldTypeId::TYPE_DOUBLE:
                    break; /// already set in ProtobufSerializerNumber<UInt16>::setFunctions().

                case FieldTypeId::TYPE_STRING:
                case FieldTypeId::TYPE_BYTES:
                {
                    write_function = [this](UInt16 value)
                    {
                        dateToString(static_cast<DayNum>(value), text_buffer);
                        writeStr(text_buffer);
                    };

                    read_function = [this]() -> UInt16
                    {
                        readStr(text_buffer);
                        return stringToDate(text_buffer);
                    };

                    default_function = [this]() -> UInt16 { return stringToDate(field_descriptor.default_value_string()); };
                    break;
                }

                default:
                    failedToSetFunctions();
            }
        }

        static void dateToString(DayNum date, String & str)
        {
            WriteBufferFromString buf{str};
            writeText(date, buf);
        }

        static DayNum stringToDate(const String & str)
        {
            DayNum date;
            ReadBufferFromString buf{str};
            readDateText(date, buf);
            return date;
        }

        [[noreturn]] void failedToSetFunctions()
        {
            throw Exception(
                "The field " + quoteString(field_descriptor.full_name()) + " has an incompatible type " + field_descriptor.type_name()
                    + " for serialization of the data type 'Date'",
                ErrorCodes::DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD);
        }
    };


    /// Serializes a ColumnVector<UInt32> containing dates to a field of any type except TYPE_MESSAGE, TYPE_GROUP, TYPE_BOOL, TYPE_ENUM.
    class ProtobufSerializerDateTime : public ProtobufSerializerNumber<UInt32>
    {
    public:
        ProtobufSerializerDateTime(
            const FieldDescriptor & field_descriptor_, const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerNumber<UInt32>(field_descriptor_, reader_or_writer_)
        {
            setFunctions();
        }

    protected:
        void setFunctions()
        {
            switch (field_typeid)
            {
                case FieldTypeId::TYPE_INT32:
                case FieldTypeId::TYPE_SINT32:
                case FieldTypeId::TYPE_UINT32:
                case FieldTypeId::TYPE_INT64:
                case FieldTypeId::TYPE_SINT64:
                case FieldTypeId::TYPE_UINT64:
                case FieldTypeId::TYPE_FIXED32:
                case FieldTypeId::TYPE_SFIXED32:
                case FieldTypeId::TYPE_FIXED64:
                case FieldTypeId::TYPE_SFIXED64:
                case FieldTypeId::TYPE_FLOAT:
                case FieldTypeId::TYPE_DOUBLE:
                    break; /// already set in ProtobufSerializerNumber<UInt32>::setFunctions().

                case FieldTypeId::TYPE_STRING:
                case FieldTypeId::TYPE_BYTES:
                {
                    write_function = [this](UInt32 value)
                    {
                        dateTimeToString(value, text_buffer);
                        writeStr(text_buffer);
                    };

                    read_function = [this]() -> UInt32
                    {
                        readStr(text_buffer);
                        return stringToDateTime(text_buffer);
                    };

                    default_function = [this]() -> UInt32 { return stringToDateTime(field_descriptor.default_value_string()); };
                    break;
                }

                default:
                    failedToSetFunctions();
            }
        }

        static void dateTimeToString(time_t tm, String & str)
        {
            WriteBufferFromString buf{str};
            writeDateTimeText(tm, buf);
        }

        static time_t stringToDateTime(const String & str)
        {
            ReadBufferFromString buf{str};
            time_t tm = 0;
            readDateTimeText(tm, buf);
            if (tm < 0)
                tm = 0;
            return tm;
        }

        [[noreturn]] void failedToSetFunctions()
        {
            throw Exception(
                "The field " + quoteString(field_descriptor.full_name()) + " has an incompatible type " + field_descriptor.type_name()
                    + " for serialization of the data type 'DateTime'",
                ErrorCodes::DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD);
        }
    };


    /// Serializes a ColumnVector<UUID> containing UUIDs to a field of type TYPE_STRING or TYPE_BYTES.
    class ProtobufSerializerUUID : public ProtobufSerializerSingleValue
    {
    public:
        ProtobufSerializerUUID(
            const google::protobuf::FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(field_descriptor_, reader_or_writer_)
        {
            setFunctions();
        }

        void writeRow(size_t row_num) override
        {
            const auto & column_vector = assert_cast<const ColumnVector<UUID> &>(*column);
            write_function(column_vector.getElement(row_num));
        }

        void readRow(size_t row_num) override
        {
            UUID value = read_function();
            auto & column_vector = assert_cast<ColumnVector<UUID> &>(column->assumeMutableRef());
            if (row_num < column_vector.size())
                column_vector.getElement(row_num) = value;
            else
                column_vector.insertValue(value);
        }

        void insertDefaults(size_t row_num) override
        {
            auto & column_vector = assert_cast<ColumnVector<UUID> &>(column->assumeMutableRef());
            if (row_num < column_vector.size())
                return;
            column_vector.insertDefault();
        }

    private:
        void setFunctions()
        {
            if ((field_typeid != FieldTypeId::TYPE_STRING) && (field_typeid != FieldTypeId::TYPE_BYTES))
            {
                throw Exception(
                    "The field " + quoteString(field_descriptor.full_name()) + " has an incompatible type " + field_descriptor.type_name()
                        + " for serialization of the data type UUID",
                    ErrorCodes::DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD);
            }

            write_function = [this](UUID value)
            {
                uuidToString(value, text_buffer);
                writeStr(text_buffer);
            };

            read_function = [this]() -> UUID
            {
                readStr(text_buffer);
                return parse<UUID>(text_buffer);
            };

            default_function = [this]() -> UUID { return parse<UUID>(field_descriptor.default_value_string()); };
        }

        static void uuidToString(const UUID & uuid, String & str)
        {
            WriteBufferFromString buf{str};
            writeText(uuid, buf);
        }

        std::function<void(UUID)> write_function;
        std::function<UUID()> read_function;
        std::function<UUID()> default_function;
        String text_buffer;
    };


    using ProtobufSerializerInterval = ProtobufSerializerNumber<Int64>;


    /// Serializes a ColumnAggregateFunction to a field of type TYPE_STRING or TYPE_BYTES.
    class ProtobufSerializerAggregateFunction : public ProtobufSerializerSingleValue
    {
    public:
        ProtobufSerializerAggregateFunction(
            const std::shared_ptr<const DataTypeAggregateFunction> & aggregate_function_data_type_,
            const google::protobuf::FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(field_descriptor_, reader_or_writer_)
            , aggregate_function_data_type(aggregate_function_data_type_)
            , aggregate_function(aggregate_function_data_type->getFunction())
        {
            if ((field_typeid != FieldTypeId::TYPE_STRING) && (field_typeid != FieldTypeId::TYPE_BYTES))
            {
                throw Exception(
                    "The field " + quoteString(field_descriptor.full_name()) + " has an incompatible type " + field_descriptor.type_name()
                        + " for serialization of the data type " + quoteString(aggregate_function_data_type->getName()),
                    ErrorCodes::DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD);
            }
        }

        void writeRow(size_t row_num) override
        {
            const auto & column_af = assert_cast<const ColumnAggregateFunction &>(*column);
            dataToString(column_af.getData()[row_num], text_buffer);
            writeStr(text_buffer);
        }

        void readRow(size_t row_num) override
        {
            auto & column_af = assert_cast<ColumnAggregateFunction &>(column->assumeMutableRef());
            Arena & arena = column_af.createOrGetArena();
            AggregateDataPtr data;
            readStr(text_buffer);
            data = stringToData(text_buffer, arena);

            if (row_num < column_af.size())
            {
                auto * old_data = std::exchange(column_af.getData()[row_num], data);
                aggregate_function->destroy(old_data);
            }
            else
                column_af.getData().push_back(data);
        }

        void insertDefaults(size_t row_num) override
        {
            auto & column_af = assert_cast<ColumnAggregateFunction &>(column->assumeMutableRef());
            if (row_num < column_af.size())
                return;

            Arena & arena = column_af.createOrGetArena();
            AggregateDataPtr data = stringToData(field_descriptor.default_value_string(), arena);
            column_af.getData().push_back(data);
        }

    private:
        void dataToString(ConstAggregateDataPtr data, String & str) const
        {
            WriteBufferFromString buf{str};
            aggregate_function->serialize(data, buf);
        }

        AggregateDataPtr stringToData(const String & str, Arena & arena) const
        {
            size_t size_of_state = aggregate_function->sizeOfData();
            AggregateDataPtr data = arena.alignedAlloc(size_of_state, aggregate_function->alignOfData());
            try
            {
                aggregate_function->create(data);
                ReadBufferFromMemory buf(str.data(), str.length());
                aggregate_function->deserialize(data, buf, &arena);
                return data;
            }
            catch (...)
            {
                aggregate_function->destroy(data);
                throw;
            }
        }

        const std::shared_ptr<const DataTypeAggregateFunction> aggregate_function_data_type;
        const AggregateFunctionPtr aggregate_function;
        String text_buffer;
    };


    /// Serializes a ColumnNullable.
    class ProtobufSerializerNullable : public ProtobufSerializer
    {
    public:
        explicit ProtobufSerializerNullable(std::unique_ptr<ProtobufSerializer> nested_serializer_)
            : nested_serializer(std::move(nested_serializer_))
        {
        }

        void setColumns(const ColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            column = columns[0];
            const auto & column_nullable = assert_cast<const ColumnNullable &>(*column);
            ColumnPtr nested_column = column_nullable.getNestedColumnPtr();
            nested_serializer->setColumns(&nested_column, 1);
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            ColumnPtr column0 = columns[0]->getPtr();
            setColumns(&column0, 1);
        }

        void writeRow(size_t row_num) override
        {
            const auto & column_nullable = assert_cast<const ColumnNullable &>(*column);
            const auto & null_map = column_nullable.getNullMapData();
            if (!null_map[row_num])
                nested_serializer->writeRow(row_num);
        }

        void readRow(size_t row_num) override
        {
            auto & column_nullable = assert_cast<ColumnNullable &>(column->assumeMutableRef());
            auto & nested_column = column_nullable.getNestedColumn();
            auto & null_map = column_nullable.getNullMapData();
            size_t old_size = null_map.size();

            nested_serializer->readRow(row_num);

            if (row_num < old_size)
            {
                null_map[row_num] = false;
            }
            else
            {
                size_t new_size = nested_column.size();
                if (new_size != old_size + 1)
                    throw Exception("Size of ColumnNullable is unexpected", ErrorCodes::LOGICAL_ERROR);
                try
                {
                    null_map.push_back(false);
                }
                catch (...)
                {
                    nested_column.popBack(1);
                    throw;
                }
            }
        }

        void insertDefaults(size_t row_num) override
        {
            auto & column_nullable = assert_cast<ColumnNullable &>(column->assumeMutableRef());
            if (row_num < column_nullable.size())
                return;
            column_nullable.insertDefault();
        }

    private:
        const std::unique_ptr<ProtobufSerializer> nested_serializer;
        ColumnPtr column;
    };


    /// Serializes a ColumnMap.
    class ProtobufSerializerMap : public ProtobufSerializer
    {
    public:
        explicit ProtobufSerializerMap(std::unique_ptr<ProtobufSerializer> nested_serializer_)
            : nested_serializer(std::move(nested_serializer_))
        {
        }

        void setColumns(const ColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            const auto & column_map = assert_cast<const ColumnMap &>(*columns[0]);
            ColumnPtr nested_column = column_map.getNestedColumnPtr();
            nested_serializer->setColumns(&nested_column, 1);
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            ColumnPtr column0 = columns[0]->getPtr();
            setColumns(&column0, 1);
        }

        void writeRow(size_t row_num) override { nested_serializer->writeRow(row_num); }
        void readRow(size_t row_num) override { nested_serializer->readRow(row_num); }
        void insertDefaults(size_t row_num) override { nested_serializer->insertDefaults(row_num); }

    private:
        const std::unique_ptr<ProtobufSerializer> nested_serializer;
    };


    /// Serializes a ColumnLowCardinality.
    class ProtobufSerializerLowCardinality : public ProtobufSerializer
    {
    public:
        explicit ProtobufSerializerLowCardinality(std::unique_ptr<ProtobufSerializer> nested_serializer_)
            : nested_serializer(std::move(nested_serializer_))
        {
        }

        void setColumns(const ColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            column = columns[0];
            const auto & column_lc = assert_cast<const ColumnLowCardinality &>(*column);
            ColumnPtr nested_column = column_lc.getDictionary().getNestedColumn();
            nested_serializer->setColumns(&nested_column, 1);
            read_value_column_set = false;
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            ColumnPtr column0 = columns[0]->getPtr();
            setColumns(&column0, 1);
        }

        void writeRow(size_t row_num) override
        {
            const auto & column_lc = assert_cast<const ColumnLowCardinality &>(*column);
            size_t unique_row_number = column_lc.getIndexes().getUInt(row_num);
            nested_serializer->writeRow(unique_row_number);
        }

        void readRow(size_t row_num) override
        {
            auto & column_lc = assert_cast<ColumnLowCardinality &>(column->assumeMutableRef());

            if (!read_value_column_set)
            {
                if (!read_value_column)
                {
                    ColumnPtr nested_column = column_lc.getDictionary().getNestedColumn();
                    read_value_column = nested_column->cloneEmpty();
                }
                nested_serializer->setColumns(&read_value_column, 1);
                read_value_column_set = true;
            }

            read_value_column->popBack(read_value_column->size());
            nested_serializer->readRow(0);

            if (row_num < column_lc.size())
            {
                if (row_num != column_lc.size() - 1)
                    throw Exception("Cannot replace an element in the middle of ColumnLowCardinality", ErrorCodes::LOGICAL_ERROR);
                column_lc.popBack(1);
            }

            column_lc.insertFromFullColumn(*read_value_column, 0);
        }

        void insertDefaults(size_t row_num) override
        {
            auto & column_lc = assert_cast<ColumnLowCardinality &>(column->assumeMutableRef());
            if (row_num < column_lc.size())
                return;

            if (!default_value_column)
            {
                ColumnPtr nested_column = column_lc.getDictionary().getNestedColumn();
                default_value_column = nested_column->cloneEmpty();
                nested_serializer->setColumns(&default_value_column, 1);
                nested_serializer->insertDefaults(0);
                read_value_column_set = false;
            }

            column_lc.insertFromFullColumn(*default_value_column, 0);
        }

    private:
        const std::unique_ptr<ProtobufSerializer> nested_serializer;
        ColumnPtr column;
        MutableColumnPtr read_value_column;
        bool read_value_column_set = false;
        MutableColumnPtr default_value_column;
    };


    /// Serializes a ColumnArray to a repeated field.
    class ProtobufSerializerArray : public ProtobufSerializer
    {
    public:
        explicit ProtobufSerializerArray(std::unique_ptr<ProtobufSerializer> element_serializer_)
            : element_serializer(std::move(element_serializer_))
        {
        }

        void setColumns(const ColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            column = columns[0];
            const auto & column_array = assert_cast<const ColumnArray &>(*column);
            ColumnPtr data_column = column_array.getDataPtr();
            element_serializer->setColumns(&data_column, 1);
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            ColumnPtr column0 = columns[0]->getPtr();
            setColumns(&column0, 1);
        }

        void writeRow(size_t row_num) override
        {
            const auto & column_array = assert_cast<const ColumnArray &>(*column);
            const auto & offsets = column_array.getOffsets();
            for (size_t i : ext::range(offsets[row_num - 1], offsets[row_num]))
                element_serializer->writeRow(i);
        }

        void readRow(size_t row_num) override
        {
            auto & column_array = assert_cast<ColumnArray &>(column->assumeMutableRef());
            auto & offsets = column_array.getOffsets();
            size_t old_size = offsets.size();
            if (row_num + 1 < old_size)
                throw Exception("Cannot replace an element in the middle of ColumnArray", ErrorCodes::LOGICAL_ERROR);
            auto data_column = column_array.getDataPtr();
            size_t old_data_size = data_column->size();

            try
            {
                element_serializer->readRow(old_data_size);
                size_t data_size = data_column->size();
                if (data_size != old_data_size + 1)
                    throw Exception("Size of ColumnArray is unexpected", ErrorCodes::LOGICAL_ERROR);

                if (row_num < old_size)
                    offsets.back() = data_size;
                else
                    offsets.push_back(data_size);
            }
            catch (...)
            {
                if (data_column->size() > old_data_size)
                    data_column->assumeMutableRef().popBack(data_column->size() - old_data_size);
                if (offsets.size() > old_size)
                    column_array.getOffsetsColumn().popBack(offsets.size() - old_size);
                throw;
            }
        }

        void insertDefaults(size_t row_num) override
        {
            auto & column_array = assert_cast<ColumnArray &>(column->assumeMutableRef());
            if (row_num < column_array.size())
                return;
            column_array.insertDefault();
        }

    private:
        const std::unique_ptr<ProtobufSerializer> element_serializer;
        ColumnPtr column;
    };


    /// Serializes a ColumnTuple as a repeated field (just like we serialize arrays).
    class ProtobufSerializerTupleAsArray : public ProtobufSerializer
    {
    public:
        ProtobufSerializerTupleAsArray(
            const std::shared_ptr<const DataTypeTuple> & tuple_data_type_,
            const FieldDescriptor & field_descriptor_,
            std::vector<std::unique_ptr<ProtobufSerializer>> element_serializers_)
            : tuple_data_type(tuple_data_type_)
            , tuple_size(tuple_data_type->getElements().size())
            , field_descriptor(field_descriptor_)
            , element_serializers(std::move(element_serializers_))
        {
            assert(tuple_size);
            assert(tuple_size == element_serializers.size());
        }

        void setColumns(const ColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            column = columns[0];
            const auto & column_tuple = assert_cast<const ColumnTuple &>(*column);
            for (size_t i : ext::range(tuple_size))
            {
                auto element_column = column_tuple.getColumnPtr(i);
                element_serializers[i]->setColumns(&element_column, 1);
            }
            current_element_index = 0;
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            ColumnPtr column0 = columns[0]->getPtr();
            setColumns(&column0, 1);
        }

        void writeRow(size_t row_num) override
        {
            for (size_t i : ext::range(tuple_size))
                element_serializers[i]->writeRow(row_num);
        }

        void readRow(size_t row_num) override
        {
            auto & column_tuple = assert_cast<ColumnTuple &>(column->assumeMutableRef());

            size_t old_size = column_tuple.size();
            if (row_num >= old_size)
                current_element_index = 0;

            insertDefaults(row_num);

            if (current_element_index >= tuple_size)
            {
                throw Exception(
                    "Too many (" + std::to_string(current_element_index) + ") elements was read from the field "
                        + field_descriptor.full_name() + " to fit in the data type " + tuple_data_type->getName(),
                    ErrorCodes::PROTOBUF_BAD_CAST);
            }

            element_serializers[current_element_index]->readRow(row_num);
            ++current_element_index;
        }

        void insertDefaults(size_t row_num) override
        {
            auto & column_tuple = assert_cast<ColumnTuple &>(column->assumeMutableRef());
            size_t old_size = column_tuple.size();

            if (row_num > old_size)
                return;

            try
            {
                for (size_t i : ext::range(tuple_size))
                    element_serializers[i]->insertDefaults(row_num);
            }
            catch (...)
            {
                for (size_t i : ext::range(tuple_size))
                {
                    auto element_column = column_tuple.getColumnPtr(i)->assumeMutable();
                    if (element_column->size() > old_size)
                        element_column->popBack(element_column->size() - old_size);
                }
                throw;
            }
        }

    private:
        const std::shared_ptr<const DataTypeTuple> tuple_data_type;
        const size_t tuple_size;
        const FieldDescriptor & field_descriptor;
        const std::vector<std::unique_ptr<ProtobufSerializer>> element_serializers;
        ColumnPtr column;
        size_t current_element_index = 0;
    };


    /// Serializes a message (root or nested) in the protobuf schema.
    class ProtobufSerializerMessage : public ProtobufSerializer
    {
    public:
        struct FieldDesc
        {
            size_t column_index;
            size_t num_columns;
            const FieldDescriptor * field_descriptor;
            std::unique_ptr<ProtobufSerializer> field_serializer;
        };

        ProtobufSerializerMessage(
            std::vector<FieldDesc> field_descs_,
            const FieldDescriptor * parent_field_descriptor_,
            bool with_length_delimiter_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : parent_field_descriptor(parent_field_descriptor_)
            , with_length_delimiter(with_length_delimiter_)
            , should_skip_if_empty(parent_field_descriptor ? shouldSkipZeroOrEmpty(*parent_field_descriptor) : false)
            , reader(reader_or_writer_.reader)
            , writer(reader_or_writer_.writer)
        {
            field_infos.reserve(field_descs_.size());
            for (auto & desc : field_descs_)
                field_infos.emplace_back(desc.column_index, desc.num_columns, *desc.field_descriptor, std::move(desc.field_serializer));

            std::sort(field_infos.begin(), field_infos.end(),
                      [](const FieldInfo & lhs, const FieldInfo & rhs) { return lhs.field_tag < rhs.field_tag; });

            for (size_t i : ext::range(field_infos.size()))
                field_index_by_field_tag.emplace(field_infos[i].field_tag, i);
        }

        void setColumns(const ColumnPtr * columns_, size_t num_columns_) override
        {
            columns.assign(columns_, columns_ + num_columns_);

            for (const FieldInfo & info : field_infos)
                info.field_serializer->setColumns(columns.data() + info.column_index, info.num_columns);

            if (reader)
            {
                missing_column_indices.clear();
                missing_column_indices.reserve(num_columns_);
                size_t current_idx = 0;
                for (const FieldInfo & info : field_infos)
                {
                    while (current_idx < info.column_index)
                        missing_column_indices.push_back(current_idx++);
                    current_idx = info.column_index + info.num_columns;
                }
                while (current_idx < num_columns_)
                    missing_column_indices.push_back(current_idx++);
            }
        }

        void setColumns(const MutableColumnPtr * columns_, size_t num_columns_) override
        {
            Columns cols;
            cols.reserve(num_columns_);
            for (size_t i : ext::range(num_columns_))
                cols.push_back(columns_[i]->getPtr());
            setColumns(cols.data(), cols.size());
        }

        void writeRow(size_t row_num) override
        {
            if (parent_field_descriptor)
                writer->startNestedMessage();
            else
                writer->startMessage();

            for (const FieldInfo & info : field_infos)
            {
                if (info.should_pack_repeated)
                    writer->startRepeatedPack();
                info.field_serializer->writeRow(row_num);
                if (info.should_pack_repeated)
                    writer->endRepeatedPack(info.field_tag, true);
            }

            if (parent_field_descriptor)
            {
                bool is_group = (parent_field_descriptor->type() == FieldTypeId::TYPE_GROUP);
                writer->endNestedMessage(parent_field_descriptor->number(), is_group, should_skip_if_empty);
            }
            else
                writer->endMessage(with_length_delimiter);
        }

        void readRow(size_t row_num) override
        {
            if (parent_field_descriptor)
                reader->startNestedMessage();
            else
                reader->startMessage(with_length_delimiter);

            if (!field_infos.empty())
            {
                last_field_index = 0;
                last_field_tag = field_infos[0].field_tag;
                size_t old_size = columns.empty() ? 0 : columns[0]->size();

                try
                {
                    int field_tag;
                    while (reader->readFieldNumber(field_tag))
                    {
                        size_t field_index = findFieldIndexByFieldTag(field_tag);
                        if (field_index == static_cast<size_t>(-1))
                            continue;
                        auto * field_serializer = field_infos[field_index].field_serializer.get();
                        field_serializer->readRow(row_num);
                        field_infos[field_index].field_read = true;
                    }

                    for (auto & info : field_infos)
                    {
                        if (info.field_read)
                            info.field_read = false;
                        else
                            info.field_serializer->insertDefaults(row_num);
                    }
                }
                catch (...)
                {
                    for (auto & column : columns)
                    {
                        if (column->size() > old_size)
                            column->assumeMutableRef().popBack(column->size() - old_size);
                    }
                    throw;
                }
            }

            if (parent_field_descriptor)
                reader->endNestedMessage();
            else
                reader->endMessage(false);
            addDefaultsToMissingColumns(row_num);
        }

        void insertDefaults(size_t row_num) override
        {
            for (const FieldInfo & info : field_infos)
                info.field_serializer->insertDefaults(row_num);
            addDefaultsToMissingColumns(row_num);
        }

    private:
        size_t findFieldIndexByFieldTag(int field_tag)
        {
            while (true)
            {
                if (field_tag == last_field_tag)
                    return last_field_index;
                if (field_tag < last_field_tag)
                    break;
                if (++last_field_index >= field_infos.size())
                    break;
                last_field_tag = field_infos[last_field_index].field_tag;
            }
            last_field_tag = field_tag;
            auto it = field_index_by_field_tag.find(field_tag);
            if (it == field_index_by_field_tag.end())
                last_field_index = static_cast<size_t>(-1);
            else
                last_field_index = it->second;
            return last_field_index;
        }

        void addDefaultsToMissingColumns(size_t row_num)
        {
            for (size_t column_idx : missing_column_indices)
            {
                auto & column = columns[column_idx];
                size_t old_size = column->size();
                if (row_num >= old_size)
                    column->assumeMutableRef().insertDefault();
            }
        }

        struct FieldInfo
        {
            FieldInfo(
                size_t column_index_,
                size_t num_columns_,
                const FieldDescriptor & field_descriptor_,
                std::unique_ptr<ProtobufSerializer> field_serializer_)
                : column_index(column_index_)
                , num_columns(num_columns_)
                , field_descriptor(&field_descriptor_)
                , field_tag(field_descriptor_.number())
                , should_pack_repeated(shouldPackRepeated(field_descriptor_))
                , field_serializer(std::move(field_serializer_))
            {
            }
            size_t column_index;
            size_t num_columns;
            const FieldDescriptor * field_descriptor;
            int field_tag;
            bool should_pack_repeated;
            std::unique_ptr<ProtobufSerializer> field_serializer;
            bool field_read = false;
        };

        const FieldDescriptor * const parent_field_descriptor;
        const bool with_length_delimiter;
        const bool should_skip_if_empty;
        ProtobufReader * const reader;
        ProtobufWriter * const writer;
        std::vector<FieldInfo> field_infos;
        std::unordered_map<int, size_t> field_index_by_field_tag;
        Columns columns;
        std::vector<size_t> missing_column_indices;
        int last_field_tag = 0;
        size_t last_field_index = static_cast<size_t>(-1);
    };


    /// Serializes a tuple with explicit names as a nested message.
    class ProtobufSerializerTupleAsNestedMessage : public ProtobufSerializer
    {
    public:
        explicit ProtobufSerializerTupleAsNestedMessage(std::unique_ptr<ProtobufSerializerMessage> nested_message_serializer_)
            : nested_message_serializer(std::move(nested_message_serializer_))
        {
        }

        void setColumns(const ColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            const auto & column_tuple = assert_cast<const ColumnTuple &>(*columns[0]);
            size_t tuple_size = column_tuple.tupleSize();
            assert(tuple_size);
            Columns element_columns;
            element_columns.reserve(tuple_size);
            for (size_t i : ext::range(tuple_size))
                element_columns.emplace_back(column_tuple.getColumnPtr(i));
            nested_message_serializer->setColumns(element_columns.data(), element_columns.size());
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            assert(num_columns == 1);
            ColumnPtr column0 = columns[0]->getPtr();
            setColumns(&column0, 1);
        }

        void writeRow(size_t row_num) override { nested_message_serializer->writeRow(row_num); }
        void readRow(size_t row_num) override { nested_message_serializer->readRow(row_num); }
        void insertDefaults(size_t row_num) override { nested_message_serializer->insertDefaults(row_num); }

    private:
        const std::unique_ptr<ProtobufSerializerMessage> nested_message_serializer;
    };


    /// Serializes a flattened Nested data type (an array of tuples with explicit names)
    /// as a repeated nested message.
    class ProtobufSerializerFlattenedNestedAsArrayOfNestedMessages : public ProtobufSerializer
    {
    public:
        explicit ProtobufSerializerFlattenedNestedAsArrayOfNestedMessages(
            std::unique_ptr<ProtobufSerializerMessage> nested_message_serializer_)
            : nested_message_serializer(std::move(nested_message_serializer_))
        {
        }

        void setColumns(const ColumnPtr * columns, size_t num_columns) override
        {
            assert(num_columns);
            data_columns.clear();
            data_columns.reserve(num_columns);
            offset_columns.clear();
            offset_columns.reserve(num_columns);

            for (size_t i : ext::range(num_columns))
            {
                const auto & column_array = assert_cast<const ColumnArray &>(*columns[i]);
                data_columns.emplace_back(column_array.getDataPtr());
                offset_columns.emplace_back(column_array.getOffsetsPtr());
            }

            std::sort(offset_columns.begin(), offset_columns.end());
            offset_columns.erase(std::unique(offset_columns.begin(), offset_columns.end()), offset_columns.end());

            nested_message_serializer->setColumns(data_columns.data(), data_columns.size());
        }

        void setColumns(const MutableColumnPtr * columns, size_t num_columns) override
        {
            Columns cols;
            cols.reserve(num_columns);
            for (size_t i : ext::range(num_columns))
                cols.push_back(columns[i]->getPtr());
            setColumns(cols.data(), cols.size());
        }

        void writeRow(size_t row_num) override
        {
            const auto & offset_column0 = assert_cast<const ColumnArray::ColumnOffsets &>(*offset_columns[0]);
            size_t start_offset = offset_column0.getElement(row_num - 1);
            size_t end_offset = offset_column0.getElement(row_num);
            for (size_t i : ext::range(1, offset_columns.size()))
            {
                const auto & offset_column = assert_cast<const ColumnArray::ColumnOffsets &>(*offset_columns[i]);
                if (offset_column.getElement(row_num) != end_offset)
                    throw Exception("Components of FlattenedNested have different sizes", ErrorCodes::PROTOBUF_BAD_CAST);
            }
            for (size_t i : ext::range(start_offset, end_offset))
                nested_message_serializer->writeRow(i);
        }

        void readRow(size_t row_num) override
        {
            size_t old_size = offset_columns[0]->size();
            if (row_num + 1 < old_size)
                throw Exception("Cannot replace an element in the middle of ColumnArray", ErrorCodes::LOGICAL_ERROR);

            size_t old_data_size = data_columns[0]->size();

            try
            {
                nested_message_serializer->readRow(old_data_size);
                size_t data_size = data_columns[0]->size();
                if (data_size != old_data_size + 1)
                    throw Exception("Unexpected number of elements of ColumnArray has been read", ErrorCodes::LOGICAL_ERROR);

                if (row_num < old_size)
                {
                    for (auto & offset_column : offset_columns)
                        assert_cast<ColumnArray::ColumnOffsets &>(offset_column->assumeMutableRef()).getData().back() = data_size;
                }
                else
                {
                    for (auto & offset_column : offset_columns)
                        assert_cast<ColumnArray::ColumnOffsets &>(offset_column->assumeMutableRef()).getData().push_back(data_size);
                }
            }
            catch (...)
            {
                for (auto & data_column : data_columns)
                {
                    if (data_column->size() > old_data_size)
                        data_column->assumeMutableRef().popBack(data_column->size() - old_data_size);
                }
                for (auto & offset_column : offset_columns)
                {
                    if (offset_column->size() > old_size)
                        offset_column->assumeMutableRef().popBack(offset_column->size() - old_size);
                }
                throw;
            }
        }

        void insertDefaults(size_t row_num) override
        {
            size_t old_size = offset_columns[0]->size();
            if (row_num < old_size)
                return;

            try
            {
                size_t data_size = data_columns[0]->size();
                for (auto & offset_column : offset_columns)
                    assert_cast<ColumnArray::ColumnOffsets &>(offset_column->assumeMutableRef()).getData().push_back(data_size);
            }
            catch (...)
            {
                for (auto & offset_column : offset_columns)
                {
                    if (offset_column->size() > old_size)
                        offset_column->assumeMutableRef().popBack(offset_column->size() - old_size);
                }
                throw;
            }
        }

    private:
        const std::unique_ptr<ProtobufSerializerMessage> nested_message_serializer;
        Columns data_columns;
        Columns offset_columns;
    };


    /// Produces a tree of ProtobufSerializers which serializes a row as a protobuf message.
    class ProtobufSerializerBuilder
    {
    public:
        explicit ProtobufSerializerBuilder(const ProtobufReaderOrWriter & reader_or_writer_) : reader_or_writer(reader_or_writer_) {}

        std::unique_ptr<ProtobufSerializerMessage> buildMessageSerializer(
            const Strings & column_names,
            const DataTypes & data_types,
            std::vector<size_t> & missing_column_indices,
            const MessageDescriptor & message_descriptor,
            bool with_length_delimiter)
        {
            std::vector<size_t> used_column_indices;
            auto serializer = buildMessageSerializerImpl(
                /* num_columns = */ column_names.size(),
                column_names.data(),
                data_types.data(),
                used_column_indices,
                message_descriptor,
                with_length_delimiter,
                /* parent_field_descriptor = */ nullptr);

            if (!serializer)
            {
                throw Exception(
                    "Not found matches between the names of the columns {" + boost::algorithm::join(column_names, ", ")
                        + "} and the fields {" + boost::algorithm::join(getFieldNames(message_descriptor), ", ") + "} of the message "
                        + quoteString(message_descriptor.full_name()) + " in the protobuf schema",
                    ErrorCodes::NO_COLUMNS_SERIALIZED_TO_PROTOBUF_FIELDS);
            }

            missing_column_indices.clear();
            missing_column_indices.reserve(column_names.size() - used_column_indices.size());
            boost::range::set_difference(ext::range(column_names.size()), used_column_indices,
                                         std::back_inserter(missing_column_indices));

            return serializer;
        }

    private:
        /// Collects all field names from the message (used only to format error messages).
        static Strings getFieldNames(const MessageDescriptor & message_descriptor)
        {
            Strings field_names;
            field_names.reserve(message_descriptor.field_count());
            for (int i : ext::range(message_descriptor.field_count()))
                field_names.emplace_back(message_descriptor.field(i)->name());
            return field_names;
        }

        static bool columnNameEqualsToFieldName(const std::string_view & column_name, const FieldDescriptor & field_descriptor)
        {
            std::string_view suffix;
            return columnNameStartsWithFieldName(column_name, field_descriptor, suffix) && suffix.empty();
        }

        /// Checks if a passed column's name starts with a specified field's name.
        /// The function also assigns `suffix` to the rest part of the column's name
        /// which doesn't match to the field's name.
        /// The function requires that rest part of the column's name to be started with a dot '.' or underline '_',
        /// but doesn't include those '.' or '_' characters into `suffix`.
        static bool columnNameStartsWithFieldName(const std::string_view & column_name, const FieldDescriptor & field_descriptor, std::string_view & suffix)
        {
            size_t matching_length = 0;
            const MessageDescriptor & containing_type = *field_descriptor.containing_type();
            if (containing_type.options().map_entry())
            {
                /// Special case. Elements of the data type Map are named as "keys" and "values",
                /// but they're internally named as "key" and "value" in protobuf schema.
                if (field_descriptor.number() == 1)
                {
                    if (ColumnNameWithProtobufFieldNameComparator::startsWith(column_name, "keys"))
                        matching_length = strlen("keys");
                    else if (ColumnNameWithProtobufFieldNameComparator::startsWith(column_name, "key"))
                        matching_length = strlen("key");
                }
                else if (field_descriptor.number() == 2)
                {
                    if (ColumnNameWithProtobufFieldNameComparator::startsWith(column_name, "values"))
                        matching_length = strlen("values");
                    else if (ColumnNameWithProtobufFieldNameComparator::startsWith(column_name, "value"))
                        matching_length = strlen("value");
                }
            }
            if (!matching_length && ColumnNameWithProtobufFieldNameComparator::startsWith(column_name, field_descriptor.name()))
            {
                matching_length = field_descriptor.name().length();
            }
            if (column_name.length() == matching_length)
                return true;
            if ((column_name.length() < matching_length + 2) || !field_descriptor.message_type())
                return false;
            char first_char_after_matching = column_name[matching_length];
            if (!ColumnNameWithProtobufFieldNameComparator::equals(first_char_after_matching, '.'))
                return false;
            suffix = column_name.substr(matching_length + 1);
            return true;
        }

        /// Finds fields in the protobuf message which can be considered as matching
        /// for a specified column's name. The found fields can be nested messages,
        /// for that case suffixes are also returned.
        /// This is only the first filter, buildMessageSerializerImpl() does other checks after calling this function.
        static bool findFieldsByColumnName(
            const std::string_view & column_name,
            const MessageDescriptor & message_descriptor,
            std::vector<std::pair<const FieldDescriptor *, std::string_view /* suffix */>> & out_field_descriptors_with_suffixes)
        {
            out_field_descriptors_with_suffixes.clear();

            /// Find all fields which have the same name as column's name (case-insensitively); i.e. we're checking
            /// field_name == column_name.
            for (int i : ext::range(message_descriptor.field_count()))
            {
                const auto & field_descriptor = *message_descriptor.field(i);
                if (columnNameEqualsToFieldName(column_name, field_descriptor))
                {
                    out_field_descriptors_with_suffixes.emplace_back(&field_descriptor, std::string_view{});
                    break;
                }
            }

            if (!out_field_descriptors_with_suffixes.empty())
                return true; /// We have an exact match, no need to compare prefixes.

            /// Find all fields which name is used as prefix in column's name; i.e. we're checking
            /// column_name == field_name + '.' + nested_message_field_name
            for (int i : ext::range(message_descriptor.field_count()))
            {
                const auto & field_descriptor = *message_descriptor.field(i);
                std::string_view suffix;
                if (columnNameStartsWithFieldName(column_name, field_descriptor, suffix))
                {
                    out_field_descriptors_with_suffixes.emplace_back(&field_descriptor, suffix);
                }
            }

            /// Shorter suffixes first.
            std::sort(out_field_descriptors_with_suffixes.begin(), out_field_descriptors_with_suffixes.end(),
                      [](const std::pair<const FieldDescriptor *, std::string_view /* suffix */> & f1,
                         const std::pair<const FieldDescriptor *, std::string_view /* suffix */> & f2)
            {
                return f1.second.length() < f2.second.length();
            });

            return !out_field_descriptors_with_suffixes.empty();
        }

        /// Builds a serializer for a protobuf message (root or nested).
        template <typename StringOrStringViewT>
        std::unique_ptr<ProtobufSerializerMessage> buildMessageSerializerImpl(
            size_t num_columns,
            const StringOrStringViewT * column_names,
            const DataTypePtr * data_types,
            std::vector<size_t> & used_column_indices,
            const MessageDescriptor & message_descriptor,
            bool with_length_delimiter,
            const FieldDescriptor * parent_field_descriptor)
        {
            std::vector<ProtobufSerializerMessage::FieldDesc> field_descs;
            boost::container::flat_map<const FieldDescriptor *, std::string_view> field_descriptors_in_use;

            used_column_indices.clear();
            used_column_indices.reserve(num_columns);

            auto add_field_serializer = [&](size_t column_index_,
                                            const std::string_view & column_name_,
                                            size_t num_columns_,
                                            const FieldDescriptor & field_descriptor_,
                                            std::unique_ptr<ProtobufSerializer> field_serializer_)
            {
                auto it = field_descriptors_in_use.find(&field_descriptor_);
                if (it != field_descriptors_in_use.end())
                {
                    throw Exception(
                        "Multiple columns (" + backQuote(StringRef{field_descriptors_in_use[&field_descriptor_]}) + ", "
                            + backQuote(StringRef{column_name_}) + ") cannot be serialized to a single protobuf field "
                            + quoteString(field_descriptor_.full_name()),
                        ErrorCodes::MULTIPLE_COLUMNS_SERIALIZED_TO_SAME_PROTOBUF_FIELD);
                }

                field_descs.push_back({column_index_, num_columns_, &field_descriptor_, std::move(field_serializer_)});
                field_descriptors_in_use.emplace(&field_descriptor_, column_name_);
            };

            std::vector<std::pair<const FieldDescriptor *, std::string_view>> field_descriptors_with_suffixes;

            /// We're going through all the passed columns.
            size_t column_idx = 0;
            size_t next_column_idx = 1;
            for (; column_idx != num_columns; column_idx = next_column_idx++)
            {
                auto column_name = column_names[column_idx];
                const auto & data_type = data_types[column_idx];

                if (!findFieldsByColumnName(column_name, message_descriptor, field_descriptors_with_suffixes))
                    continue;

                if ((field_descriptors_with_suffixes.size() == 1) && field_descriptors_with_suffixes[0].second.empty())
                {
                    /// Simple case: one column is serialized as one field.
                    const auto & field_descriptor = *field_descriptors_with_suffixes[0].first;
                    auto field_serializer = buildFieldSerializer(column_name, data_type, field_descriptor, field_descriptor.is_repeated());

                    if (field_serializer)
                    {
                        add_field_serializer(column_idx, column_name, 1, field_descriptor, std::move(field_serializer));
                        used_column_indices.push_back(column_idx);
                        continue;
                    }
                }

                for (const auto & [field_descriptor, suffix] : field_descriptors_with_suffixes)
                {
                    if (!suffix.empty())
                    {
                        /// Complex case: one or more columns are serialized as a nested message.
                        std::vector<std::string_view> names_relative_to_nested_message;
                        names_relative_to_nested_message.reserve(num_columns - column_idx);
                        names_relative_to_nested_message.emplace_back(suffix);

                        for (size_t j : ext::range(column_idx + 1, num_columns))
                        {
                            std::string_view next_suffix;
                            if (!columnNameStartsWithFieldName(column_names[j], *field_descriptor, next_suffix))
                                break;
                            names_relative_to_nested_message.emplace_back(next_suffix);
                        }

                        /// Now we have up to `names_relative_to_nested_message.size()` sequential columns
                        /// which can be serialized as a nested message.

                        /// Calculate how many of those sequential columns are arrays.
                        size_t num_arrays = 0;
                        for (size_t j : ext::range(column_idx, column_idx + names_relative_to_nested_message.size()))
                        {
                            if (data_types[j]->getTypeId() != TypeIndex::Array)
                                break;
                            ++num_arrays;
                        }

                        /// We will try to serialize the sequential columns as one nested message,
                        /// then, if failed, as an array of nested messages (on condition those columns are array).
                        bool has_fallback_to_array_of_nested_messages = num_arrays && field_descriptor->is_repeated();

                        /// Try to serialize the sequential columns as one nested message.
                        try
                        {
                            std::vector<size_t> used_column_indices_in_nested;
                            auto nested_message_serializer = buildMessageSerializerImpl(
                                names_relative_to_nested_message.size(),
                                names_relative_to_nested_message.data(),
                                &data_types[column_idx],
                                used_column_indices_in_nested,
                                *field_descriptor->message_type(),
                                false,
                                field_descriptor);

                            if (nested_message_serializer)
                            {
                                for (size_t & idx_in_nested : used_column_indices_in_nested)
                                    used_column_indices.push_back(idx_in_nested + column_idx);

                                next_column_idx = used_column_indices.back() + 1;
                                add_field_serializer(column_idx, column_name, next_column_idx - column_idx, *field_descriptor, std::move(nested_message_serializer));
                                break;
                            }
                        }
                        catch (Exception & e)
                        {
                            if ((e.code() != ErrorCodes::PROTOBUF_FIELD_NOT_REPEATED) || !has_fallback_to_array_of_nested_messages)
                                throw;
                        }

                        if (has_fallback_to_array_of_nested_messages)
                        {
                            /// Try to serialize the sequential columns as an array of nested messages.
                            DataTypes array_nested_data_types;
                            array_nested_data_types.reserve(num_arrays);
                            for (size_t j : ext::range(column_idx, column_idx + num_arrays))
                                array_nested_data_types.emplace_back(assert_cast<const DataTypeArray &>(*data_types[j]).getNestedType());

                            std::vector<size_t> used_column_indices_in_nested;
                            auto nested_message_serializer = buildMessageSerializerImpl(
                                array_nested_data_types.size(),
                                names_relative_to_nested_message.data(),
                                array_nested_data_types.data(),
                                used_column_indices_in_nested,
                                *field_descriptor->message_type(),
                                false,
                                field_descriptor);

                            if (nested_message_serializer)
                            {
                                auto field_serializer = std::make_unique<ProtobufSerializerFlattenedNestedAsArrayOfNestedMessages>(std::move(nested_message_serializer));

                                for (size_t & idx_in_nested : used_column_indices_in_nested)
                                    used_column_indices.push_back(idx_in_nested + column_idx);

                                next_column_idx = used_column_indices.back() + 1;
                                add_field_serializer(column_idx, column_name, next_column_idx - column_idx, *field_descriptor, std::move(field_serializer));
                                break;
                            }
                        }
                    }
                }
            }

            /// Check that we've found matching columns for all the required fields.
            if ((message_descriptor.file()->syntax() == google::protobuf::FileDescriptor::SYNTAX_PROTO2)
                    && reader_or_writer.writer)
            {
                for (int i : ext::range(message_descriptor.field_count()))
                {
                    const auto & field_descriptor = *message_descriptor.field(i);
                    if (field_descriptor.is_required() && !field_descriptors_in_use.count(&field_descriptor))
                        throw Exception(
                            "Field " + quoteString(field_descriptor.full_name()) + " is required to be set",
                            ErrorCodes::NO_COLUMN_SERIALIZED_TO_REQUIRED_PROTOBUF_FIELD);
                }
            }

            if (field_descs.empty())
                return nullptr;

            return std::make_unique<ProtobufSerializerMessage>(
                std::move(field_descs), parent_field_descriptor, with_length_delimiter, reader_or_writer);
        }

        /// Builds a serializer for one-to-one match:
        /// one column is serialized as one field in the protobuf message.
        std::unique_ptr<ProtobufSerializer> buildFieldSerializer(
            const std::string_view & column_name,
            const DataTypePtr & data_type,
            const FieldDescriptor & field_descriptor,
            bool allow_repeat)
        {
            auto data_type_id = data_type->getTypeId();
            switch (data_type_id)
            {
                case TypeIndex::UInt8: return std::make_unique<ProtobufSerializerNumber<UInt8>>(field_descriptor, reader_or_writer);
                case TypeIndex::UInt16: return std::make_unique<ProtobufSerializerNumber<UInt16>>(field_descriptor, reader_or_writer);
                case TypeIndex::UInt32: return std::make_unique<ProtobufSerializerNumber<UInt32>>(field_descriptor, reader_or_writer);
                case TypeIndex::UInt64: return std::make_unique<ProtobufSerializerNumber<UInt64>>(field_descriptor, reader_or_writer);
                case TypeIndex::UInt128: return std::make_unique<ProtobufSerializerNumber<UInt128>>(field_descriptor, reader_or_writer);
                case TypeIndex::UInt256: return std::make_unique<ProtobufSerializerNumber<UInt256>>(field_descriptor, reader_or_writer);
                case TypeIndex::Int8: return std::make_unique<ProtobufSerializerNumber<Int8>>(field_descriptor, reader_or_writer);
                case TypeIndex::Int16: return std::make_unique<ProtobufSerializerNumber<Int16>>(field_descriptor, reader_or_writer);
                case TypeIndex::Int32: return std::make_unique<ProtobufSerializerNumber<Int32>>(field_descriptor, reader_or_writer);
                case TypeIndex::Int64: return std::make_unique<ProtobufSerializerNumber<Int64>>(field_descriptor, reader_or_writer);
                case TypeIndex::Int128: return std::make_unique<ProtobufSerializerNumber<Int128>>(field_descriptor, reader_or_writer);
                case TypeIndex::Int256: return std::make_unique<ProtobufSerializerNumber<Int256>>(field_descriptor, reader_or_writer);
                case TypeIndex::Float32: return std::make_unique<ProtobufSerializerNumber<Float32>>(field_descriptor, reader_or_writer);
                case TypeIndex::Float64: return std::make_unique<ProtobufSerializerNumber<Float64>>(field_descriptor, reader_or_writer);
                case TypeIndex::Date: return std::make_unique<ProtobufSerializerDate>(field_descriptor, reader_or_writer);
                case TypeIndex::DateTime: return std::make_unique<ProtobufSerializerDateTime>(field_descriptor, reader_or_writer);
                case TypeIndex::DateTime64: return std::make_unique<ProtobufSerializerDateTime64>(assert_cast<const DataTypeDateTime64 &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::String: return std::make_unique<ProtobufSerializerString<false>>(field_descriptor, reader_or_writer);
                case TypeIndex::FixedString: return std::make_unique<ProtobufSerializerString<true>>(typeid_cast<std::shared_ptr<const DataTypeFixedString>>(data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Enum8: return std::make_unique<ProtobufSerializerEnum<Int8>>(typeid_cast<std::shared_ptr<const DataTypeEnum8>>(data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Enum16: return std::make_unique<ProtobufSerializerEnum<Int16>>(typeid_cast<std::shared_ptr<const DataTypeEnum16>>(data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Decimal32: return std::make_unique<ProtobufSerializerDecimal<Decimal32>>(assert_cast<const DataTypeDecimal<Decimal32> &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Decimal64: return std::make_unique<ProtobufSerializerDecimal<Decimal64>>(assert_cast<const DataTypeDecimal<Decimal64> &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Decimal128: return std::make_unique<ProtobufSerializerDecimal<Decimal128>>(assert_cast<const DataTypeDecimal<Decimal128> &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Decimal256: return std::make_unique<ProtobufSerializerDecimal<Decimal256>>(assert_cast<const DataTypeDecimal<Decimal256> &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::UUID: return std::make_unique<ProtobufSerializerUUID>(field_descriptor, reader_or_writer);
                case TypeIndex::Interval: return std::make_unique<ProtobufSerializerInterval>(field_descriptor, reader_or_writer);
                case TypeIndex::AggregateFunction: return std::make_unique<ProtobufSerializerAggregateFunction>(typeid_cast<std::shared_ptr<const DataTypeAggregateFunction>>(data_type), field_descriptor, reader_or_writer);

                case TypeIndex::Nullable:
                {
                    const auto & nullable_data_type = assert_cast<const DataTypeNullable &>(*data_type);
                    auto nested_serializer = buildFieldSerializer(column_name, nullable_data_type.getNestedType(), field_descriptor, allow_repeat);
                    if (!nested_serializer)
                        return nullptr;
                    return std::make_unique<ProtobufSerializerNullable>(std::move(nested_serializer));
                }

                case TypeIndex::LowCardinality:
                {
                    const auto & low_cardinality_data_type = assert_cast<const DataTypeLowCardinality &>(*data_type);
                    auto nested_serializer
                        = buildFieldSerializer(column_name, low_cardinality_data_type.getDictionaryType(), field_descriptor, allow_repeat);
                    if (!nested_serializer)
                        return nullptr;
                    return std::make_unique<ProtobufSerializerLowCardinality>(std::move(nested_serializer));
                }

                case TypeIndex::Map:
                {
                    const auto & map_data_type = assert_cast<const DataTypeMap &>(*data_type);
                    auto nested_serializer = buildFieldSerializer(column_name, map_data_type.getNestedType(), field_descriptor, allow_repeat);
                    if (!nested_serializer)
                        return nullptr;
                    return std::make_unique<ProtobufSerializerMap>(std::move(nested_serializer));
                }

                case TypeIndex::Array:
                {
                    /// Array is serialized as a repeated field.
                    const auto & array_data_type = assert_cast<const DataTypeArray &>(*data_type);

                    if (!allow_repeat)
                        throwFieldNotRepeated(field_descriptor, column_name);

                    auto nested_serializer = buildFieldSerializer(column_name, array_data_type.getNestedType(), field_descriptor,
                                                                  /* allow_repeat = */ false); // We do our repeating now, so for nested type we forget about the repeating.
                    if (!nested_serializer)
                        return nullptr;
                    return std::make_unique<ProtobufSerializerArray>(std::move(nested_serializer));
                }

                case TypeIndex::Tuple:
                {
                    /// Tuple is serialized in one of two ways:
                    /// 1) If the tuple has explicit names then it can be serialized as a nested message.
                    /// 2) Any tuple can be serialized as a repeated field, just like Array.
                    const auto & tuple_data_type = assert_cast<const DataTypeTuple &>(*data_type);
                    size_t size_of_tuple = tuple_data_type.getElements().size();

                    if (tuple_data_type.haveExplicitNames() && field_descriptor.message_type())
                    {
                        /// Try to serialize as a nested message.
                        std::vector<size_t> used_column_indices;
                        auto nested_message_serializer = buildMessageSerializerImpl(
                            size_of_tuple,
                            tuple_data_type.getElementNames().data(),
                            tuple_data_type.getElements().data(),
                            used_column_indices,
                            *field_descriptor.message_type(),
                            false,
                            &field_descriptor);

                        if (!nested_message_serializer)
                        {
                            throw Exception(
                                "Not found matches between the names of the tuple's elements {"
                                    + boost::algorithm::join(tuple_data_type.getElementNames(), ", ") + "} and the fields {"
                                    + boost::algorithm::join(getFieldNames(*field_descriptor.message_type()), ", ") + "} of the message "
                                    + quoteString(field_descriptor.message_type()->full_name()) + " in the protobuf schema",
                                ErrorCodes::NO_COLUMNS_SERIALIZED_TO_PROTOBUF_FIELDS);
                        }

                        return std::make_unique<ProtobufSerializerTupleAsNestedMessage>(std::move(nested_message_serializer));
                    }

                    /// Serialize as a repeated field.
                    if (!allow_repeat && (size_of_tuple > 1))
                        throwFieldNotRepeated(field_descriptor, column_name);

                    std::vector<std::unique_ptr<ProtobufSerializer>> nested_serializers;
                    for (const auto & nested_data_type : tuple_data_type.getElements())
                    {
                        auto nested_serializer = buildFieldSerializer(column_name, nested_data_type, field_descriptor,
                                                                      /* allow_repeat = */ false); // We do our repeating now, so for nested type we forget about the repeating.
                        if (!nested_serializer)
                            break;
                        nested_serializers.push_back(std::move(nested_serializer));
                    }

                    if (nested_serializers.size() != size_of_tuple)
                        return nullptr;

                    return std::make_unique<ProtobufSerializerTupleAsArray>(
                        typeid_cast<std::shared_ptr<const DataTypeTuple>>(data_type),
                        field_descriptor,
                        std::move(nested_serializers));
                }

                default:
                    throw Exception("Unknown data type: " + data_type->getName(), ErrorCodes::LOGICAL_ERROR);
            }
        }

        [[noreturn]] static void throwFieldNotRepeated(const FieldDescriptor & field_descriptor, const std::string_view & column_name)
        {
            if (!field_descriptor.is_repeated())
                throw Exception(
                    "The field " + quoteString(field_descriptor.full_name())
                        + " must be repeated in the protobuf schema to match the column " + backQuote(StringRef{column_name}),
                    ErrorCodes::PROTOBUF_FIELD_NOT_REPEATED);

            throw Exception(
                "The field " + quoteString(field_descriptor.full_name())
                    + " is repeated but the level of repeatedness is not enough to serialize a multidimensional array from the column "
                    + backQuote(StringRef{column_name}) + ". It's recommended to make the parent field repeated as well.",
                ErrorCodes::PROTOBUF_FIELD_NOT_REPEATED);
        }

        const ProtobufReaderOrWriter reader_or_writer;
    };
}


std::unique_ptr<ProtobufSerializer> ProtobufSerializer::create(
    const Strings & column_names,
    const DataTypes & data_types,
    std::vector<size_t> & missing_column_indices,
    const google::protobuf::Descriptor & message_descriptor,
    bool with_length_delimiter,
    ProtobufReader & reader)
{
    return ProtobufSerializerBuilder(reader).buildMessageSerializer(column_names, data_types, missing_column_indices, message_descriptor, with_length_delimiter);
}

std::unique_ptr<ProtobufSerializer> ProtobufSerializer::create(
    const Strings & column_names,
    const DataTypes & data_types,
    const google::protobuf::Descriptor & message_descriptor,
    bool with_length_delimiter,
    ProtobufWriter & writer)
{
    std::vector<size_t> missing_column_indices;
    return ProtobufSerializerBuilder(writer).buildMessageSerializer(column_names, data_types, missing_column_indices, message_descriptor, with_length_delimiter);
}
}
#endif
