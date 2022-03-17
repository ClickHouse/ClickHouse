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
#   include <DataTypes/DataTypeString.h>
#   include <DataTypes/Serializations/SerializationDecimal.h>
#   include <DataTypes/Serializations/SerializationFixedString.h>
#   include <Formats/ProtobufReader.h>
#   include <Formats/ProtobufWriter.h>
#   include <Formats/RowInputMissingColumnsFiller.h>
#   include <IO/Operators.h>
#   include <IO/ReadBufferFromString.h>
#   include <IO/ReadHelpers.h>
#   include <IO/WriteBufferFromString.h>
#   include <IO/WriteHelpers.h>
#   include <base/range.h>
#   include <base/sort.h>
#   include <google/protobuf/descriptor.h>
#   include <google/protobuf/descriptor.pb.h>
#   include <boost/algorithm/string.hpp>
#   include <boost/container/flat_map.hpp>
#   include <boost/container/flat_set.hpp>
#   include <boost/numeric/conversion/cast.hpp>
#   include <boost/range/algorithm.hpp>
#   include <boost/range/algorithm_ext/erase.hpp>
#   include <base/logger_useful.h>

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
    extern const int BAD_ARGUMENTS;
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


    WriteBuffer & writeIndent(WriteBuffer & out, size_t size) { return out << String(size * 4, ' '); }


    [[noreturn]] void wrongNumberOfColumns(size_t number_of_columns, const String & expected)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of columns: expected {}, specified {}", expected, number_of_columns);
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
        ProtobufSerializerSingleValue(
            const std::string_view & column_name_,
            const FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : column_name(column_name_)
            , field_descriptor(field_descriptor_)
            , field_typeid(field_descriptor_.type())
            , field_tag(field_descriptor.number())
            , reader(reader_or_writer_.reader)
            , writer(reader_or_writer_.writer)
            , skip_zero_or_empty(shouldSkipZeroOrEmpty(field_descriptor))
        {
        }

        void setColumns(const ColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            column = columns[0];
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
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

        [[noreturn]] void incompatibleColumnType(const std::string_view & column_type) const
        {
            throw Exception(
                ErrorCodes::DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD,
                "The column {} ({}) cannot be serialized to the field {} ({}) due to their types are not compatible",
                quoteString(column_name),
                column_type,
                quoteString(field_descriptor.full_name()),
                field_descriptor.type_name());
        }

        [[noreturn]] void cannotConvertValue(const std::string_view & src_value, const std::string_view & src_type_name, const std::string_view & dest_type_name) const
        {
            throw Exception(
                "Could not convert value '" + String{src_value} + "' from type " + String{src_type_name} + " to type "
                    + String{dest_type_name} + " while " + (reader ? "reading" : "writing") + " field "
                    + quoteString(field_descriptor.name()) + " " + (reader ? "for inserting into" : "extracted from") + " column "
                    + quoteString(column_name),
                ErrorCodes::PROTOBUF_BAD_CAST);
        }

        const String column_name;
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

        ProtobufSerializerNumber(const std::string_view & column_name_, const FieldDescriptor & field_descriptor_, const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(column_name_, field_descriptor_, reader_or_writer_)
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

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerNumber<" << TypeName<NumberType> << ">: column " << quoteString(column_name)
                                     << " -> field " << quoteString(field_descriptor.full_name()) << " (" << field_descriptor.type_name()
                                     << ")\n";
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
                        incompatibleColumnType(TypeName<NumberType>);

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
                    incompatibleColumnType(TypeName<NumberType>);
            }
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
            const std::string_view & column_name_,
            const std::shared_ptr<const DataTypeFixedString> & fixed_string_data_type_,
            const google::protobuf::FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(column_name_, field_descriptor_, reader_or_writer_)
            , fixed_string_data_type(fixed_string_data_type_)
            , n(fixed_string_data_type->getN())
        {
            static_assert(is_fixed_string, "This constructor for FixedString only");
            setFunctions();
            prepareEnumMapping();
        }

        ProtobufSerializerString(
            const std::string_view & column_name_,
            const google::protobuf::FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(column_name_, field_descriptor_, reader_or_writer_)
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

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerString<" << (is_fixed_string ? "fixed" : "") << ">: column "
                                     << quoteString(column_name) << " -> field " << quoteString(field_descriptor.full_name()) << " ("
                                     << field_descriptor.type_name() << ")\n";
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
                    this->incompatibleColumnType(is_fixed_string ? "FixedString" : "String");
            }
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
            WriteBufferFromVector buf{str, AppendModeTag{}};
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
            const std::string_view & column_name_,
            const std::shared_ptr<const EnumDataType> & enum_data_type_,
            const FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : BaseClass(column_name_, field_descriptor_, reader_or_writer_), enum_data_type(enum_data_type_)
        {
            assert(enum_data_type);
            setFunctions();
            prepareEnumMapping();
        }

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerEnum<" << TypeName<NumberType> << ">: column " << quoteString(this->column_name)
                                     << " -> field " << quoteString(this->field_descriptor.full_name()) << " ("
                                     << this->field_descriptor.type_name() << ")\n";
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
                    this->incompatibleColumnType(enum_data_type->getName());
            }
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
            for (int i : collections::range(enum_descriptor.value_count()))
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
            const std::string_view & column_name_,
            const DataTypeDecimalBase<DecimalType> & decimal_data_type_,
            const FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(column_name_, field_descriptor_, reader_or_writer_)
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

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerDecimal<" << TypeName<DecimalType> << ">: column " << quoteString(column_name)
                                     << " -> field " << quoteString(field_descriptor.full_name()) << " (" << field_descriptor.type_name()
                                     << ")\n";
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
                        incompatibleColumnType(TypeName<DecimalType>);
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
                                writeText(decimal, scale, buf, false);
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
                    incompatibleColumnType(TypeName<DecimalType>);
            }
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
                writeText(decimal, scale, buf, false);
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
            const std::string_view & column_name_,
            const FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerNumber<UInt16>(column_name_, field_descriptor_, reader_or_writer_)
        {
            setFunctions();
        }

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerDate: column " << quoteString(column_name) << " -> field "
                                     << quoteString(field_descriptor.full_name()) << " (" << field_descriptor.type_name() << ")\n";
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
                    incompatibleColumnType("Date");
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
    };


    /// Serializes a ColumnVector<UInt32> containing datetimes to a field of any type except TYPE_MESSAGE, TYPE_GROUP, TYPE_BOOL, TYPE_ENUM.
    class ProtobufSerializerDateTime : public ProtobufSerializerNumber<UInt32>
    {
    public:
        ProtobufSerializerDateTime(
            const std::string_view & column_name_,
            const DataTypeDateTime & type,
            const FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerNumber<UInt32>(column_name_, field_descriptor_, reader_or_writer_),
            date_lut(type.getTimeZone())
        {
            setFunctions();
        }

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerDateTime: column " << quoteString(column_name) << " -> field "
                                     << quoteString(field_descriptor.full_name()) << " (" << field_descriptor.type_name() << ")\n";
        }

    protected:
        const DateLUTImpl & date_lut;

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
                        dateTimeToString(value, text_buffer, date_lut);
                        writeStr(text_buffer);
                    };

                    read_function = [this]() -> UInt32
                    {
                        readStr(text_buffer);
                        return stringToDateTime(text_buffer, date_lut);
                    };

                    default_function = [this]() -> UInt32 { return stringToDateTime(field_descriptor.default_value_string(), date_lut); };
                    break;
                }

                default:
                    incompatibleColumnType("DateTime");
            }
        }

        static void dateTimeToString(time_t tm, String & str, const DateLUTImpl & lut)
        {
            WriteBufferFromString buf{str};
            writeDateTimeText(tm, buf, lut);
        }

        static time_t stringToDateTime(const String & str, const DateLUTImpl & lut)
        {
            ReadBufferFromString buf{str};
            time_t tm = 0;
            readDateTimeText(tm, buf, lut);
            if (tm < 0)
                tm = 0;
            return tm;
        }
    };


    /// Serializes a ColumnVector<UUID> containing UUIDs to a field of type TYPE_STRING or TYPE_BYTES.
    class ProtobufSerializerUUID : public ProtobufSerializerSingleValue
    {
    public:
        ProtobufSerializerUUID(
            const std::string_view & column_name_,
            const google::protobuf::FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(column_name_, field_descriptor_, reader_or_writer_)
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

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerUUID: column " << quoteString(column_name) << " -> field "
                                     << quoteString(field_descriptor.full_name()) << " (" << field_descriptor.type_name() << ")\n";
        }

    private:
        void setFunctions()
        {
            if ((field_typeid != FieldTypeId::TYPE_STRING) && (field_typeid != FieldTypeId::TYPE_BYTES))
                incompatibleColumnType("UUID");

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
            const std::string_view & column_name_,
            const std::shared_ptr<const DataTypeAggregateFunction> & aggregate_function_data_type_,
            const google::protobuf::FieldDescriptor & field_descriptor_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : ProtobufSerializerSingleValue(column_name_, field_descriptor_, reader_or_writer_)
            , aggregate_function_data_type(aggregate_function_data_type_)
            , aggregate_function(aggregate_function_data_type->getFunction())
        {
            if ((field_typeid != FieldTypeId::TYPE_STRING) && (field_typeid != FieldTypeId::TYPE_BYTES))
                incompatibleColumnType(aggregate_function_data_type->getName());
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

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerAggregateFunction: column " << quoteString(column_name) << " -> field "
                                     << quoteString(field_descriptor.full_name()) << " (" << field_descriptor.type_name() << ")\n";
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
                aggregate_function->deserialize(data, buf, std::nullopt, &arena);
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
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            column = columns[0];
            const auto & column_nullable = assert_cast<const ColumnNullable &>(*column);
            ColumnPtr nested_column = column_nullable.getNestedColumnPtr();
            nested_serializer->setColumns(&nested_column, 1);
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
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

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerNullable ->\n";
            nested_serializer->describeTree(out, indent + 1);
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
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            const auto & column_map = assert_cast<const ColumnMap &>(*columns[0]);
            ColumnPtr nested_column = column_map.getNestedColumnPtr();
            nested_serializer->setColumns(&nested_column, 1);
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            ColumnPtr column0 = columns[0]->getPtr();
            setColumns(&column0, 1);
        }

        void writeRow(size_t row_num) override { nested_serializer->writeRow(row_num); }
        void readRow(size_t row_num) override { nested_serializer->readRow(row_num); }
        void insertDefaults(size_t row_num) override { nested_serializer->insertDefaults(row_num); }

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerMap ->\n";
            nested_serializer->describeTree(out, indent + 1);
        }

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
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            column = columns[0];
            const auto & column_lc = assert_cast<const ColumnLowCardinality &>(*column);
            ColumnPtr nested_column = column_lc.getDictionary().getNestedColumn();
            nested_serializer->setColumns(&nested_column, 1);
            read_value_column_set = false;
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
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

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerLowCardinality ->\n";
            nested_serializer->describeTree(out, indent + 1);
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
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            column = columns[0];
            const auto & column_array = assert_cast<const ColumnArray &>(*column);
            ColumnPtr data_column = column_array.getDataPtr();
            element_serializer->setColumns(&data_column, 1);
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            ColumnPtr column0 = columns[0]->getPtr();
            setColumns(&column0, 1);
        }

        void writeRow(size_t row_num) override
        {
            const auto & column_array = assert_cast<const ColumnArray &>(*column);
            const auto & offsets = column_array.getOffsets();
            for (size_t i : collections::range(offsets[row_num - 1], offsets[row_num]))
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

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerArray ->\n";
            element_serializer->describeTree(out, indent + 1);
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
            const std::string_view & column_name_,
            const std::shared_ptr<const DataTypeTuple> & tuple_data_type_,
            const FieldDescriptor & field_descriptor_,
            std::vector<std::unique_ptr<ProtobufSerializer>> element_serializers_)
            : column_name(column_name_)
            , tuple_data_type(tuple_data_type_)
            , tuple_size(tuple_data_type->getElements().size())
            , field_descriptor(field_descriptor_)
            , element_serializers(std::move(element_serializers_))
        {
            assert(tuple_size);
            assert(tuple_size == element_serializers.size());
        }

        void setColumns(const ColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            column = columns[0];
            const auto & column_tuple = assert_cast<const ColumnTuple &>(*column);
            for (size_t i : collections::range(tuple_size))
            {
                auto element_column = column_tuple.getColumnPtr(i);
                element_serializers[i]->setColumns(&element_column, 1);
            }
            current_element_index = 0;
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            ColumnPtr column0 = columns[0]->getPtr();
            setColumns(&column0, 1);
        }

        void writeRow(size_t row_num) override
        {
            for (size_t i : collections::range(tuple_size))
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
                    ErrorCodes::PROTOBUF_BAD_CAST,
                    "Column {}: More than {} elements was read from the field {} to fit in the data type {}",
                    quoteString(column_name),
                    tuple_size,
                    quoteString(field_descriptor.full_name()),
                    tuple_data_type->getName());
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
                for (size_t i : collections::range(tuple_size))
                    element_serializers[i]->insertDefaults(row_num);
            }
            catch (...)
            {
                for (size_t i : collections::range(tuple_size))
                {
                    auto element_column = column_tuple.getColumnPtr(i)->assumeMutable();
                    if (element_column->size() > old_size)
                        element_column->popBack(element_column->size() - old_size);
                }
                throw;
            }
        }

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerTupleAsArray: column " << quoteString(column_name) << " ("
                                     << tuple_data_type->getName() << ") -> field " << quoteString(field_descriptor.full_name()) << " ("
                                     << field_descriptor.type_name() << ") ->\n";
            for (const auto & element_serializer : element_serializers)
                element_serializer->describeTree(out, indent + 1);
        }

    private:
        const String column_name;
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
            std::vector<size_t> column_indices;
            const FieldDescriptor * field_descriptor;
            std::unique_ptr<ProtobufSerializer> field_serializer;
        };

        ProtobufSerializerMessage(
            std::vector<FieldDesc> && field_descs_,
            const FieldDescriptor * parent_field_descriptor_,
            bool with_length_delimiter_,
            std::unique_ptr<RowInputMissingColumnsFiller> missing_columns_filler_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : parent_field_descriptor(parent_field_descriptor_)
            , with_length_delimiter(with_length_delimiter_)
            , missing_columns_filler(std::move(missing_columns_filler_))
            , should_skip_if_empty(parent_field_descriptor ? shouldSkipZeroOrEmpty(*parent_field_descriptor) : false)
            , reader(reader_or_writer_.reader)
            , writer(reader_or_writer_.writer)
        {
            field_infos.reserve(field_descs_.size());
            for (auto & desc : field_descs_)
                field_infos.emplace_back(std::move(desc.column_indices), *desc.field_descriptor, std::move(desc.field_serializer));

            ::sort(field_infos.begin(), field_infos.end(),
                      [](const FieldInfo & lhs, const FieldInfo & rhs) { return lhs.field_tag < rhs.field_tag; });

            for (size_t i : collections::range(field_infos.size()))
                field_index_by_field_tag.emplace(field_infos[i].field_tag, i);
        }

        void setHasEnvelopeAsParent()
        {
            has_envelope_as_parent = true;
        }

        void setColumns(const ColumnPtr * columns_, size_t num_columns_) override
        {
            if (!num_columns_)
                wrongNumberOfColumns(num_columns_, ">0");

            std::vector<ColumnPtr> field_columns;
            for (const FieldInfo & info : field_infos)
            {
                field_columns.clear();
                field_columns.reserve(info.column_indices.size());
                for (size_t column_index : info.column_indices)
                {
                    if (column_index >= num_columns_)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong column index {}, expected column indices <{}", column_index, num_columns_);
                    field_columns.emplace_back(columns_[column_index]);
                }
                info.field_serializer->setColumns(field_columns.data(), field_columns.size());
            }

            if (reader)
            {
                mutable_columns.resize(num_columns_);
                for (size_t i : collections::range(num_columns_))
                    mutable_columns[i] = columns_[i]->assumeMutable();

                std::vector<UInt8> column_is_missing;
                column_is_missing.resize(num_columns_, true);
                for (const FieldInfo & info : field_infos)
                    for (size_t i : info.column_indices)
                        column_is_missing[i] = false;

                has_missing_columns = (std::find(column_is_missing.begin(), column_is_missing.end(), true) != column_is_missing.end());
            }
        }

        void setColumns(const MutableColumnPtr * columns_, size_t num_columns_) override
        {
            Columns cols;
            cols.reserve(num_columns_);
            for (size_t i : collections::range(num_columns_))
                cols.push_back(columns_[i]->getPtr());
            setColumns(cols.data(), cols.size());
        }

        void writeRow(size_t row_num) override
        {
            if (parent_field_descriptor || has_envelope_as_parent)
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
            else if (has_envelope_as_parent)
            {
                writer->endNestedMessage(1, false, should_skip_if_empty);
            }
            else
                writer->endMessage(with_length_delimiter);
        }

        void readRow(size_t row_num) override
        {
            if (parent_field_descriptor || has_envelope_as_parent)
                reader->startNestedMessage();
            else
                reader->startMessage(with_length_delimiter);

            if (!field_infos.empty())
            {
                last_field_index = 0;
                last_field_tag = field_infos[0].field_tag;
                size_t old_size = mutable_columns.empty() ? 0 : mutable_columns[0]->size();

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
                    for (auto & column : mutable_columns)
                    {
                        if (column->size() > old_size)
                            column->popBack(column->size() - old_size);
                    }
                    throw;
                }
            }

            if (parent_field_descriptor || has_envelope_as_parent)
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

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            size_t num_columns = 0;
            for (const auto & field_info : field_infos)
                num_columns += field_info.column_indices.size();

            writeIndent(out, indent) << "ProtobufSerializerMessage: " << num_columns << " columns ->";
            if (parent_field_descriptor)
                out << " field " << quoteString(parent_field_descriptor->full_name()) << " (" << parent_field_descriptor->type_name() << ")";

            for (const auto & field_info : field_infos)
            {
                out << "\n";
                writeIndent(out, indent + 1) << "Columns #";
                for (size_t j = 0; j != field_info.column_indices.size(); ++j)
                {
                    if (j)
                        out << ", ";
                    out << field_info.column_indices[j];
                }
                out << " ->\n";
                field_info.field_serializer->describeTree(out, indent + 2);
            }
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
            if (has_missing_columns)
                missing_columns_filler->addDefaults(mutable_columns, row_num);
        }

        struct FieldInfo
        {
            FieldInfo(
                std::vector<size_t> && column_indices_,
                const FieldDescriptor & field_descriptor_,
                std::unique_ptr<ProtobufSerializer> field_serializer_)
                : column_indices(std::move(column_indices_))
                , field_descriptor(&field_descriptor_)
                , field_tag(field_descriptor_.number())
                , should_pack_repeated(shouldPackRepeated(field_descriptor_))
                , field_serializer(std::move(field_serializer_))
            {
            }
            std::vector<size_t> column_indices;
            const FieldDescriptor * field_descriptor;
            int field_tag;
            bool should_pack_repeated;
            std::unique_ptr<ProtobufSerializer> field_serializer;
            bool field_read = false;
        };

        const FieldDescriptor * const parent_field_descriptor;
        bool has_envelope_as_parent = false;
        const bool with_length_delimiter;
        const std::unique_ptr<RowInputMissingColumnsFiller> missing_columns_filler;
        const bool should_skip_if_empty;
        ProtobufReader * const reader;
        ProtobufWriter * const writer;
        std::vector<FieldInfo> field_infos;
        std::unordered_map<int, size_t> field_index_by_field_tag;
        MutableColumns mutable_columns;
        bool has_missing_columns = false;
        int last_field_tag = 0;
        size_t last_field_index = static_cast<size_t>(-1);
    };

    /// Serializes a top-level envelope message in the protobuf schema.
    /// "Envelope" means that the contained subtree of serializers is enclosed in a message just once,
    /// i.e. only when the first and the last row read/write trigger a read/write of the msg header.
    class ProtobufSerializerEnvelope : public ProtobufSerializer
    {
    public:
        ProtobufSerializerEnvelope(
            std::unique_ptr<ProtobufSerializerMessage>&& serializer_,
            const ProtobufReaderOrWriter & reader_or_writer_)
            : serializer(std::move(serializer_))
            , reader(reader_or_writer_.reader)
            , writer(reader_or_writer_.writer)
        {
            // The inner serializer has a backreference of type protobuf::FieldDescriptor * to it's parent
            // serializer. If it is unset, it considers itself the top-level message, otherwise a nested
            // message and accordingly it makes start/endMessage() vs. startEndNestedMessage() calls into
            // Protobuf(Writer|Reader). There is no field descriptor because Envelopes merely forward calls
            // but don't contain data to be serialized. We must still force the inner serializer to act
            // as nested message.
            serializer->setHasEnvelopeAsParent();
        }

        void setColumns(const ColumnPtr * columns_, size_t num_columns_) override
        {
            serializer->setColumns(columns_, num_columns_);
        }

        void setColumns(const MutableColumnPtr * columns_, size_t num_columns_) override
        {
            serializer->setColumns(columns_, num_columns_);
        }

        void writeRow(size_t row_num) override
        {
            if (first_call_of_write_row)
            {
                writer->startMessage();
                first_call_of_write_row = false;
            }

            serializer->writeRow(row_num);
        }

        void finalizeWrite() override
        {
            writer->endMessage(/*with_length_delimiter = */ true);
        }

        void readRow(size_t row_num) override
        {
            if (first_call_of_read_row)
            {
                reader->startMessage(/*with_length_delimiter = */ true);
                first_call_of_read_row = false;
            }

            int field_tag;
            [[maybe_unused]] bool ret = reader->readFieldNumber(field_tag);
            assert(ret);

            serializer->readRow(row_num);
        }

        void insertDefaults(size_t row_num) override
        {
            serializer->insertDefaults(row_num);
        }

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerEnvelope ->\n";
            serializer->describeTree(out, indent + 1);
        }

        std::unique_ptr<ProtobufSerializerMessage> serializer;
        ProtobufReader * const reader;
        ProtobufWriter * const writer;
        bool first_call_of_write_row = true;
        bool first_call_of_read_row = true;
    };

    /// Serializes a tuple with explicit names as a nested message.
    class ProtobufSerializerTupleAsNestedMessage : public ProtobufSerializer
    {
    public:
        explicit ProtobufSerializerTupleAsNestedMessage(std::unique_ptr<ProtobufSerializerMessage> message_serializer_)
            : message_serializer(std::move(message_serializer_))
        {
        }

        void setColumns(const ColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            const auto & column_tuple = assert_cast<const ColumnTuple &>(*columns[0]);
            size_t tuple_size = column_tuple.tupleSize();
            assert(tuple_size);
            Columns element_columns;
            element_columns.reserve(tuple_size);
            for (size_t i : collections::range(tuple_size))
                element_columns.emplace_back(column_tuple.getColumnPtr(i));
            message_serializer->setColumns(element_columns.data(), element_columns.size());
        }

        void setColumns(const MutableColumnPtr * columns, [[maybe_unused]] size_t num_columns) override
        {
            if (num_columns != 1)
                wrongNumberOfColumns(num_columns, "1");
            ColumnPtr column0 = columns[0]->getPtr();
            setColumns(&column0, 1);
        }

        void writeRow(size_t row_num) override { message_serializer->writeRow(row_num); }
        void readRow(size_t row_num) override { message_serializer->readRow(row_num); }
        void insertDefaults(size_t row_num) override { message_serializer->insertDefaults(row_num); }

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerTupleAsNestedMessage ->\n";
            message_serializer->describeTree(out, indent + 1);
        }

    private:
        const std::unique_ptr<ProtobufSerializerMessage> message_serializer;
    };


    /// Serializes a flattened Nested data type (an array of tuples with explicit names)
    /// as a repeated nested message.
    class ProtobufSerializerFlattenedNestedAsArrayOfNestedMessages : public ProtobufSerializer
    {
    public:
        explicit ProtobufSerializerFlattenedNestedAsArrayOfNestedMessages(
            const std::vector<std::string_view> & column_names_,
            const FieldDescriptor * parent_field_descriptor_,
            std::unique_ptr<ProtobufSerializerMessage> message_serializer_,
            const std::function<String(size_t)> & get_root_desc_function_)
            : parent_field_descriptor(parent_field_descriptor_)
            , message_serializer(std::move(message_serializer_))
            , get_root_desc_function(get_root_desc_function_)
        {
            column_names.reserve(column_names_.size());
            for (const auto & column_name : column_names_)
                column_names.emplace_back(column_name);
        }

        void setColumns(const ColumnPtr * columns, size_t num_columns) override
        {
            if (!num_columns)
                wrongNumberOfColumns(num_columns, ">0");
            data_columns.clear();
            data_columns.reserve(num_columns);
            offset_columns.clear();
            offset_columns.reserve(num_columns);

            for (size_t i : collections::range(num_columns))
            {
                const auto & column_array = assert_cast<const ColumnArray &>(*columns[i]);
                data_columns.emplace_back(column_array.getDataPtr());

                auto offset_column = column_array.getOffsetsPtr();
                if (std::binary_search(offset_columns.begin(), offset_columns.end(), offset_column))
                    continue;

                /// Keep `offset_columns` sorted.
                offset_columns.insert(std::upper_bound(offset_columns.begin(), offset_columns.end(), offset_column), offset_column);

                /// All the columns listed in `offset_columns` should have equal offsets.
                if (i >= 1)
                {
                    const auto & column_array0 = assert_cast<const ColumnArray &>(*columns[0]);
                    if (!column_array0.hasEqualOffsets(column_array))
                    {
                        throw Exception(ErrorCodes::PROTOBUF_BAD_CAST,
                                        "Column #{} {} and column #{} {} are supposed to have equal offsets according to the following serialization tree:\n{}",
                                        0, quoteString(column_names[0]), i, quoteString(column_names[i]), get_root_desc_function(0));
                    }
                }
            }

            message_serializer->setColumns(data_columns.data(), data_columns.size());
        }

        void setColumns(const MutableColumnPtr * columns, size_t num_columns) override
        {
            Columns cols;
            cols.reserve(num_columns);
            for (size_t i : collections::range(num_columns))
                cols.push_back(columns[i]->getPtr());
            setColumns(cols.data(), cols.size());
        }

        void writeRow(size_t row_num) override
        {
            const auto & offset_column0 = assert_cast<const ColumnArray::ColumnOffsets &>(*offset_columns[0]);
            size_t start_offset = offset_column0.getElement(row_num - 1);
            size_t end_offset = offset_column0.getElement(row_num);
            for (size_t i : collections::range(start_offset, end_offset))
                message_serializer->writeRow(i);
        }

        void readRow(size_t row_num) override
        {
            size_t old_size = offset_columns[0]->size();
            if (row_num + 1 < old_size)
                throw Exception("Cannot replace an element in the middle of ColumnArray", ErrorCodes::LOGICAL_ERROR);

            size_t old_data_size = data_columns[0]->size();

            try
            {
                message_serializer->readRow(old_data_size);
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

        void describeTree(WriteBuffer & out, size_t indent) const override
        {
            writeIndent(out, indent) << "ProtobufSerializerFlattenedNestedAsArrayOfNestedMessages: columns ";
            for (size_t i = 0; i != column_names.size(); ++i)
            {
                if (i)
                    out << ", ";
                out << "#" << i << " " << quoteString(column_names[i]);
            }
            out << " ->";
            if (parent_field_descriptor)
                out << " field " << quoteString(parent_field_descriptor->full_name()) << " (" << parent_field_descriptor->type_name() << ") ->\n";
            message_serializer->describeTree(out, indent + 1);
        }

    private:
        Strings column_names;
        const FieldDescriptor * parent_field_descriptor;
        const std::unique_ptr<ProtobufSerializerMessage> message_serializer;
        const std::function<String(size_t)> get_root_desc_function;
        Columns data_columns;
        Columns offset_columns;
    };


    /// Produces a tree of ProtobufSerializers which serializes a row as a protobuf message.
    class ProtobufSerializerBuilder
    {
    public:
        explicit ProtobufSerializerBuilder(const ProtobufReaderOrWriter & reader_or_writer_) : reader_or_writer(reader_or_writer_) {}

        std::unique_ptr<ProtobufSerializer> buildMessageSerializer(
            const Strings & column_names,
            const DataTypes & data_types,
            std::vector<size_t> & missing_column_indices,
            const MessageDescriptor & message_descriptor,
            bool with_length_delimiter,
            bool with_envelope)
        {
            root_serializer_ptr = std::make_shared<ProtobufSerializer *>();
            get_root_desc_function = [root_serializer_ptr = root_serializer_ptr](size_t indent) -> String
            {
                WriteBufferFromOwnString buf;
                (*root_serializer_ptr)->describeTree(buf, indent);
                return buf.str();
            };

            std::vector<size_t> used_column_indices;
            auto message_serializer = buildMessageSerializerImpl(
                /* num_columns = */ column_names.size(),
                column_names.data(),
                data_types.data(),
                message_descriptor,
                with_length_delimiter,
                /* parent_field_descriptor = */ nullptr,
                used_column_indices,
                /* columns_are_reordered_outside = */ false,
                /* check_nested_while_filling_missing_columns = */ true);

            if (!message_serializer)
            {
                throw Exception(
                    "Not found matches between the names of the columns {" + boost::algorithm::join(column_names, ", ")
                        + "} and the fields {" + boost::algorithm::join(getFieldNames(message_descriptor), ", ") + "} of the message "
                        + quoteString(message_descriptor.full_name()) + " in the protobuf schema",
                    ErrorCodes::NO_COLUMNS_SERIALIZED_TO_PROTOBUF_FIELDS);
            }

            missing_column_indices.clear();
            missing_column_indices.reserve(column_names.size() - used_column_indices.size());
            auto used_column_indices_sorted = std::move(used_column_indices);
            ::sort(used_column_indices_sorted.begin(), used_column_indices_sorted.end());
            boost::range::set_difference(collections::range(column_names.size()), used_column_indices_sorted,
                                         std::back_inserter(missing_column_indices));

            if (!with_envelope)
            {
                *root_serializer_ptr = message_serializer.get();
#if 0
                LOG_INFO(&Poco::Logger::get("ProtobufSerializer"), "Serialization tree:\n{}", get_root_desc_function(0));
#endif
                return message_serializer;
            }
            else
            {
                auto envelope_serializer = std::make_unique<ProtobufSerializerEnvelope>(std::move(message_serializer), reader_or_writer);
                *root_serializer_ptr = envelope_serializer.get();
#if 0
                LOG_INFO(&Poco::Logger::get("ProtobufSerializer"), "Serialization tree:\n{}", get_root_desc_function(0));
#endif
                return envelope_serializer;
            }
        }

    private:
        /// Collects all field names from the message (used only to format error messages).
        static Strings getFieldNames(const MessageDescriptor & message_descriptor)
        {
            Strings field_names;
            field_names.reserve(message_descriptor.field_count());
            for (int i : collections::range(message_descriptor.field_count()))
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
            for (int i : collections::range(message_descriptor.field_count()))
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
            for (int i : collections::range(message_descriptor.field_count()))
            {
                const auto & field_descriptor = *message_descriptor.field(i);
                std::string_view suffix;
                if (columnNameStartsWithFieldName(column_name, field_descriptor, suffix))
                {
                    out_field_descriptors_with_suffixes.emplace_back(&field_descriptor, suffix);
                }
            }

            /// Shorter suffixes first.
            ::sort(out_field_descriptors_with_suffixes.begin(), out_field_descriptors_with_suffixes.end(),
                      [](const std::pair<const FieldDescriptor *, std::string_view /* suffix */> & f1,
                         const std::pair<const FieldDescriptor *, std::string_view /* suffix */> & f2)
            {
                return f1.second.length() < f2.second.length();
            });

            return !out_field_descriptors_with_suffixes.empty();
        }

        /// Removes TypeIndex::Array from the specified vector of data types,
        /// and also removes corresponding elements from two other vectors.
        template <typename T1, typename T2>
        static void removeNonArrayElements(DataTypes & data_types, std::vector<T1> & elements1, std::vector<T2> & elements2)
        {
            size_t initial_size = data_types.size();
            assert(initial_size == elements1.size() && initial_size == elements2.size());
            data_types.reserve(initial_size * 2);
            elements1.reserve(initial_size * 2);
            elements2.reserve(initial_size * 2);
            for (size_t i : collections::range(initial_size))
            {
                if (data_types[i]->getTypeId() == TypeIndex::Array)
                {
                    data_types.push_back(std::move(data_types[i]));
                    elements1.push_back(std::move(elements1[i]));
                    elements2.push_back(std::move(elements2[i]));
                }
            }
            data_types.erase(data_types.begin(), data_types.begin() + initial_size);
            elements1.erase(elements1.begin(), elements1.begin() + initial_size);
            elements2.erase(elements2.begin(), elements2.begin() + initial_size);
        }

        /// Treats specified column indices as indices in another vector of column indices.
        /// Useful for handling of nested messages.
        static void transformColumnIndices(std::vector<size_t> & column_indices, const std::vector<size_t> & outer_indices)
        {
            for (size_t & idx : column_indices)
                idx = outer_indices[idx];
        }

        /// Builds a serializer for a protobuf message (root or nested).
        ///
        /// Some of the passed columns might be skipped, the function sets `used_column_indices` to
        /// the list of those columns which match any fields in the protobuf message.
        ///
        /// Normally `columns_are_reordered_outside` should be false - if it's false it means that
        /// the used column indices will be passed to ProtobufSerializerMessage, which will write/read
        /// only those columns and set the rest of columns by default.
        /// Set `columns_are_reordered_outside` to true if you're going to reorder columns
        /// according to `used_column_indices` returned and pass to
        /// ProtobufSerializerMessage::setColumns() only the columns which are actually used.
        std::unique_ptr<ProtobufSerializerMessage> buildMessageSerializerImpl(
            size_t num_columns,
            const String * column_names,
            const DataTypePtr * data_types,
            const MessageDescriptor & message_descriptor,
            bool with_length_delimiter,
            const FieldDescriptor * parent_field_descriptor,
            std::vector<size_t> & used_column_indices,
            bool columns_are_reordered_outside,
            bool check_nested_while_filling_missing_columns)
        {
            std::vector<std::string_view> column_names_sv;
            column_names_sv.reserve(num_columns);
            for (size_t i = 0; i != num_columns; ++i)
                column_names_sv.emplace_back(column_names[i]);

            return buildMessageSerializerImpl(
                num_columns,
                column_names_sv.data(),
                data_types,
                message_descriptor,
                with_length_delimiter,
                parent_field_descriptor,
                used_column_indices,
                columns_are_reordered_outside,
                check_nested_while_filling_missing_columns);
        }

        std::unique_ptr<ProtobufSerializerMessage> buildMessageSerializerImpl(
            size_t num_columns,
            const std::string_view * column_names,
            const DataTypePtr * data_types,
            const MessageDescriptor & message_descriptor,
            bool with_length_delimiter,
            const FieldDescriptor * parent_field_descriptor,
            std::vector<size_t> & used_column_indices,
            bool columns_are_reordered_outside,
            bool check_nested_while_filling_missing_columns)
        {
            std::vector<ProtobufSerializerMessage::FieldDesc> field_descs;
            boost::container::flat_map<const FieldDescriptor *, std::string_view> field_descriptors_in_use;

            used_column_indices.clear();
            used_column_indices.reserve(num_columns);
            boost::container::flat_set<size_t> used_column_indices_sorted;
            used_column_indices_sorted.reserve(num_columns);
            size_t sequential_column_index = 0;

            auto add_field_serializer = [&](const std::string_view & column_name_,
                                            std::vector<size_t> && column_indices_,
                                            const FieldDescriptor & field_descriptor_,
                                            std::unique_ptr<ProtobufSerializer> field_serializer_)
            {
                auto it = field_descriptors_in_use.find(&field_descriptor_);
                if (it != field_descriptors_in_use.end())
                {
                    throw Exception(
                        "Multiple columns (" + backQuote(StringRef{it->second}) + ", "
                            + backQuote(StringRef{column_name_}) + ") cannot be serialized to a single protobuf field "
                            + quoteString(field_descriptor_.full_name()),
                        ErrorCodes::MULTIPLE_COLUMNS_SERIALIZED_TO_SAME_PROTOBUF_FIELD);
                }

                used_column_indices.insert(used_column_indices.end(), column_indices_.begin(), column_indices_.end());
                used_column_indices_sorted.insert(column_indices_.begin(), column_indices_.end());

                auto column_indices_to_pass_to_message_serializer = std::move(column_indices_);
                if (columns_are_reordered_outside)
                {
                    for (auto & index : column_indices_to_pass_to_message_serializer)
                        index = sequential_column_index++;
                }

                field_descs.push_back({std::move(column_indices_to_pass_to_message_serializer), &field_descriptor_, std::move(field_serializer_)});
                field_descriptors_in_use.emplace(&field_descriptor_, column_name_);
            };

            std::vector<std::pair<const FieldDescriptor *, std::string_view>> field_descriptors_with_suffixes;

            /// We're going through all the passed columns.
            for (size_t column_idx : collections::range(num_columns))
            {
                if (used_column_indices_sorted.count(column_idx))
                    continue;

                const auto & column_name = column_names[column_idx];
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
                        add_field_serializer(column_name, {column_idx}, field_descriptor, std::move(field_serializer));
                        continue;
                    }
                }

                for (const auto & [field_descriptor, suffix] : field_descriptors_with_suffixes)
                {
                    if (!suffix.empty())
                    {
                        /// Complex case: one or more columns are serialized as a nested message.
                        std::vector<size_t> nested_column_indices;
                        std::vector<std::string_view> nested_column_names;
                        nested_column_indices.reserve(num_columns - used_column_indices.size());
                        nested_column_names.reserve(num_columns - used_column_indices.size());
                        nested_column_indices.push_back(column_idx);
                        nested_column_names.push_back(suffix);

                        for (size_t j : collections::range(column_idx + 1, num_columns))
                        {
                            if (used_column_indices_sorted.count(j))
                                continue;
                            std::string_view other_suffix;
                            if (!columnNameStartsWithFieldName(column_names[j], *field_descriptor, other_suffix))
                                continue;
                            nested_column_indices.push_back(j);
                            nested_column_names.push_back(other_suffix);
                        }

                        DataTypes nested_data_types;
                        nested_data_types.reserve(nested_column_indices.size());
                        for (size_t j : nested_column_indices)
                            nested_data_types.push_back(data_types[j]);

                        /// Now we have up to `nested_message_column_names.size()` columns
                        /// which can be serialized as a nested message.

                        /// We will try to serialize those columns as one nested message,
                        /// then, if failed, as an array of nested messages (on condition if those columns are array).
                        bool has_fallback_to_array_of_nested_messages = false;
                        if (field_descriptor->is_repeated())
                        {
                            bool has_arrays
                                = boost::range::find_if(
                                      nested_data_types, [](const DataTypePtr & dt) { return (dt->getTypeId() == TypeIndex::Array); })
                                != nested_data_types.end();
                            if (has_arrays)
                                has_fallback_to_array_of_nested_messages = true;
                        }

                        /// Try to serialize those columns as one nested message.
                        try
                        {
                            std::vector<size_t> used_column_indices_in_nested;
                            auto nested_message_serializer = buildMessageSerializerImpl(
                                nested_column_names.size(),
                                nested_column_names.data(),
                                nested_data_types.data(),
                                *field_descriptor->message_type(),
                                /* with_length_delimiter = */ false,
                                field_descriptor,
                                used_column_indices_in_nested,
                                /* columns_are_reordered_outside = */ true,
                                /* check_nested_while_filling_missing_columns = */ false);

                            /// `columns_are_reordered_outside` is true because column indices are
                            /// going to be transformed and then written to the outer message,
                            /// see add_field_serializer() below.

                            if (nested_message_serializer)
                            {
                                transformColumnIndices(used_column_indices_in_nested, nested_column_indices);
                                add_field_serializer(
                                    column_name,
                                    std::move(used_column_indices_in_nested),
                                    *field_descriptor,
                                    std::move(nested_message_serializer));
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
                            /// Try to serialize those columns as an array of nested messages.
                            removeNonArrayElements(nested_data_types, nested_column_names, nested_column_indices);
                            for (DataTypePtr & dt : nested_data_types)
                                dt = assert_cast<const DataTypeArray &>(*dt).getNestedType();

                            std::vector<size_t> used_column_indices_in_nested;
                            auto nested_message_serializer = buildMessageSerializerImpl(
                                nested_column_names.size(),
                                nested_column_names.data(),
                                nested_data_types.data(),
                                *field_descriptor->message_type(),
                                /* with_length_delimiter = */ false,
                                field_descriptor,
                                used_column_indices_in_nested,
                                /* columns_are_reordered_outside = */ true,
                                /* check_nested_while_filling_missing_columns = */ false);

                            /// `columns_are_reordered_outside` is true because column indices are
                            /// going to be transformed and then written to the outer message,
                            /// see add_field_serializer() below.

                            if (nested_message_serializer)
                            {
                                std::vector<std::string_view> column_names_used;
                                column_names_used.reserve(used_column_indices_in_nested.size());
                                for (size_t i : used_column_indices_in_nested)
                                    column_names_used.emplace_back(nested_column_names[i]);
                                auto field_serializer = std::make_unique<ProtobufSerializerFlattenedNestedAsArrayOfNestedMessages>(
                                    std::move(column_names_used), field_descriptor, std::move(nested_message_serializer), get_root_desc_function);
                                transformColumnIndices(used_column_indices_in_nested, nested_column_indices);
                                add_field_serializer(column_name, std::move(used_column_indices_in_nested), *field_descriptor, std::move(field_serializer));
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
                for (int i : collections::range(message_descriptor.field_count()))
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

            std::unique_ptr<RowInputMissingColumnsFiller> missing_columns_filler;
            if (reader_or_writer.reader)
            {
                if (check_nested_while_filling_missing_columns)
                    missing_columns_filler = std::make_unique<RowInputMissingColumnsFiller>(num_columns, column_names, data_types);
                else
                    missing_columns_filler = std::make_unique<RowInputMissingColumnsFiller>();
            }

            return std::make_unique<ProtobufSerializerMessage>(
                std::move(field_descs), parent_field_descriptor, with_length_delimiter,
                std::move(missing_columns_filler), reader_or_writer);
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
                case TypeIndex::UInt8: return std::make_unique<ProtobufSerializerNumber<UInt8>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::UInt16: return std::make_unique<ProtobufSerializerNumber<UInt16>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::UInt32: return std::make_unique<ProtobufSerializerNumber<UInt32>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::UInt64: return std::make_unique<ProtobufSerializerNumber<UInt64>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::UInt128: return std::make_unique<ProtobufSerializerNumber<UInt128>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::UInt256: return std::make_unique<ProtobufSerializerNumber<UInt256>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::Int8: return std::make_unique<ProtobufSerializerNumber<Int8>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::Int16: return std::make_unique<ProtobufSerializerNumber<Int16>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::Int32: return std::make_unique<ProtobufSerializerNumber<Int32>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::Int64: return std::make_unique<ProtobufSerializerNumber<Int64>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::Int128: return std::make_unique<ProtobufSerializerNumber<Int128>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::Int256: return std::make_unique<ProtobufSerializerNumber<Int256>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::Float32: return std::make_unique<ProtobufSerializerNumber<Float32>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::Float64: return std::make_unique<ProtobufSerializerNumber<Float64>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::Date: return std::make_unique<ProtobufSerializerDate>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::DateTime: return std::make_unique<ProtobufSerializerDateTime>(column_name, assert_cast<const DataTypeDateTime &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::DateTime64: return std::make_unique<ProtobufSerializerDateTime64>(column_name, assert_cast<const DataTypeDateTime64 &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::String: return std::make_unique<ProtobufSerializerString<false>>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::FixedString: return std::make_unique<ProtobufSerializerString<true>>(column_name, typeid_cast<std::shared_ptr<const DataTypeFixedString>>(data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Enum8: return std::make_unique<ProtobufSerializerEnum<Int8>>(column_name, typeid_cast<std::shared_ptr<const DataTypeEnum8>>(data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Enum16: return std::make_unique<ProtobufSerializerEnum<Int16>>(column_name, typeid_cast<std::shared_ptr<const DataTypeEnum16>>(data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Decimal32: return std::make_unique<ProtobufSerializerDecimal<Decimal32>>(column_name, assert_cast<const DataTypeDecimal<Decimal32> &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Decimal64: return std::make_unique<ProtobufSerializerDecimal<Decimal64>>(column_name, assert_cast<const DataTypeDecimal<Decimal64> &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Decimal128: return std::make_unique<ProtobufSerializerDecimal<Decimal128>>(column_name, assert_cast<const DataTypeDecimal<Decimal128> &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::Decimal256: return std::make_unique<ProtobufSerializerDecimal<Decimal256>>(column_name, assert_cast<const DataTypeDecimal<Decimal256> &>(*data_type), field_descriptor, reader_or_writer);
                case TypeIndex::UUID: return std::make_unique<ProtobufSerializerUUID>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::Interval: return std::make_unique<ProtobufSerializerInterval>(column_name, field_descriptor, reader_or_writer);
                case TypeIndex::AggregateFunction: return std::make_unique<ProtobufSerializerAggregateFunction>(column_name, typeid_cast<std::shared_ptr<const DataTypeAggregateFunction>>(data_type), field_descriptor, reader_or_writer);

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
                        auto message_serializer = buildMessageSerializerImpl(
                            size_of_tuple,
                            tuple_data_type.getElementNames().data(),
                            tuple_data_type.getElements().data(),
                            *field_descriptor.message_type(),
                            /* with_length_delimiter = */ false,
                            &field_descriptor,
                            used_column_indices,
                            /* columns_are_reordered_outside = */ false,
                            /* check_nested_while_filling_missing_columns = */ false);

                        if (!message_serializer)
                        {
                            throw Exception(
                                "Not found matches between the names of the tuple's elements {"
                                    + boost::algorithm::join(tuple_data_type.getElementNames(), ", ") + "} and the fields {"
                                    + boost::algorithm::join(getFieldNames(*field_descriptor.message_type()), ", ") + "} of the message "
                                    + quoteString(field_descriptor.message_type()->full_name()) + " in the protobuf schema",
                                ErrorCodes::NO_COLUMNS_SERIALIZED_TO_PROTOBUF_FIELDS);
                        }

                        return std::make_unique<ProtobufSerializerTupleAsNestedMessage>(std::move(message_serializer));
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
                        column_name,
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
        std::function<String(size_t)> get_root_desc_function;
        std::shared_ptr<ProtobufSerializer *> root_serializer_ptr;
    };

    template <typename Type>
    DataTypePtr getEnumDataType(const google::protobuf::EnumDescriptor * enum_descriptor)
    {
        std::vector<std::pair<String, Type>> values;
        for (int i = 0; i != enum_descriptor->value_count(); ++i)
        {
            const auto * enum_value_descriptor = enum_descriptor->value(i);
            values.emplace_back(enum_value_descriptor->name(), enum_value_descriptor->number());
        }
        return std::make_shared<DataTypeEnum<Type>>(std::move(values));
    }

    NameAndTypePair getNameAndDataTypeFromField(const google::protobuf::FieldDescriptor * field_descriptor, bool allow_repeat = true)
    {
        if (allow_repeat && field_descriptor->is_map())
        {
            auto name_and_type = getNameAndDataTypeFromField(field_descriptor, false);
            const auto * tuple_type = assert_cast<const DataTypeTuple *>(name_and_type.type.get());
            return {name_and_type.name, std::make_shared<DataTypeMap>(tuple_type->getElements())};
        }

        if (allow_repeat && field_descriptor->is_repeated())
        {
            auto name_and_type = getNameAndDataTypeFromField(field_descriptor, false);
            return {name_and_type.name, std::make_shared<DataTypeArray>(name_and_type.type)};
        }

        switch (field_descriptor->type())
        {
            case FieldTypeId::TYPE_SFIXED32: [[fallthrough]];
            case FieldTypeId::TYPE_SINT32: [[fallthrough]];
            case FieldTypeId::TYPE_INT32:
                return {field_descriptor->name(), std::make_shared<DataTypeInt32>()};
            case FieldTypeId::TYPE_SFIXED64: [[fallthrough]];
            case FieldTypeId::TYPE_SINT64: [[fallthrough]];
            case FieldTypeId::TYPE_INT64:
                return {field_descriptor->name(), std::make_shared<DataTypeInt64>()};
            case FieldTypeId::TYPE_BOOL:
                return {field_descriptor->name(), std::make_shared<DataTypeUInt8>()};
            case FieldTypeId::TYPE_FLOAT:
                return {field_descriptor->name(), std::make_shared<DataTypeFloat32>()};
            case FieldTypeId::TYPE_DOUBLE:
                return {field_descriptor->name(), std::make_shared<DataTypeFloat64>()};
            case FieldTypeId::TYPE_UINT32: [[fallthrough]];
            case FieldTypeId::TYPE_FIXED32:
                return {field_descriptor->name(), std::make_shared<DataTypeUInt32>()};
            case FieldTypeId::TYPE_UINT64: [[fallthrough]];
            case FieldTypeId::TYPE_FIXED64:
                return {field_descriptor->name(), std::make_shared<DataTypeUInt64>()};
            case FieldTypeId::TYPE_BYTES: [[fallthrough]];
            case FieldTypeId::TYPE_STRING:
                return {field_descriptor->name(), std::make_shared<DataTypeString>()};
            case FieldTypeId::TYPE_ENUM:
            {
                const auto * enum_descriptor = field_descriptor->enum_type();
                if (enum_descriptor->value_count() == 0)
                    throw Exception("Empty enum field", ErrorCodes::BAD_ARGUMENTS);
                int max_abs = std::abs(enum_descriptor->value(0)->number());
                for (int i = 1; i != enum_descriptor->value_count(); ++i)
                {
                    if (std::abs(enum_descriptor->value(i)->number()) > max_abs)
                        max_abs = std::abs(enum_descriptor->value(i)->number());
                }
                if (max_abs < 128)
                    return {field_descriptor->name(), getEnumDataType<Int8>(enum_descriptor)};
                else if (max_abs < 32768)
                    return {field_descriptor->name(), getEnumDataType<Int16>(enum_descriptor)};
                else
                    throw Exception("ClickHouse supports only 8-bit and 16-bit enums", ErrorCodes::BAD_ARGUMENTS);
            }
            case FieldTypeId::TYPE_GROUP: [[fallthrough]];
            case FieldTypeId::TYPE_MESSAGE:
            {
                const auto * message_descriptor = field_descriptor->message_type();
                if (message_descriptor->field_count() == 1)
                {
                    const auto * nested_field_descriptor = message_descriptor->field(0);
                    auto nested_name_and_type = getNameAndDataTypeFromField(nested_field_descriptor);
                    return {field_descriptor->name() + "_" + nested_name_and_type.name, nested_name_and_type.type};
                }
                else
                {
                    DataTypes nested_types;
                    Strings nested_names;
                    for (int i = 0; i != message_descriptor->field_count(); ++i)
                    {
                        auto nested_name_and_type = getNameAndDataTypeFromField(message_descriptor->field(i));
                        nested_types.push_back(nested_name_and_type.type);
                        nested_names.push_back(nested_name_and_type.name);
                    }
                    return {field_descriptor->name(), std::make_shared<DataTypeTuple>(std::move(nested_types), std::move(nested_names))};
                }
            }
        }

        __builtin_unreachable();
    }
}

std::unique_ptr<ProtobufSerializer> ProtobufSerializer::create(
    const Strings & column_names,
    const DataTypes & data_types,
    std::vector<size_t> & missing_column_indices,
    const google::protobuf::Descriptor & message_descriptor,
    bool with_length_delimiter,
    bool with_envelope,
    ProtobufReader & reader)
{
    return ProtobufSerializerBuilder(reader).buildMessageSerializer(column_names, data_types, missing_column_indices, message_descriptor, with_length_delimiter, with_envelope);
}

std::unique_ptr<ProtobufSerializer> ProtobufSerializer::create(
    const Strings & column_names,
    const DataTypes & data_types,
    const google::protobuf::Descriptor & message_descriptor,
    bool with_length_delimiter,
    bool with_envelope,
    ProtobufWriter & writer)
{
    std::vector<size_t> missing_column_indices;
    return ProtobufSerializerBuilder(writer).buildMessageSerializer(column_names, data_types, missing_column_indices, message_descriptor, with_length_delimiter, with_envelope);
}

NamesAndTypesList protobufSchemaToCHSchema(const google::protobuf::Descriptor * message_descriptor)
{
    NamesAndTypesList schema;
    for (int i = 0; i != message_descriptor->field_count(); ++i)
        schema.push_back(getNameAndDataTypeFromField(message_descriptor->field(i)));
    return schema;
}

}
#endif
