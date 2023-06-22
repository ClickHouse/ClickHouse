#include <Formats/CapnProtoSerializer.h>
#include <Formats/FormatSettings.h>
#include <Formats/CapnProtoSchema.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/IDataType.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnMap.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
    extern const int BAD_TYPE_OF_FIELD;
    extern const int CAPN_PROTO_BAD_CAST;
    extern const int INCORRECT_DATA;
    extern const int ILLEGAL_COLUMN;
}

namespace
{
    std::pair<String, String> splitFieldName(const String & name)
    {
        const auto * begin = name.data();
        const auto * end = name.data() + name.size();
        const auto * it = find_first_symbols<'_', '.'>(begin, end);
        String first = String(begin, it);
        String second = it == end ? "" : String(it + 1, end);
        return {first, second};
    }

    std::optional<capnp::StructSchema::Field> findFieldByName(const capnp::StructSchema & struct_schema, const String & name)
    {
        const auto & fields = struct_schema.getFields();
        for (auto field : fields)
        {
            auto field_name = String(field.getProto().getName());
            if (boost::to_lower_copy(name) == boost::to_lower_copy(field_name))
                return field;
        }
        return std::nullopt;
    }

    [[noreturn]] void throwCannotConvert(const DataTypePtr & type, const String & name, const capnp::Type & capnp_type)
    {
        throw Exception(
            ErrorCodes::CAPN_PROTO_BAD_CAST,
            "Cannot convert ClickHouse column \"{}\" with type {} to CapnProto type {}",
            name,
            type->getName(),
            getCapnProtoFullTypeName(capnp_type));
    }

    struct FieldBuilder
    {
        virtual ~FieldBuilder() = default;
    };

    struct ListBuilder : public FieldBuilder
    {
        explicit ListBuilder(capnp::DynamicValue::Builder builder) : impl(builder.as<capnp::DynamicList>())
        {
        }

        capnp::DynamicList::Builder impl;
        std::vector<std::unique_ptr<FieldBuilder>> nested_builders;
    };

    struct StructBuilder : public FieldBuilder
    {
        explicit StructBuilder(capnp::DynamicValue::Builder builder, size_t fields_size) : impl(builder.as<capnp::DynamicStruct>()), field_builders(fields_size)
        {
        }

        explicit StructBuilder(capnp::DynamicStruct::Builder struct_builder, size_t fields_size) : impl(std::move(struct_builder)), field_builders(fields_size)
        {
        }

        capnp::DynamicStruct::Builder impl;
        std::vector<std::unique_ptr<FieldBuilder>> field_builders;
    };

    std::unique_ptr<FieldBuilder> initStructFieldBuilderIfNeeded(const ColumnPtr & column, size_t row_num, capnp::DynamicStruct::Builder & struct_builder, const capnp::StructSchema::Field & field, const capnp::Type & capnp_type, size_t nested_fields_size)
    {
        switch (capnp_type.which())
        {
            case capnp::schema::Type::LIST:
            {
                const auto * array_column = assert_cast<const ColumnArray *>(column.get());
                size_t size = array_column->getOffsets()[row_num] - array_column->getOffsets()[row_num - 1];
                return std::make_unique<ListBuilder>(struct_builder.init(field, static_cast<unsigned>(size)));
            }
            case capnp::schema::Type::STRUCT:
            {
                return std::make_unique<StructBuilder>(struct_builder.init(field), nested_fields_size);
            }
            default:
                return nullptr;
        }
    }

    class ICapnProtoSerializer
    {
    public:
        virtual std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder * builder, size_t row_num) = 0;
        virtual void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) = 0;

        virtual ~ICapnProtoSerializer() = default;
    };

    template <typename NumericType, bool is_bool_data_type, capnp::DynamicValue::Type capnp_dynamic_type>
    class CapnProtoIntegerSerializer : public ICapnProtoSerializer
    {
    public:
        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            if constexpr (capnp_dynamic_type == capnp::DynamicValue::Type::INT)
                return capnp::DynamicValue::Reader(column->getInt(row_num));
            if constexpr (capnp_dynamic_type == capnp::DynamicValue::Type::UINT)
                return capnp::DynamicValue::Reader(column->getUInt(row_num));
            return capnp::DynamicValue::Reader(column->getBool(row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            NumericType value;
            if constexpr (capnp_dynamic_type == capnp::DynamicValue::Type::INT)
                value = static_cast<NumericType>(reader.as<Int64>());
            else if constexpr (capnp_dynamic_type == capnp::DynamicValue::Type::UINT)
                value = static_cast<NumericType>(reader.as<UInt64>());
            else if constexpr (capnp_dynamic_type == capnp::DynamicValue::Type::BOOL)
                value = static_cast<NumericType>(reader.as<bool>());

            if constexpr (is_bool_data_type)
                assert_cast<ColumnUInt8 &>(column).insertValue(static_cast<bool>(value));
            else
                assert_cast<ColumnVector<NumericType> &>(column).insertValue(value);
        }
    };

    template <typename NumericType, bool is_bool_data_type = false>
    static std::unique_ptr<ICapnProtoSerializer> createIntegerSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
    {
        switch (capnp_type.which())
        {
            case capnp::schema::Type::INT8: [[fallthrough]];
            case capnp::schema::Type::INT16: [[fallthrough]];
            case capnp::schema::Type::INT32: [[fallthrough]];
            case capnp::schema::Type::INT64:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, is_bool_data_type, capnp::DynamicValue::Type::INT>>();
            case capnp::schema::Type::UINT8: [[fallthrough]];
            case capnp::schema::Type::UINT16: [[fallthrough]];
            case capnp::schema::Type::UINT32: [[fallthrough]];
            case capnp::schema::Type::UINT64:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, is_bool_data_type, capnp::DynamicValue::Type::UINT>>();
            case capnp::schema::Type::BOOL:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, is_bool_data_type, capnp::DynamicValue::Type::BOOL>>();
            default:
                throwCannotConvert(data_type, column_name, capnp_type);
        }
    }

    template <typename NumericType>
    class CapnProtoBigIntegerSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoBigIntegerSerializer(const DataTypePtr & data_type_, const String & column_name, const capnp::Type & capnp_type) : data_type(data_type_)
        {
            if (!capnp_type.isData())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            auto data = column->getDataAt(row_num);
            return capnp::DynamicValue::Reader(capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto value = reader.as<capnp::Data>();
            if (value.size() != sizeof(NumericType))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size of {} value: {}", data_type->getName(), value.size());

            column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
        }

    private:
        DataTypePtr data_type;
    };

    template <typename FloatType>
    class CapnProtoFloatSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoFloatSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            if (!capnp_type.isFloat32() && !capnp_type.isFloat64())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            return capnp::DynamicValue::Reader(column->getFloat64(row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            assert_cast<ColumnVector<FloatType> &>(column).insertValue(reader.as<FloatType>());
        }
    };

    template <typename EnumType>
    class CapnProtoEnumSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoEnumSerializer(
            const DataTypePtr & data_type_,
            const String & column_name,
            const capnp::Type & capnp_type,
            const FormatSettings::CapnProtoEnumComparingMode enum_comparing_mode_) : data_type(data_type_), enum_schema(capnp_type.asEnum()), enum_comparing_mode(enum_comparing_mode_)
        {
            if (!capnp_type.isEnum())
                throwCannotConvert(data_type, column_name, capnp_type);

            bool to_lower = enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_NAMES_CASE_INSENSITIVE;
            const auto * enum_type = assert_cast<const DataTypeEnum<EnumType> *>(data_type.get());
            const auto & enum_values = dynamic_cast<const EnumValues<EnumType> &>(*enum_type);

            auto enumerants = enum_schema.getEnumerants();
            constexpr auto max_value = std::is_same_v<EnumType, Int8> ? INT8_MAX : INT16_MAX;
            if (enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_VALUES)
            {
                /// In CapnProto Enum fields are numbered sequentially starting from zero.
                if (enumerants.size() > max_value)
                    throw Exception(
                        ErrorCodes::CAPN_PROTO_BAD_CAST,
                        "Enum from CapnProto schema contains values that are out of range for Clickhouse enum type {}",
                        data_type->getName());

                auto values = enum_values.getSetOfAllValues();
                std::unordered_set<EnumType> capn_enum_values;
                for (auto enumerant : enumerants)
                    capn_enum_values.insert(EnumType(enumerant.getOrdinal()));
                if (values != capn_enum_values)
                    throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "The set of values in Enum from CapnProto schema is different from the set of values in ClickHouse Enum");
            }
            else
            {
                auto names = enum_values.getSetOfAllNames(to_lower);
                std::unordered_set<String> capn_enum_names;

                for (auto enumerant : enumerants)
                {
                    String name = enumerant.getProto().getName();
                    capn_enum_names.insert(to_lower ? boost::algorithm::to_lower_copy(name) : name);
                }

                if (names != capn_enum_names)
                    throw Exception(
                        ErrorCodes::CAPN_PROTO_BAD_CAST,
                        "The set of names in Enum from CapnProto schema is different from the set of names in ClickHouse Enum");
            }
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            const auto * enum_data_type = assert_cast<const DataTypeEnum<EnumType> *>(data_type.get());
            EnumType enum_value = assert_cast<const ColumnVector<EnumType> &>(*column).getElement(row_num);
            if (enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_VALUES)
                return capnp::DynamicValue::Reader(capnp::DynamicEnum(enum_schema, enum_value));

            auto enum_name = enum_data_type->getNameForValue(enum_value);
            for (const auto enumerant : enum_schema.getEnumerants())
            {
                if (compareEnumNames(String(enum_name), enumerant.getProto().getName(), enum_comparing_mode))
                    return capnp::DynamicValue::Reader(capnp::DynamicEnum(enumerant));
            }

            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert CLickHouse Enum value to CapnProto Enum");
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto enum_value = reader.as<capnp::DynamicEnum>();
            auto enumerant = *kj::_::readMaybe(enum_value.getEnumerant());
            auto enum_type = assert_cast<const DataTypeEnum<EnumType> *>(data_type.get());
            DataTypePtr nested_type = std::make_shared<DataTypeNumber<EnumType>>();
            switch (enum_comparing_mode)
            {
                case FormatSettings::CapnProtoEnumComparingMode::BY_VALUES:
                {
                    assert_cast<ColumnVector<EnumType> &>(column).insertValue(static_cast<EnumType>(enumerant.getOrdinal()));
                    return;
                }
                case FormatSettings::CapnProtoEnumComparingMode::BY_NAMES:
                {
                    auto value = enum_type->getValue(String(enumerant.getProto().getName()));
                    assert_cast<ColumnVector<EnumType> &>(column).insertValue(value);
                    return;
                }
                case FormatSettings::CapnProtoEnumComparingMode::BY_NAMES_CASE_INSENSITIVE:
                {
                    /// Find the same enum name case insensitive.
                    String enum_name = enumerant.getProto().getName();
                    for (auto & name : enum_type->getAllRegisteredNames())
                    {
                        if (compareEnumNames(name, enum_name, enum_comparing_mode))
                        {
                            assert_cast<ColumnVector<EnumType> &>(column).insertValue(enum_type->getValue(name));
                            break;
                        }
                    }
                    return;
                }
            }
        }

    private:
        bool compareEnumNames(const String & first, const String & second, const FormatSettings::CapnProtoEnumComparingMode mode)
        {
            if (mode == FormatSettings::CapnProtoEnumComparingMode::BY_NAMES_CASE_INSENSITIVE)
                return boost::algorithm::to_lower_copy(first) == boost::algorithm::to_lower_copy(second);
            return first == second;
        }

        DataTypePtr data_type;
        capnp::EnumSchema enum_schema;
        const FormatSettings::CapnProtoEnumComparingMode enum_comparing_mode;
    };

    class CapnProtoDateSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoDateSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            if (!capnp_type.isUInt16())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            return capnp::DynamicValue::Reader(column->getUInt(row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            assert_cast<ColumnDate &>(column).insertValue(reader.as<UInt16>());
        }
    };

    class CapnProtoDate32Serializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoDate32Serializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            if (!capnp_type.isInt32())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            return capnp::DynamicValue::Reader(column->getInt(row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            assert_cast<ColumnDate32 &>(column).insertValue(reader.as<Int32>());
        }
    };

    class CapnProtoDateTimeSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoDateTimeSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            if (!capnp_type.isUInt32())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            return capnp::DynamicValue::Reader(column->getInt(row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            assert_cast<ColumnDateTime &>(column).insertValue(reader.as<UInt32>());
        }
    };

    class CapnProtoDateTime64Serializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoDateTime64Serializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            if (!capnp_type.isInt64())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            return capnp::DynamicValue::Reader(column->getInt(row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            assert_cast<ColumnDateTime64 &>(column).insertValue(reader.as<Int64>());
        }
    };

    template <typename DecimalType>
    class CapnProtoDecimalSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoDecimalSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            auto which = WhichDataType(data_type);
            if ((!capnp_type.isInt32() && which.isDecimal32()) || (!capnp_type.isInt64() && which.isDecimal64()))
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            return capnp::DynamicValue::Reader(column->getInt(row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            assert_cast<ColumnDecimal<DecimalType> &>(column).insertValue(reader.as<typename DecimalType::NativeType>());
        }
    };

    template <typename DecimalType>
    class CapnProtoBigDecimalSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoBigDecimalSerializer(const DataTypePtr & data_type_, const String & column_name, const capnp::Type & capnp_type) : data_type(data_type_)
        {
            if (!capnp_type.isData())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            auto data = column->getDataAt(row_num);
            return capnp::DynamicValue::Reader(capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto value = reader.as<capnp::Data>();
            if (value.size() != sizeof(DecimalType))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size of {} value: {}", data_type->getName(), value.size());

            column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
        }

    private:
        DataTypePtr data_type;
    };

    template <bool is_binary>
    class CapnProtoStringSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoStringSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type_) : capnp_type(capnp_type_)
        {
            if (!capnp_type.isData() && !capnp_type.isText())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            if constexpr (is_binary)
            {
                auto data = column->getDataAt(row_num);
                return capnp::DynamicValue::Reader(capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size));
            }

            /// For type TEXT 0 byte must be at the end, so we cannot use getDataAt method, because it cuts last 0 byte.
            const auto & string_column = assert_cast<const ColumnString &>(*column);
            const auto & chars = string_column.getChars();
            const auto & offsets = string_column.getOffsets();
            size_t start = offsets[ssize_t(row_num) - 1];
            size_t size = offsets[row_num] - start;
            return capnp::DynamicValue::Reader(capnp::Text::Reader(reinterpret_cast<const char *>(&chars[start]), size));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            if constexpr (is_binary)
            {
                auto value = reader.as<capnp::Data>();
                column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
            else
            {
                auto value = reader.as<capnp::Text>();
                column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
        }

    private:
        capnp::Type capnp_type;
    };

    template <bool is_binary>
    class CapnProtoFixedStringSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoFixedStringSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type_) : capnp_type(capnp_type_)
        {
            if (!capnp_type.isData() && !capnp_type.isText())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            auto data = column->getDataAt(row_num);
            if constexpr (is_binary)
                return capnp::DynamicValue::Reader(capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size));

            if (data.data[data.size - 1] == 0)
                return capnp::DynamicValue::Reader(capnp::Text::Reader(reinterpret_cast<const char *>(data.data), data.size));

            /// In TEXT type data should be null-terminated, but ClickHouse FixedString data could not be.
            /// To make data null-terminated we should copy it to temporary String object and use it in capnp::Text::Reader.
            /// Note that capnp::Text::Reader works only with pointer to the data and it's size, so we should
            /// guarantee that new String object life time is longer than capnp::Text::Reader life time.
            tmp_string = data.toString();
            return capnp::DynamicValue::Reader(capnp::Text::Reader(reinterpret_cast<const char *>(tmp_string.data()), tmp_string.size()));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto & fixed_string_column = assert_cast<ColumnFixedString &>(column);
            if constexpr (is_binary)
            {
                auto value = reader.as<capnp::Data>();
                if (value.size() > fixed_string_column.getN())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read data with size {} to FixedString with size {}", value.size(), fixed_string_column.getN());

                fixed_string_column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
            else
            {
                auto value = reader.as<capnp::Text>();
                if (value.size() > fixed_string_column.getN())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read data with size {} to FixedString with size {}", value.size(), fixed_string_column.getN());

                fixed_string_column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
        }

    private:
        String tmp_string;
        capnp::Type capnp_type;
    };

    class CapnProtoIPv4Serializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoIPv4Serializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            if (!capnp_type.isUInt32())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            return capnp::DynamicValue::Reader(assert_cast<const ColumnIPv4 &>(*column).getElement(row_num).toUnderType());
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            assert_cast<ColumnIPv4 &>(column).insertValue(IPv4(reader.as<UInt32>()));
        }
    };

    class CapnProtoIPv6Serializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoIPv6Serializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            if (!capnp_type.isData())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            auto data = column->getDataAt(row_num);
            return capnp::DynamicValue::Reader(capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto value = reader.as<capnp::Data>();
            if (value.size() != sizeof(IPv6))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size of IPv6 value: {}", value.size());

            column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
        }
    };

    class CapnProtoUUIDSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoUUIDSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            if (!capnp_type.isData())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder *, size_t row_num) override
        {
            auto data = column->getDataAt(row_num);
            return capnp::DynamicValue::Reader(capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size));
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto value = reader.as<capnp::Data>();
            if (value.size() != sizeof(UUID))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size of UUID value: {}", value.size());

            column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
        }
    };

    std::unique_ptr<ICapnProtoSerializer> createSerializer(const DataTypePtr & type, const String & name, const capnp::Type & capnp_type, const FormatSettings::CapnProto & settings);

    class CapnProtoLowCardinalitySerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoLowCardinalitySerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type, const FormatSettings::CapnProto & settings)
        {
            nested_serializer = createSerializer(assert_cast<const DataTypeLowCardinality &>(*data_type).getDictionaryType(), column_name, capnp_type, settings);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder * field_builder, size_t row_num) override
        {
            const auto & low_cardinality_column = assert_cast<const ColumnLowCardinality &>(*column);
            size_t index = low_cardinality_column.getIndexAt(row_num);
            const auto & dict_column = low_cardinality_column.getDictionary().getNestedColumn();
            return nested_serializer->writeRow(dict_column, field_builder, index);
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto & low_cardinality_column = assert_cast<ColumnLowCardinality &>(column);
            auto tmp_column = low_cardinality_column.getDictionary().getNestedColumn()->cloneEmpty();
            nested_serializer->readRow(*tmp_column, reader);
            low_cardinality_column.insertFromFullColumn(*tmp_column, 0);
        }

    private:
        std::unique_ptr<ICapnProtoSerializer> nested_serializer;
    };

    class CapnProtoNullableSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoNullableSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type, const FormatSettings::CapnProto & settings)
        {
            if (!capnp_type.isStruct())
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert column \"{}\": Nullable can be represented only as a named union of type Void and nested type, got CapnProto type {}",
                    column_name,
                    getCapnProtoFullTypeName(capnp_type));

            /// Check that struct is a named union of type VOID and one arbitrary type.
            auto struct_schema = capnp_type.asStruct();
            if (!checkIfStructIsNamedUnion(struct_schema))
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert column \"{}\": Nullable can be represented only as a named union of type Void and nested type."
                    "Given CapnProto struct is not a named union: {}",
                    column_name,
                    getCapnProtoFullTypeName(capnp_type));

            auto union_fields = struct_schema.getUnionFields();
            if (union_fields.size() != 2)
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert column \"{}\": Nullable can be represented only as a named union of type Void and nested type."
                    "Given CapnProto union have more than 2 fields: {}",
                    column_name,
                    getCapnProtoFullTypeName(capnp_type));

            auto first = union_fields[0];
            auto second = union_fields[1];
            auto nested_type = assert_cast<const DataTypeNullable *>(data_type.get())->getNestedType();
            if (first.getType().isVoid())
            {
                null_field = first;
                nested_field = second;
                nested_capnp_type = second.getType();
                if (nested_capnp_type.isStruct())
                    nested_fields_size = nested_capnp_type.asStruct().getFields().size();
                nested_serializer = createSerializer(nested_type, column_name, nested_capnp_type, settings);
            }
            else if (second.getType().isVoid())
            {
                null_field = second;
                nested_field = first;
                nested_capnp_type = first.getType();
                if (nested_capnp_type.isStruct())
                    nested_fields_size = nested_capnp_type.asStruct().getFields().size();
                nested_serializer = createSerializer(nested_type, column_name, nested_capnp_type, settings);
            }
            else
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert column \"{}\": Nullable can be represented only as a named union of type Void and nested type."
                    "Given CapnProto union doesn't have field with type Void: {}",
                    column_name,
                    getCapnProtoFullTypeName(capnp_type));
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder * field_builder, size_t row_num) override
        {
            assert(field_builder);
            auto & struct_builder = assert_cast<StructBuilder &>(*field_builder);
            const auto & nullable_column = assert_cast<const ColumnNullable &>(*column);
            if (nullable_column.isNullAt(row_num))
            {
                struct_builder.impl.set(null_field, capnp::Void());
            }
            else
            {
                struct_builder.impl.clear(nested_field);
                const auto & nested_column = nullable_column.getNestedColumnPtr();
                auto nested_field_builder = initStructFieldBuilderIfNeeded(nested_column, row_num, struct_builder.impl, nested_field, nested_capnp_type, nested_fields_size);
                auto value = nested_serializer->writeRow(nested_column, nested_field_builder.get(), row_num);
                if (value)
                    struct_builder.impl.set(nested_field, *value);
            }

            return std::nullopt;
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto struct_reader = reader.as<capnp::DynamicStruct>();
            auto & nullable_column = assert_cast<ColumnNullable &>(column);
            auto field = *kj::_::readMaybe(struct_reader.which());
            if (field.getType().isVoid())
                nullable_column.insertDefault();
            else
            {
                auto & nested_column = nullable_column.getNestedColumn();
                auto nested_reader = struct_reader.get(field);
                nested_serializer->readRow(nested_column, nested_reader);
                nullable_column.getNullMapData().push_back(0);
            }
        }

    private:
        std::unique_ptr<ICapnProtoSerializer> nested_serializer;
        capnp::StructSchema::Field null_field;
        capnp::StructSchema::Field nested_field;
        size_t nested_fields_size = 0;
        capnp::Type nested_capnp_type;
    };

    class CapnProtoArraySerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoArraySerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type, const FormatSettings::CapnProto & settings)
        {
            if (!capnp_type.isList())
                throwCannotConvert(data_type, column_name, capnp_type);

            auto nested_type = assert_cast<const DataTypeArray *>(data_type.get())->getNestedType();
            element_type = capnp_type.asList().getElementType();
            if (element_type.isStruct())
                element_struct_fields = element_type.asStruct().getFields().size();
            nested_serializer = createSerializer(nested_type, column_name, capnp_type.asList().getElementType(), settings);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder * field_builder, size_t row_num) override
        {
            assert(field_builder);
            auto & list_builder = assert_cast<ListBuilder &>(*field_builder);
            const auto * array_column = assert_cast<const ColumnArray *>(column.get());
            const auto & nested_column = array_column->getDataPtr();
            const auto & offsets = array_column->getOffsets();
            auto offset = offsets[row_num - 1];
            size_t size = offsets[row_num] - offset;
            bool need_nested_builders = list_builder.nested_builders.empty();
            for (unsigned i = 0; i != static_cast<unsigned>(size); ++i)
            {
                if (need_nested_builders)
                {
                    /// For nested lists we need to initialize nested list builder.
                    if (element_type.isList())
                    {
                        const auto & nested_offset = checkAndGetColumn<ColumnArray>(*nested_column)->getOffsets();
                        size_t nested_array_size = nested_offset[offset + i] - nested_offset[offset + i - 1];
                        list_builder.nested_builders.emplace_back(std::make_unique<ListBuilder>(list_builder.impl.init(i, static_cast<unsigned>(nested_array_size))));
                    }
                    else if (element_type.isStruct())
                    {
                        list_builder.nested_builders.emplace_back(std::make_unique<StructBuilder>(list_builder.impl[i], element_struct_fields));
                    }
                    else
                    {
                        list_builder.nested_builders.emplace_back();
                    }
                }

                auto value = nested_serializer->writeRow(nested_column, list_builder.nested_builders[i].get(), offset + i);
                if (value)
                    list_builder.impl.set(i, *value);
            }

            return std::nullopt;
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto list_reader = reader.as<capnp::DynamicList>();
            auto & column_array = assert_cast<ColumnArray &>(column);
            auto & offsets = column_array.getOffsets();
            offsets.push_back(offsets.back() + list_reader.size());

            auto & nested_column = column_array.getData();
            for (const auto & nested_reader : list_reader)
                nested_serializer->readRow(nested_column, nested_reader);
        }

    private:
        std::unique_ptr<ICapnProtoSerializer> nested_serializer;
        capnp::Type element_type;
        size_t element_struct_fields;
    };

    class CapnProtoMapSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoMapSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type, const FormatSettings::CapnProto & settings)
        {
            /// We output/input Map type as follow CapnProto schema
            ///
            /// struct Map {
            ///     struct Entry {
            ///         key @0: Key;
            ///         value @1: Value;
            ///     }
            ///     entries @0 :List(Entry);
            /// }

            if (!capnp_type.isStruct())
                throwCannotConvert(data_type, column_name, capnp_type);

            auto struct_schema = capnp_type.asStruct();

            if (checkIfStructContainsUnnamedUnion(struct_schema))
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert ClickHouse column \"{}\" with type {} to CapnProto Struct with unnamed union {}",
                    column_name,
                    data_type->getName(),
                    getCapnProtoFullTypeName(capnp_type));

            if (struct_schema.getFields().size() != 1)
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert ClickHouse column \"{}\": Map type can be represented as a Struct with one list field, got struct: {}",
                    column_name,
                    getCapnProtoFullTypeName(capnp_type));

            const auto & field_type = struct_schema.getFields()[0].getType();
            if (!field_type.isList())
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert ClickHouse column \"{}\": Map type can be represented as a Struct with one list field, got field: {}",
                    column_name,
                    getCapnProtoFullTypeName(field_type));

            auto list_element_type = field_type.asList().getElementType();
            if (!list_element_type.isStruct())
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert ClickHouse column \"{}\": Field of struct that represents Map should be a list of structs, got list of {}",
                    column_name,
                    getCapnProtoFullTypeName(list_element_type));

            auto key_value_struct = list_element_type.asStruct();
            if (checkIfStructContainsUnnamedUnion(key_value_struct))
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert ClickHouse column \"{}\": struct that represents Map entries is unnamed union: {}",
                    column_name,
                    getCapnProtoFullTypeName(list_element_type));

            if (key_value_struct.getFields().size() != 2)
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert ClickHouse column \"{}\": struct that represents Map entries should contain only 2 fields, got struct {}",
                    column_name,
                    getCapnProtoFullTypeName(list_element_type));

            const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
            DataTypes types = {map_type.getKeyType(), map_type.getValueType()};
            Names names = {"key", "value"};
            auto entries_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(types, names));
            entries_field = struct_schema.getFields()[0];
            entries_capnp_type = entries_field.getType();
            nested_serializer = createSerializer(entries_type, column_name, field_type, settings);
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder * field_builder, size_t row_num) override
        {
            assert(field_builder);
            auto & struct_builder = assert_cast<StructBuilder &>(*field_builder);
            const auto & entries_column = assert_cast<const ColumnMap *>(column.get())->getNestedColumnPtr();
            auto entries_builder = initStructFieldBuilderIfNeeded(entries_column, row_num, struct_builder.impl, entries_field, entries_capnp_type, 0);
            nested_serializer->writeRow(entries_column, entries_builder.get(), row_num);
            return std::nullopt;
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto struct_reader = reader.as<capnp::DynamicStruct>();
            auto & entries_column = assert_cast<ColumnMap &>(column).getNestedColumn();
            nested_serializer->readRow(entries_column, struct_reader.get(entries_field));
        }

    private:
        std::unique_ptr<ICapnProtoSerializer> nested_serializer;
        capnp::StructSchema::Field entries_field;
        capnp::Type entries_capnp_type;
    };

    class CapnProtoStructureSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoStructureSerializer(const DataTypes & data_types, const Names & names, const capnp::StructSchema & schema, const FormatSettings::CapnProto & settings)
        {
            initialize(data_types, names, schema, settings);
        }

        CapnProtoStructureSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type, const FormatSettings::CapnProto & settings)
        {
            if (!capnp_type.isStruct())
                throwCannotConvert(data_type, column_name, capnp_type);

            auto struct_schema = capnp_type.asStruct();

            if (checkIfStructIsNamedUnion(struct_schema) || checkIfStructContainsUnnamedUnion(struct_schema))
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert ClickHouse column \"{}\" with type {} to CapnProto named union/struct with unnamed union {}",
                    column_name,
                    data_type->getName(),
                    getCapnProtoFullTypeName(capnp_type));

            const auto * tuple_data_type = assert_cast<const DataTypeTuple *>(data_type.get());
            auto nested_types = tuple_data_type->getElements();
            Names nested_names;
            bool have_explicit_names = tuple_data_type->haveExplicitNames();
            auto structure_fields = struct_schema.getFields();
            if (!have_explicit_names)
            {
                if (nested_types.size() != structure_fields.size())
                    throw Exception(
                        ErrorCodes::CAPN_PROTO_BAD_CAST,
                        "Cannot convert ClickHouse column \"{}\" with type {} to CapnProto type {}: Tuple and Struct have different sizes {} != {}",
                        column_name,
                        data_type->getName(),
                        getCapnProtoFullTypeName(capnp_type),
                        nested_types.size(),
                        structure_fields.size());
                nested_names.reserve(structure_fields.size());
                for (auto field : structure_fields)
                    nested_names.push_back(field.getProto().getName());
            }
            else
            {
                nested_names = tuple_data_type->getElementNames();
            }

            try
            {
                initialize(nested_types, nested_names, struct_schema, settings);
            }
            catch (Exception & e)
            {
                e.addMessage("(while converting column {})", column_name);
                throw e;
            }
        }

        std::optional<capnp::DynamicValue::Reader> writeRow(const ColumnPtr & column, FieldBuilder * builder, size_t row_num) override
        {
            assert(builder);
            auto & struct_builder = assert_cast<StructBuilder &>(*builder);
            if (auto tuple_column = typeid_cast<const ColumnTuple *>(column.get()))
                writeRow(tuple_column->getColumnsCopy(), struct_builder, row_num);
            else
                writeRow(Columns{column}, struct_builder, row_num);
            return std::nullopt;
        }

        void writeRow(const Columns & columns, StructBuilder & struct_builder, size_t row_num)
        {
            for (size_t i = 0; i != columns.size(); ++i)
            {
                const auto & field = fields[i];
                size_t field_index = field.getIndex();
                if (likely(!struct_builder.field_builders[field_index]))
                    struct_builder.field_builders[field_index] = initStructFieldBuilderIfNeeded(
                        columns[i], row_num, struct_builder.impl, field, fields_types[i], nested_field_sizes[i]);

                auto value = field_serializers[i]->writeRow(columns[i], struct_builder.field_builders[field_index].get(), row_num);
                if (value)
                    struct_builder.impl.set(field, *value);
            }
        }

        void readRow(IColumn & column, const capnp::DynamicValue::Reader & reader) override
        {
            auto struct_reader = reader.as<capnp::DynamicStruct>();
            if (auto tuple_column = typeid_cast<ColumnTuple *>(&column))
            {
                for (size_t i = 0; i != tuple_column->tupleSize(); ++i)
                    field_serializers[i]->readRow(tuple_column->getColumn(i), struct_reader.get(fields[i]));
            }
            else
                field_serializers[0]->readRow(column, struct_reader.get(fields[0]));
        }

        void readRow(MutableColumns & columns, const capnp::DynamicStruct::Reader & reader)
        {
            for (size_t i = 0; i != columns.size(); ++i)
                field_serializers[i]->readRow(*columns[i], reader.get(fields[i]));
        }

    private:
        void initialize(const DataTypes & data_types, const Names & names, const capnp::StructSchema & schema, const FormatSettings::CapnProto & settings)
        {
            field_serializers.reserve(data_types.size());
            fields.reserve(data_types.size());
            fields_types.reserve(data_types.size());
            nested_field_sizes.reserve(data_types.size());
            for (size_t i = 0; i != data_types.size(); ++i)
            {
                auto [field_name, _] = splitFieldName(names[i]);
                auto field = findFieldByName(schema, field_name);
                if (!field)
                    throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Capnproto schema doesn't contain field with name {}", field_name);

                fields.push_back(*field);
                auto capnp_type = field->getType();
                fields_types.push_back(capnp_type);
                nested_field_sizes.push_back(capnp_type.isStruct() ? capnp_type.asStruct().getFields().size() : 0);
                field_serializers.push_back(createSerializer(data_types[i], names[i], capnp_type, settings));
            }
        }

        std::vector<std::unique_ptr<ICapnProtoSerializer>> field_serializers;
        std::vector<capnp::StructSchema::Field> fields;
        std::vector<size_t> nested_field_sizes;
        std::vector<capnp::Type> fields_types;
    };

    std::unique_ptr<ICapnProtoSerializer> createSerializer(const DataTypePtr & type, const String & name, const capnp::Type & capnp_type, const FormatSettings::CapnProto & settings)
    {
        auto [field_name, nested_name] = splitFieldName(name);
        if (!nested_name.empty() && !capnp_type.isList())
        {
            if (!capnp_type.isStruct())
                throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Field {} is not a struct", field_name);

            return std::make_unique<CapnProtoStructureSerializer>(DataTypes{type}, Names{nested_name}, capnp_type.asStruct(), settings);
        }

        switch (type->getTypeId())
        {
            case TypeIndex::Int8:
                return createIntegerSerializer<Int8>(type, name, capnp_type);
            case TypeIndex::UInt8:
                if (isBool(type))
                    return createIntegerSerializer<UInt8, true>(type, name, capnp_type);
                return createIntegerSerializer<UInt8>(type, name, capnp_type);
            case TypeIndex::Int16:
                return createIntegerSerializer<Int16>(type, name, capnp_type);
            case TypeIndex::UInt16:
                return createIntegerSerializer<UInt16>(type, name, capnp_type);
            case TypeIndex::Int32:
                return createIntegerSerializer<Int32>(type, name, capnp_type);
            case TypeIndex::UInt32:
                return createIntegerSerializer<UInt32>(type, name, capnp_type);
            case TypeIndex::Int64:
                return createIntegerSerializer<Int64>(type, name, capnp_type);
            case TypeIndex::UInt64:
                return createIntegerSerializer<UInt64>(type, name, capnp_type);
            case TypeIndex::Int128:
                return std::make_unique<CapnProtoBigIntegerSerializer<Int128>>(type, name, capnp_type);
            case TypeIndex::UInt128:
                return std::make_unique<CapnProtoBigIntegerSerializer<UInt128>>(type, name, capnp_type);
            case TypeIndex::Int256:
                return std::make_unique<CapnProtoBigIntegerSerializer<Int256>>(type, name, capnp_type);
            case TypeIndex::UInt256:
                return std::make_unique<CapnProtoBigIntegerSerializer<UInt256>>(type, name, capnp_type);
            case TypeIndex::Float32:
                return std::make_unique<CapnProtoFloatSerializer<Float32>>(type, name, capnp_type);
            case TypeIndex::Float64:
                return std::make_unique<CapnProtoFloatSerializer<Float64>>(type, name, capnp_type);
            case TypeIndex::Date:
                return std::make_unique<CapnProtoDateSerializer>(type, name, capnp_type);
            case TypeIndex::Date32:
                return std::make_unique<CapnProtoDate32Serializer>(type, name, capnp_type);
            case TypeIndex::DateTime:
                return std::make_unique<CapnProtoDateTimeSerializer>(type, name, capnp_type);
            case TypeIndex::DateTime64:
                return std::make_unique<CapnProtoDateTime64Serializer>(type, name, capnp_type);
            case TypeIndex::Decimal32:
                return std::make_unique<CapnProtoDecimalSerializer<Decimal32>>(type, name, capnp_type);
            case TypeIndex::Decimal64:
                return std::make_unique<CapnProtoDecimalSerializer<Decimal64>>(type, name, capnp_type);
            case TypeIndex::Decimal128:
                return std::make_unique<CapnProtoBigDecimalSerializer<Decimal128>>(type, name, capnp_type);
            case TypeIndex::Decimal256:
                return std::make_unique<CapnProtoBigDecimalSerializer<Decimal256>>(type, name, capnp_type);
            case TypeIndex::IPv4:
                return std::make_unique<CapnProtoIPv4Serializer>(type, name, capnp_type);
            case TypeIndex::IPv6:
                return std::make_unique<CapnProtoIPv6Serializer>(type, name, capnp_type);
            case TypeIndex::UUID:
                return std::make_unique<CapnProtoUUIDSerializer>(type, name, capnp_type);
            case TypeIndex::Enum8:
                return std::make_unique<CapnProtoEnumSerializer<Int8>>(type, name, capnp_type, settings.enum_comparing_mode);
            case TypeIndex::Enum16:
                return std::make_unique<CapnProtoEnumSerializer<Int16>>(type, name, capnp_type, settings.enum_comparing_mode);
            case TypeIndex::String:
                if (capnp_type.isData())
                    return std::make_unique<CapnProtoStringSerializer<true>>(type, name, capnp_type);
                return std::make_unique<CapnProtoStringSerializer<false>>(type, name, capnp_type);
            case TypeIndex::FixedString:
                if (capnp_type.isData())
                    return std::make_unique<CapnProtoFixedStringSerializer<true>>(type, name, capnp_type);
                return std::make_unique<CapnProtoFixedStringSerializer<false>>(type, name, capnp_type);
            case TypeIndex::LowCardinality:
                return std::make_unique<CapnProtoLowCardinalitySerializer>(type, name, capnp_type, settings);
            case TypeIndex::Nullable:
                return std::make_unique<CapnProtoNullableSerializer>(type, name, capnp_type, settings);
            case TypeIndex::Array:
                return std::make_unique<CapnProtoArraySerializer>(type, name, capnp_type, settings);
            case TypeIndex::Map:
                return std::make_unique<CapnProtoMapSerializer>(type, name, capnp_type, settings);
            case TypeIndex::Tuple:
                return std::make_unique<CapnProtoStructureSerializer>(type, name, capnp_type, settings);
            default:
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported in CapnProto format", type->getName());
        }
    }
}

class CapnProtoSerializer::Impl
{
public:
    Impl(const DataTypes & data_types, const Names & names, const capnp::StructSchema & schema, const FormatSettings::CapnProto & settings)
        : struct_serializer(std::make_unique<CapnProtoStructureSerializer>(data_types, names, schema, settings))
        , fields_size(schema.getFields().size())
    {
    }

    void writeRow(const Columns & columns, capnp::DynamicStruct::Builder builder, size_t row_num)
    {
        StructBuilder struct_builder(std::move(builder), fields_size);
        struct_serializer->writeRow(columns, struct_builder, row_num);
    }

    void readRow(MutableColumns & columns, capnp::DynamicStruct::Reader & reader)
    {
        struct_serializer->readRow(columns, reader);
    }

private:
    std::unique_ptr<CapnProtoStructureSerializer> struct_serializer;
    size_t fields_size;
};

CapnProtoSerializer::CapnProtoSerializer(const DataTypes & data_types, const Names & names, const capnp::StructSchema & schema, const FormatSettings::CapnProto & settings)
    : serializer_impl(std::make_unique<Impl>(data_types, names, schema, settings))
{
}

void CapnProtoSerializer::writeRow(const Columns & columns, capnp::DynamicStruct::Builder builder, size_t row_num)
{
    serializer_impl->writeRow(columns, std::move(builder), row_num);
}

void CapnProtoSerializer::readRow(MutableColumns & columns, capnp::DynamicStruct::Reader & reader)
{
    serializer_impl->readRow(columns, reader);
}

CapnProtoSerializer::~CapnProtoSerializer() = default;

}
