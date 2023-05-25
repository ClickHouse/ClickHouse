#include "config.h"

#if USE_CAPNP

#include <Formats/CapnProtoSerializer.h>
#include <Formats/FormatSettings.h>
#include <Formats/CapnProtoSchema.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
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
    extern const int LOGICAL_ERROR;
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
        explicit ListBuilder(capnp::DynamicValue::Builder builder, UInt32 elements_size) : impl(builder.as<capnp::DynamicList>()), nested_builders(elements_size)
        {
        }

        capnp::DynamicList::Builder impl;
        std::vector<std::unique_ptr<FieldBuilder>> nested_builders;
    };

    struct StructBuilder : public FieldBuilder
    {
        explicit StructBuilder(capnp::DynamicStruct::Builder struct_builder, size_t fields_size) : impl(std::move(struct_builder)), field_builders(fields_size)
        {
        }

        capnp::DynamicStruct::Builder impl;
        std::vector<std::unique_ptr<FieldBuilder>> field_builders;
    };

    class ICapnProtoSerializer
    {
    public:
        virtual void writeRow(
            const ColumnPtr & column,
            std::unique_ptr<FieldBuilder> & builder,
            capnp::DynamicStruct::Builder & parent_struct_builder,
            UInt32 slot_offset,
            size_t row_num) = 0;

        virtual void writeRow(
            const ColumnPtr & column,
            std::unique_ptr<FieldBuilder> & builder,
            capnp::DynamicList::Builder & parent_list_builder,
            UInt32 array_index,
            size_t row_num) = 0;

        virtual void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) = 0;

        virtual void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) = 0;

        virtual ~ICapnProtoSerializer() = default;
    };

    template <typename CHNumericType, typename CapnProtoNumericType, bool convert_to_bool_on_read>
    class CapnProtoIntegerSerializer : public ICapnProtoSerializer
    {
    public:
        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            CapnProtoNumericType value = static_cast<CapnProtoNumericType>(assert_cast<const ColumnVector<CHNumericType> &>(*column).getElement(row_num));
            builder_impl.setDataField<CapnProtoNumericType>(slot_offset, value);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_list_builder.getBuilderImpl();
            CapnProtoNumericType value = static_cast<CapnProtoNumericType>(assert_cast<const ColumnVector<CHNumericType> &>(*column).getElement(row_num));
            builder_impl.setDataElement<CapnProtoNumericType>(array_index, value);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            CapnProtoNumericType value = reader_impl.getDataField<CapnProtoNumericType>(slot_offset);
            if constexpr (convert_to_bool_on_read)
                assert_cast<ColumnUInt8 &>(column).insertValue(static_cast<bool>(value));
            else
                assert_cast<ColumnVector<CHNumericType> &>(column).insertValue(static_cast<CHNumericType>(value));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            CapnProtoNumericType value = reader_impl.getDataElement<CapnProtoNumericType>(array_index);
            if constexpr (convert_to_bool_on_read)
                assert_cast<ColumnUInt8 &>(column).insertValue(static_cast<bool>(value));
            else
                assert_cast<ColumnVector<CHNumericType> &>(column).insertValue(static_cast<CHNumericType>(value));
        }
    };

    template <typename NumericType, bool convert_to_bool_on_read = false>
    std::unique_ptr<ICapnProtoSerializer> createIntegerSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
    {
        switch (capnp_type.which())
        {
            case capnp::schema::Type::INT8:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, Int8, convert_to_bool_on_read>>();
            case capnp::schema::Type::INT16:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, Int16, convert_to_bool_on_read>>();
            case capnp::schema::Type::INT32:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, Int32, convert_to_bool_on_read>>();
            case capnp::schema::Type::INT64:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, Int64, convert_to_bool_on_read>>();
            case capnp::schema::Type::UINT8:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, UInt8, convert_to_bool_on_read>>();
            case capnp::schema::Type::UINT16:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, UInt16, convert_to_bool_on_read>>();
            case capnp::schema::Type::UINT32:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, UInt32, convert_to_bool_on_read>>();
            case capnp::schema::Type::UINT64:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, UInt64, convert_to_bool_on_read>>();
            case capnp::schema::Type::BOOL:
                return std::make_unique<CapnProtoIntegerSerializer<NumericType, bool, convert_to_bool_on_read>>();
            default:
                throwCannotConvert(data_type, column_name, capnp_type);
        }
    }

    template <typename CHFloatType, typename CapnProtoFloatType>
    class CapnProtoFloatSerializer : public ICapnProtoSerializer
    {
    public:
        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            CapnProtoFloatType value = static_cast<CapnProtoFloatType>(assert_cast<const ColumnVector<CHFloatType> &>(*column).getElement(row_num));
            builder_impl.setDataField<CapnProtoFloatType>(slot_offset, value);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_list_builder.getBuilderImpl();
            CapnProtoFloatType value = static_cast<CapnProtoFloatType>(assert_cast<const ColumnVector<CHFloatType> &>(*column).getElement(row_num));
            builder_impl.setDataElement<CapnProtoFloatType>(array_index, value);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            CapnProtoFloatType value = reader_impl.getDataField<CapnProtoFloatType>(slot_offset);
            assert_cast<ColumnVector<CHFloatType> &>(column).insertValue(static_cast<CHFloatType>(value));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            CapnProtoFloatType value = reader_impl.getDataElement<CapnProtoFloatType>(array_index);
            assert_cast<ColumnVector<CHFloatType> &>(column).insertValue(static_cast<CHFloatType>(value));
        }
    };

    template <typename FloatType>
    std::unique_ptr<ICapnProtoSerializer> createFloatSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
    {
        switch (capnp_type.which())
        {
            case capnp::schema::Type::FLOAT32:
                return std::make_unique<CapnProtoFloatSerializer<FloatType, Float32>>();
            case capnp::schema::Type::FLOAT64:
                return std::make_unique<CapnProtoFloatSerializer<FloatType, Float64>>();
            default:
                throwCannotConvert(data_type, column_name, capnp_type);
        }
    }

    template <typename EnumType>
    class CapnProtoEnumSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoEnumSerializer(
            const DataTypePtr & data_type_,
            const String & column_name,
            const capnp::Type & capnp_type,
            const FormatSettings::CapnProtoEnumComparingMode enum_comparing_mode_) : data_type(data_type_), enum_comparing_mode(enum_comparing_mode_)
        {
            if (!capnp_type.isEnum())
                throwCannotConvert(data_type, column_name, capnp_type);

            bool to_lower = enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_NAMES_CASE_INSENSITIVE;
            const auto * enum_type = assert_cast<const DataTypeEnum<EnumType> *>(data_type.get());
            const auto & enum_values = dynamic_cast<const EnumValues<EnumType> &>(*enum_type);

            enum_schema = capnp_type.asEnum();
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
                auto all_values = enum_values.getValues();
                if (all_values.size() != enumerants.size())
                    throw Exception(
                        ErrorCodes::CAPN_PROTO_BAD_CAST,
                        "The set of names in Enum from CapnProto schema is different from the set of names in ClickHouse Enum");

                std::unordered_map<String, EnumType> ch_name_to_value;
                for (auto & [name, value] : all_values)
                    ch_name_to_value[to_lower ? boost::algorithm::to_lower_copy(name) : name] = value;

                for (auto enumerant : enumerants)
                {
                    String capnp_name = enumerant.getProto().getName();
                    UInt16 capnp_value = enumerant.getOrdinal();
                    auto it = ch_name_to_value.find(to_lower ? boost::algorithm::to_lower_copy(capnp_name) : capnp_name);
                    if (it == ch_name_to_value.end())
                        throw Exception(
                            ErrorCodes::CAPN_PROTO_BAD_CAST,
                            "The set of names in Enum from CapnProto schema is different from the set of names in ClickHouse Enum");

                    ch_to_capnp_values[it->second] = capnp_value;
                    capnp_to_ch_values[capnp_value] = it->second;
                }
            }
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            EnumType enum_value = assert_cast<const ColumnVector<EnumType> &>(*column).getElement(row_num);
            UInt16 capnp_value;
            if (enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_VALUES)
                capnp_value = static_cast<UInt16>(enum_value);
            else
                capnp_value = ch_to_capnp_values[enum_value];

            builder_impl.setDataField<UInt16>(slot_offset, capnp_value);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_list_builder.getBuilderImpl();
            EnumType enum_value = assert_cast<const ColumnVector<EnumType> &>(*column).getElement(row_num);
            UInt16 capnp_value;
            if (enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_VALUES)
                capnp_value = static_cast<UInt16>(enum_value);
            else
                capnp_value = ch_to_capnp_values[enum_value];

            builder_impl.setDataElement<UInt16>(array_index, capnp_value);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            UInt16 capnp_value = reader_impl.getDataField<UInt16>(slot_offset);
            EnumType value;
            if (enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_VALUES)
                value = static_cast<EnumType>(capnp_value);
            else
                value = capnp_to_ch_values[capnp_value];
            
            assert_cast<ColumnVector<EnumType> &>(column).insertValue(value);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            UInt16 capnp_value = reader_impl.getDataElement<UInt16>(array_index);
            EnumType value;
            if (enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_VALUES)
                value = static_cast<EnumType>(capnp_value);
            else
                value = capnp_to_ch_values[capnp_value];
            
            assert_cast<ColumnVector<EnumType> &>(column).insertValue(value);
        }

    private:
        DataTypePtr data_type;
        capnp::EnumSchema enum_schema;
        const FormatSettings::CapnProtoEnumComparingMode enum_comparing_mode;
        std::unordered_map<EnumType, UInt16> ch_to_capnp_values;
        std::unordered_map<UInt16, EnumType> capnp_to_ch_values;
    };

    class CapnProtoDateSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoDateSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            if (!capnp_type.isUInt16())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            UInt16 value = assert_cast<const ColumnDate &>(*column).getElement(row_num);
            builder_impl.setDataField<UInt16>(slot_offset, value);
        }
        
        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_list_builder.getBuilderImpl();
            UInt16 value = assert_cast<const ColumnDate &>(*column).getElement(row_num);
            builder_impl.setDataElement<UInt16>(array_index, value);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            UInt16 value = reader_impl.getDataField<UInt16>(slot_offset);
            assert_cast<ColumnDate &>(column).insertValue(value);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            UInt16 value = reader_impl.getDataElement<UInt16>(array_index);
            assert_cast<ColumnDate &>(column).insertValue(value);
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

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            Int32 value = assert_cast<const ColumnDate32 &>(*column).getElement(row_num);
            builder_impl.setDataField<Int32>(slot_offset, value);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_list_builder.getBuilderImpl();
            Int32 value = assert_cast<const ColumnDate32 &>(*column).getElement(row_num);
            builder_impl.setDataElement<Int32>(array_index, value);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            Int32 value = reader_impl.getDataField<Int32>(slot_offset);
            assert_cast<ColumnDate32 &>(column).insertValue(value);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            Int32 value = reader_impl.getDataElement<Int32>(array_index);
            assert_cast<ColumnDate32 &>(column).insertValue(value);
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

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            UInt32 value = assert_cast<const ColumnDateTime &>(*column).getElement(row_num);
            builder_impl.setDataField<UInt32>(slot_offset, value);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_list_builder.getBuilderImpl();
            UInt32 value = assert_cast<const ColumnDateTime &>(*column).getElement(row_num);
            builder_impl.setDataElement<UInt32>(array_index, value);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            UInt32 value = reader_impl.getDataField<UInt32>(slot_offset);
            assert_cast<ColumnDateTime &>(column).insertValue(value);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            UInt32 value = reader_impl.getDataElement<UInt32>(array_index);
            assert_cast<ColumnDateTime &>(column).insertValue(value);
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

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            Int64 value = assert_cast<const ColumnDateTime64 &>(*column).getElement(row_num);
            builder_impl.setDataField<Int64>(slot_offset, value);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_list_builder.getBuilderImpl();
            Int64 value = assert_cast<const ColumnDateTime64 &>(*column).getElement(row_num);
            builder_impl.setDataElement<Int64>(array_index, value);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            Int64 value = reader_impl.getDataField<Int64>(slot_offset);
            assert_cast<ColumnDateTime64 &>(column).insertValue(value);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            Int64 value = reader_impl.getDataElement<Int64>(array_index);
            assert_cast<ColumnDateTime64 &>(column).insertValue(value);
        }
    };

    template <typename DecimalType>
    class CapnProtoDecimalSerializer : public ICapnProtoSerializer
    {
    public:
        using NativeType = typename DecimalType::NativeType;

        CapnProtoDecimalSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            auto which = WhichDataType(data_type);
            if ((!capnp_type.isInt32() && which.isDecimal32()) || (!capnp_type.isInt64() && which.isDecimal64()))
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            DecimalType value = assert_cast<const ColumnDecimal<DecimalType> &>(*column).getElement(row_num);
            builder_impl.setDataField<NativeType>(slot_offset, value);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_list_builder.getBuilderImpl();
            DecimalType value = assert_cast<const ColumnDecimal<DecimalType> &>(*column).getElement(row_num);
            builder_impl.setDataElement<NativeType>(array_index, value);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            NativeType value = reader_impl.getDataField<NativeType>(slot_offset);
            assert_cast<ColumnDecimal<DecimalType> &>(column).insertValue(value);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            NativeType value = reader_impl.getDataElement<NativeType>(array_index);
            assert_cast<ColumnDecimal<DecimalType> &>(column).insertValue(value);
        }
    };

    template <typename T>
    class CapnProtoFixedSizeRawDataSerializer : public ICapnProtoSerializer
    {
    private:
        static constexpr size_t value_size = sizeof(T);

    public:
        CapnProtoFixedSizeRawDataSerializer(const DataTypePtr & data_type_, const String & column_name, const capnp::Type & capnp_type) : data_type(data_type_)
        {
            if (!capnp_type.isData())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            auto data = column->getDataAt(row_num);
            capnp::Data::Reader value = capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size);
            builder_impl.getPointerField(slot_offset).template setBlob<capnp::Data>(value);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_struct_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            auto data = column->getDataAt(row_num);
            capnp::Data::Reader value = capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size);
            builder_impl.getPointerElement(array_index).setBlob<capnp::Data>(value);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            capnp::Data::Reader value = reader_impl.getPointerField(slot_offset).template getBlob<capnp::Data>(nullptr, 0);
            if (value.size() != value_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size of {} value: {}", data_type->getName(), value.size());

            column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            capnp::Data::Reader value = reader_impl.getPointerElement(array_index).getBlob<capnp::Data>(nullptr, 0);
            if (value.size() != value_size)
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
        CapnProtoStringSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type)
        {
            if (!capnp_type.isData() && !capnp_type.isText())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            auto data = column->getDataAt(row_num);
            if constexpr (is_binary)
            {
                capnp::Data::Reader value = capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size);
                builder_impl.getPointerField(slot_offset).setBlob<capnp::Data>(value);
            }
            else
            {
                capnp::Text::Reader value = capnp::Text::Reader(data.data, data.size);
                builder_impl.getPointerField(slot_offset).setBlob<capnp::Text>(value);
            }
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_struct_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            auto data = column->getDataAt(row_num);
            if constexpr (is_binary)
            {
                capnp::Data::Reader value = capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size);
                builder_impl.getPointerElement(array_index).setBlob<capnp::Data>(value);
            }
            else
            {
                capnp::Text::Reader value = capnp::Text::Reader(data.data, data.size);
                builder_impl.getPointerElement(array_index).setBlob<capnp::Text>(value);
            }
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            if constexpr (is_binary)
            {
                capnp::Data::Reader value = reader_impl.getPointerField(slot_offset).getBlob<capnp::Data>(nullptr, 0);
                column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
            else
            {
                capnp::Text::Reader value = reader_impl.getPointerField(slot_offset).getBlob<capnp::Text>(nullptr, 0);
                column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            if constexpr (is_binary)
            {
                capnp::Data::Reader value = reader_impl.getPointerElement(array_index).getBlob<capnp::Data>(nullptr, 0);
                column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
            else
            {
                capnp::Text::Reader value = reader_impl.getPointerElement(array_index).getBlob<capnp::Text>(nullptr, 0);
                column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
        }
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

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            auto data = column->getDataAt(row_num);
            if constexpr (is_binary)
            {
                capnp::Data::Reader value = capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size);
                builder_impl.getPointerField(slot_offset).setBlob<capnp::Data>(value);
            }
            else
            {
                if (data.data[data.size - 1] == 0)
                {
                    capnp::Text::Reader value = capnp::Text::Reader(data.data, data.size);
                    builder_impl.getPointerField(slot_offset).setBlob<capnp::Text>(value);
                }
                else
                {
                    /// In TEXT type data should be null-terminated, but ClickHouse FixedString data could not be.
                    /// To make data null-terminated we should copy it to temporary String object and use it in capnp::Text::Reader.
                    /// Note that capnp::Text::Reader works only with pointer to the data and it's size, so we should
                    /// guarantee that new String object life time is longer than capnp::Text::Reader life time.
                    tmp_string = data.toString();
                    capnp::Text::Reader value = capnp::Text::Reader(tmp_string.data(), tmp_string.size());
                    builder_impl.getPointerField(slot_offset).setBlob<capnp::Text>(value);
                }
            }
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_struct_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            auto data = column->getDataAt(row_num);
            if constexpr (is_binary)
            {
                capnp::Data::Reader value = capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size);
                builder_impl.getPointerElement(array_index).setBlob<capnp::Data>(value);
            }
            else
            {
                if (data.data[data.size - 1] == 0)
                {
                    capnp::Text::Reader value = capnp::Text::Reader(data.data, data.size);
                    builder_impl.getPointerElement(array_index).setBlob<capnp::Text>(value);
                }
                else
                {
                    /// In TEXT type data should be null-terminated, but ClickHouse FixedString data could not be.
                    /// To make data null-terminated we should copy it to temporary String object and use it in capnp::Text::Reader.
                    /// Note that capnp::Text::Reader works only with pointer to the data and it's size, so we should
                    /// guarantee that new String object life time is longer than capnp::Text::Reader life time.
                    tmp_string = data.toString();
                    capnp::Text::Reader value = capnp::Text::Reader(tmp_string.data(), tmp_string.size());
                    builder_impl.getPointerElement(array_index).setBlob<capnp::Text>(value);
                }
            }
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            auto & fixed_string_column = assert_cast<ColumnFixedString &>(column);
            if constexpr (is_binary)
            {
                capnp::Data::Reader value = reader_impl.getPointerField(slot_offset).getBlob<capnp::Data>(nullptr, 0);
                if (value.size() > fixed_string_column.getN())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read data with size {} to FixedString with size {}", value.size(), fixed_string_column.getN());

                fixed_string_column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
            else
            {
                capnp::Text::Reader value = reader_impl.getPointerField(slot_offset).getBlob<capnp::Text>(nullptr, 0);
                if (value.size() > fixed_string_column.getN())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read data with size {} to FixedString with size {}", value.size(), fixed_string_column.getN());

                fixed_string_column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            auto & fixed_string_column = assert_cast<ColumnFixedString &>(column);
            if constexpr (is_binary)
            {
                capnp::Data::Reader value = reader_impl.getPointerElement(array_index).getBlob<capnp::Data>(nullptr, 0);
                if (value.size() > fixed_string_column.getN())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read data with size {} to FixedString with size {}", value.size(), fixed_string_column.getN());

                fixed_string_column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
            }
            else
            {
                capnp::Text::Reader value = reader_impl.getPointerElement(array_index).getBlob<capnp::Text>(nullptr, 0);
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

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            auto & builder_impl = parent_struct_builder.getBuilderImpl();
            UInt32 value = assert_cast<const ColumnIPv4 &>(*column).getElement(row_num);
            builder_impl.setDataField<UInt32>(slot_offset, value);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            auto & builder_impl = parent_list_builder.getBuilderImpl();
            UInt32 value = assert_cast<const ColumnIPv4 &>(*column).getElement(row_num);
            builder_impl.setDataElement<UInt32>(array_index, value);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            UInt32 value = reader_impl.getDataField<UInt32>(slot_offset);
            assert_cast<ColumnIPv4 &>(column).insertValue(IPv4(value));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            UInt32 value = reader_impl.getDataElement<UInt32>(array_index);
            assert_cast<ColumnIPv4 &>(column).insertValue(IPv4(value));
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

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            const auto & low_cardinality_column = assert_cast<const ColumnLowCardinality &>(*column);
            size_t index = low_cardinality_column.getIndexAt(row_num);
            const auto & dict_column = low_cardinality_column.getDictionary().getNestedColumn();
            nested_serializer->writeRow(dict_column, field_builder, parent_struct_builder, slot_offset, index);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            const auto & low_cardinality_column = assert_cast<const ColumnLowCardinality &>(*column);
            size_t index = low_cardinality_column.getIndexAt(row_num);
            const auto & dict_column = low_cardinality_column.getDictionary().getNestedColumn();
            nested_serializer->writeRow(dict_column, field_builder, parent_list_builder, array_index, index);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            auto & low_cardinality_column = assert_cast<ColumnLowCardinality &>(column);
            auto tmp_column = low_cardinality_column.getDictionary().getNestedColumn()->cloneEmpty();
            nested_serializer->readRow(*tmp_column, parent_struct_reader, slot_offset);
            low_cardinality_column.insertFromFullColumn(*tmp_column, 0);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            auto & low_cardinality_column = assert_cast<ColumnLowCardinality &>(column);
            auto tmp_column = low_cardinality_column.getDictionary().getNestedColumn()->cloneEmpty();
            nested_serializer->readRow(*tmp_column, parent_list_reader, array_index);
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
            struct_schema = capnp_type.asStruct();
            auto node = struct_schema.getProto().getStruct();
            struct_size = capnp::_::StructSize(node.getDataWordCount(), node.getPointerCount());
            discriminant_offset = node.getDiscriminantOffset();
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
            nested_slot_offset = first.getProto().getSlot().getOffset(); /// Both fields have the same offset.
            if (first.getType().isVoid())
            {
                nested_serializer = createSerializer(nested_type, column_name, second.getType(), settings);
                null_discriminant = 0;
                nested_discriminant = 1;
            }
            else if (second.getType().isVoid())
            {
                nested_serializer = createSerializer(nested_type, column_name, first.getType(), settings);
                null_discriminant = 1;
                nested_discriminant = 0;
            }
            else
                throw Exception(
                    ErrorCodes::CAPN_PROTO_BAD_CAST,
                    "Cannot convert column \"{}\": Nullable can be represented only as a named union of type Void and nested type."
                    "Given CapnProto union doesn't have field with type Void: {}",
                    column_name,
                    getCapnProtoFullTypeName(capnp_type));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            if (!field_builder)
            {
                auto builder_impl = parent_struct_builder.getBuilderImpl();
                auto struct_builder_impl = capnp::DynamicStruct::Builder(struct_schema, builder_impl.getPointerField(slot_offset).initStruct(struct_size));
                field_builder = std::make_unique<StructBuilder>(std::move(struct_builder_impl), 1);
            }

            auto & struct_builder = assert_cast<StructBuilder &>(*field_builder);

            const auto & nullable_column = assert_cast<const ColumnNullable &>(*column);
            if (nullable_column.isNullAt(row_num))
            {
                auto struct_builder_impl = struct_builder.impl.getBuilderImpl();
                struct_builder_impl.setDataField<uint16_t>(discriminant_offset, null_discriminant);
                struct_builder_impl.setDataField<capnp::Void>(nested_slot_offset, capnp::Void());
            }
            else
            {
                const auto & nested_column = nullable_column.getNestedColumnPtr();
                struct_builder.impl.getBuilderImpl().setDataField<uint16_t>(discriminant_offset, nested_discriminant);
                nested_serializer->writeRow(nested_column, struct_builder.field_builders[0], struct_builder.impl, nested_slot_offset, row_num);
            }
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            if (!field_builder)
            {
                auto builder_impl = parent_list_builder.getBuilderImpl();
                auto struct_builder_impl = capnp::DynamicStruct::Builder(struct_schema, builder_impl.getStructElement(array_index));
                field_builder = std::make_unique<StructBuilder>(std::move(struct_builder_impl), 1);
            }

            auto & struct_builder = assert_cast<StructBuilder &>(*field_builder);

            const auto & nullable_column = assert_cast<const ColumnNullable &>(*column);
            if (nullable_column.isNullAt(row_num))
            {
                auto struct_builder_impl = struct_builder.impl.getBuilderImpl();
                struct_builder_impl.setDataField<uint16_t>(discriminant_offset, null_discriminant);
                struct_builder_impl.setDataField<capnp::Void>(nested_slot_offset, capnp::Void());
            }
            else
            {
                const auto & nested_column = nullable_column.getNestedColumnPtr();
                struct_builder.impl.getBuilderImpl().setDataField<uint16_t>(discriminant_offset, nested_discriminant);
                nested_serializer->writeRow(nested_column, struct_builder.field_builders[0], struct_builder.impl, nested_slot_offset, row_num);
            }
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            auto & nullable_column = assert_cast<ColumnNullable &>(column);
            auto reader_impl = parent_struct_reader.getReaderImpl();
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, reader_impl.getPointerField(slot_offset).getStruct(nullptr));

            auto discriminant = struct_reader.getReaderImpl().getDataField<uint16_t>(discriminant_offset);

            if (discriminant == null_discriminant)
                nullable_column.insertDefault();
            else
            {
                auto & nested_column = nullable_column.getNestedColumn();
                nested_serializer->readRow(nested_column, struct_reader, nested_slot_offset);
                nullable_column.getNullMapData().push_back(0);
            }
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            auto & nullable_column = assert_cast<ColumnNullable &>(column);
            auto reader_impl = parent_list_reader.getReaderImpl();
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, reader_impl.getStructElement(array_index));

            auto discriminant = struct_reader.getReaderImpl().getDataField<uint16_t>(discriminant_offset);

            if (discriminant == null_discriminant)
                nullable_column.insertDefault();
            else
            {
                auto & nested_column = nullable_column.getNestedColumn();
                nested_serializer->readRow(nested_column, struct_reader, nested_slot_offset);
                nullable_column.getNullMapData().push_back(0);
            }
        }

    private:
        std::unique_ptr<ICapnProtoSerializer> nested_serializer;
        capnp::StructSchema struct_schema;
        capnp::_::StructSize struct_size;
        UInt32 discriminant_offset;
        UInt16 null_discriminant;
        UInt16 nested_discriminant;
        UInt32 nested_slot_offset;
    };

    class CapnProtoArraySerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoArraySerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type, const FormatSettings::CapnProto & settings)
        {
            if (!capnp_type.isList())
                throwCannotConvert(data_type, column_name, capnp_type);

            auto nested_type = assert_cast<const DataTypeArray *>(data_type.get())->getNestedType();
            list_schema = capnp_type.asList();
            auto element_type = list_schema.getElementType();
            element_size = capnp::elementSizeFor(element_type.which());
            if (element_type.isStruct())
            {
                element_is_struct = true;
                auto node = element_type.asStruct().getProto().getStruct();
                element_struct_size = capnp::_::StructSize(node.getDataWordCount(), node.getPointerCount());
            }

            nested_serializer = createSerializer(nested_type, column_name, capnp_type.asList().getElementType(), settings);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            const auto * array_column = assert_cast<const ColumnArray *>(column.get());
            const auto & nested_column = array_column->getDataPtr();
            const auto & offsets = array_column->getOffsets();
            auto offset = offsets[row_num - 1];
            UInt32 size = static_cast<UInt32>(offsets[row_num] - offset);

            if (!field_builder)
            {
                auto builder_impl = parent_struct_builder.getBuilderImpl();
                capnp::DynamicList::Builder list_builder_impl;
                if (element_is_struct)
                    list_builder_impl = capnp::DynamicList::Builder(list_schema, builder_impl.getPointerField(slot_offset).initStructList(size, element_struct_size));
                else
                    list_builder_impl = capnp::DynamicList::Builder(list_schema, builder_impl.getPointerField(slot_offset).initList(element_size, size));
                field_builder = std::make_unique<ListBuilder>(std::move(list_builder_impl), size);
            }

            auto & list_builder = assert_cast<ListBuilder &>(*field_builder);
            for (UInt32 i = 0; i != size; ++i)
                nested_serializer->writeRow(nested_column, list_builder.nested_builders[i], list_builder.impl, i, offset + i);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            const auto * array_column = assert_cast<const ColumnArray *>(column.get());
            const auto & nested_column = array_column->getDataPtr();
            const auto & offsets = array_column->getOffsets();
            auto offset = offsets[row_num - 1];
            UInt32 size = static_cast<UInt32>(offsets[row_num] - offset);

            if (!field_builder)
            {
                auto builder_impl = parent_list_builder.getBuilderImpl();
                capnp::DynamicList::Builder list_builder_impl;
                if (element_is_struct)
                    list_builder_impl = capnp::DynamicList::Builder(list_schema, builder_impl.getPointerElement(array_index).initStructList(size, element_struct_size));
                else
                    list_builder_impl = capnp::DynamicList::Builder(list_schema, builder_impl.getPointerElement(array_index).initList(element_size, size));
                field_builder = std::make_unique<ListBuilder>(std::move(list_builder_impl), size);
            }

            auto & list_builder = assert_cast<ListBuilder &>(*field_builder);
            for (UInt32 i = 0; i != size; ++i)
                nested_serializer->writeRow(nested_column, list_builder.nested_builders[i], list_builder.impl, i, offset + i);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            const auto & reader_impl = parent_struct_reader.getReaderImpl();
            auto list_reader = capnp::DynamicList::Reader(list_schema, reader_impl.getPointerField(slot_offset).getList(element_size, nullptr));
            UInt32 size = list_reader.size();
            auto & column_array = assert_cast<ColumnArray &>(column);
            auto & offsets = column_array.getOffsets();
            offsets.push_back(offsets.back() + list_reader.size());

            auto & nested_column = column_array.getData();
            for (UInt32 i = 0; i != size; ++i)
                nested_serializer->readRow(nested_column, list_reader, i);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            const auto & reader_impl = parent_list_reader.getReaderImpl();
            auto list_reader = capnp::DynamicList::Reader(list_schema, reader_impl.getPointerElement(array_index).getList(element_size, nullptr));
            UInt32 size = list_reader.size();
            auto & column_array = assert_cast<ColumnArray &>(column);
            auto & offsets = column_array.getOffsets();
            offsets.push_back(offsets.back() + list_reader.size());

            auto & nested_column = column_array.getData();
            for (UInt32 i = 0; i != size; ++i)
                nested_serializer->readRow(nested_column, list_reader, i);
        }

    private:
        capnp::ListSchema list_schema;
        std::unique_ptr<ICapnProtoSerializer> nested_serializer;
        capnp::ElementSize element_size;
        capnp::_::StructSize element_struct_size;
        bool element_is_struct = false;

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

            struct_schema = capnp_type.asStruct();
            auto node = struct_schema.getProto().getStruct();
            struct_size = capnp::_::StructSize(node.getDataWordCount(), node.getPointerCount());

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
            nested_serializer = createSerializer(entries_type, column_name, field_type, settings);
            entries_slot_offset = struct_schema.getFields()[0].getProto().getSlot().getOffset();
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            if (!field_builder)
            {
                auto builder_impl = parent_struct_builder.getBuilderImpl();
                auto struct_builder_impl = capnp::DynamicStruct::Builder(struct_schema, builder_impl.getPointerField(slot_offset).initStruct(struct_size));
                field_builder = std::make_unique<StructBuilder>(std::move(struct_builder_impl), 1);
            }

            auto & struct_builder = assert_cast<StructBuilder &>(*field_builder);
            const auto & entries_column = assert_cast<const ColumnMap *>(column.get())->getNestedColumnPtr();
            nested_serializer->writeRow(entries_column, struct_builder.field_builders[0], struct_builder.impl, entries_slot_offset, row_num);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            if (!field_builder)
            {
                auto builder_impl = parent_list_builder.getBuilderImpl();
                auto struct_builder_impl = capnp::DynamicStruct::Builder(struct_schema, builder_impl.getStructElement(array_index));
                field_builder = std::make_unique<StructBuilder>(std::move(struct_builder_impl), 1);
            }

            auto & struct_builder = assert_cast<StructBuilder &>(*field_builder);
            const auto & entries_column = assert_cast<const ColumnMap *>(column.get())->getNestedColumnPtr();
            nested_serializer->writeRow(entries_column, struct_builder.field_builders[0], struct_builder.impl, entries_slot_offset, row_num);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            auto reader_impl = parent_struct_reader.getReaderImpl();
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, reader_impl.getPointerField(slot_offset).getStruct(nullptr));
            auto & entries_column = assert_cast<ColumnMap &>(column).getNestedColumn();
            nested_serializer->readRow(entries_column, struct_reader, entries_slot_offset);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            auto reader_impl = parent_list_reader.getReaderImpl();
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, reader_impl.getStructElement(array_index));
            auto & entries_column = assert_cast<ColumnMap &>(column).getNestedColumn();
            nested_serializer->readRow(entries_column, struct_reader, entries_slot_offset);
        }

    private:
        std::unique_ptr<ICapnProtoSerializer> nested_serializer;
        capnp::StructSchema struct_schema;
        capnp::_::StructSize struct_size;
        UInt32 entries_slot_offset;
    };

    class CapnProtoStructureSerializer : public ICapnProtoSerializer
    {
    public:
        CapnProtoStructureSerializer(const DataTypes & data_types, const Names & names, const capnp::StructSchema & schema, const FormatSettings::CapnProto & settings) : struct_schema(schema)
        {
            if (checkIfStructIsNamedUnion(schema) || checkIfStructContainsUnnamedUnion(schema))
                throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Root CapnProto Struct cannot be named union/struct with unnamed union");

            initialize(data_types, names, settings);
        }

        CapnProtoStructureSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type, const FormatSettings::CapnProto & settings)
        {
            if (!capnp_type.isStruct())
                throwCannotConvert(data_type, column_name, capnp_type);

            struct_schema = capnp_type.asStruct();

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
                initialize(nested_types, nested_names, settings);
            }
            catch (Exception & e)
            {
                e.addMessage("(while converting column {})", column_name);
                throw std::move(e);
            }
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            if (!field_builder)
            {
                auto builder_impl = parent_struct_builder.getBuilderImpl();
                auto struct_builder_impl = capnp::DynamicStruct::Builder(struct_schema, builder_impl.getPointerField(slot_offset).initStruct(struct_size));
                field_builder = std::make_unique<StructBuilder>(std::move(struct_builder_impl), fields_count);
            }

            auto & struct_builder = assert_cast<StructBuilder &>(*field_builder);
            if (const auto * tuple_column = typeid_cast<const ColumnTuple *>(column.get()))
            {
                const auto & columns = tuple_column->getColumns();
                for (size_t i = 0; i != columns.size(); ++i)
                    fields_serializers[i]->writeRow(columns[i], struct_builder.field_builders[fields_indexes[i]], struct_builder.impl, fields_offsets[i], row_num);
            }
            else
            {
                fields_serializers[0]->writeRow(column, struct_builder.field_builders[fields_indexes[0]], struct_builder.impl, fields_offsets[0], row_num);
            }
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            if (!field_builder)
            {
                auto builder_impl = parent_list_builder.getBuilderImpl();
                auto struct_builder_impl = capnp::DynamicStruct::Builder(struct_schema, builder_impl.getStructElement(array_index));
                field_builder = std::make_unique<StructBuilder>(std::move(struct_builder_impl), fields_count);
            }

            auto & struct_builder = assert_cast<StructBuilder &>(*field_builder);
            if (const auto * tuple_column = typeid_cast<const ColumnTuple *>(column.get()))
            {
                const auto & columns = tuple_column->getColumns();
                for (size_t i = 0; i != columns.size(); ++i)
                    fields_serializers[i]->writeRow(columns[i], struct_builder.field_builders[fields_indexes[i]], struct_builder.impl, fields_offsets[i], row_num);
            }
            else
            {
                fields_serializers[0]->writeRow(column, struct_builder.field_builders[fields_indexes[0]], struct_builder.impl, fields_offsets[0], row_num);
            }
        }

        void writeRow(const Columns & columns, StructBuilder & struct_builder, size_t row_num)
        {
            for (size_t i = 0; i != columns.size(); ++i)
                fields_serializers[i]->writeRow(columns[i], struct_builder.field_builders[fields_indexes[i]], struct_builder.impl, fields_offsets[i], row_num);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            auto reader_impl = parent_struct_reader.getReaderImpl();
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, reader_impl.getPointerField(slot_offset).getStruct(nullptr));
            if (auto * tuple_column = typeid_cast<ColumnTuple *>(&column))
            {
                for (size_t i = 0; i != tuple_column->tupleSize(); ++i)
                    fields_serializers[i]->readRow(tuple_column->getColumn(i), struct_reader, fields_offsets[i]);
            }
            else
                fields_serializers[0]->readRow(column, struct_reader, fields_offsets[0]);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            auto reader_impl = parent_list_reader.getReaderImpl();
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, reader_impl.getStructElement(array_index));
            if (auto * tuple_column = typeid_cast<ColumnTuple *>(&column))
            {
                for (size_t i = 0; i != tuple_column->tupleSize(); ++i)
                    fields_serializers[i]->readRow(tuple_column->getColumn(i), struct_reader, fields_offsets[i]);
            }
            else
                fields_serializers[0]->readRow(column, struct_reader, fields_offsets[0]);
        }

        void readRow(MutableColumns & columns, const capnp::DynamicStruct::Reader & reader)
        {
            for (size_t i = 0; i != columns.size(); ++i)
                fields_serializers[i]->readRow(*columns[i], reader, fields_offsets[i]);
        }

    private:
        void initialize(const DataTypes & data_types, const Names & names, const FormatSettings::CapnProto & settings)
        {
            auto node = struct_schema.getProto().getStruct();
            struct_size = capnp::_::StructSize(node.getDataWordCount(), node.getPointerCount());
            fields_count = struct_schema.getFields().size();
            fields_serializers.reserve(data_types.size());
            fields_offsets.reserve(data_types.size());
            fields_indexes.reserve(data_types.size());
            for (size_t i = 0; i != data_types.size(); ++i)
            {
                auto [field_name, _] = splitFieldName(names[i]);
                auto field = findFieldByName(struct_schema, field_name);
                if (!field)
                    throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Capnproto schema doesn't contain field with name {}", field_name);

                auto capnp_type = field->getType();
                fields_serializers.push_back(createSerializer(data_types[i], names[i], capnp_type, settings));
                fields_offsets.push_back(field->getProto().getSlot().getOffset());
                fields_indexes.push_back(field->getIndex());
            }
        }

        capnp::StructSchema struct_schema;
        capnp::_::StructSize struct_size;
        size_t fields_count;
        std::vector<std::unique_ptr<ICapnProtoSerializer>> fields_serializers;
        std::vector<UInt32> fields_offsets;
        std::vector<size_t> fields_indexes;

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
                return std::make_unique<CapnProtoFixedSizeRawDataSerializer<Int128>>(type, name, capnp_type);
            case TypeIndex::UInt128:
                return std::make_unique<CapnProtoFixedSizeRawDataSerializer<UInt128>>(type, name, capnp_type);
            case TypeIndex::Int256:
                return std::make_unique<CapnProtoFixedSizeRawDataSerializer<Int256>>(type, name, capnp_type);
            case TypeIndex::UInt256:
                return std::make_unique<CapnProtoFixedSizeRawDataSerializer<UInt256>>(type, name, capnp_type);
            case TypeIndex::Float32:
                return createFloatSerializer<Float32>(type, name, capnp_type);
            case TypeIndex::Float64:
                return createFloatSerializer<Float64>(type, name, capnp_type);
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
                return std::make_unique<CapnProtoFixedSizeRawDataSerializer<Decimal128>>(type, name, capnp_type);
            case TypeIndex::Decimal256:
                return std::make_unique<CapnProtoFixedSizeRawDataSerializer<Decimal256>>(type, name, capnp_type);
            case TypeIndex::IPv4:
                return std::make_unique<CapnProtoIPv4Serializer>(type, name, capnp_type);
            case TypeIndex::IPv6:
                return std::make_unique<CapnProtoFixedSizeRawDataSerializer<IPv6>>(type, name, capnp_type);
            case TypeIndex::UUID:
                return std::make_unique<CapnProtoFixedSizeRawDataSerializer<UUID>>(type, name, capnp_type);
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

#endif
