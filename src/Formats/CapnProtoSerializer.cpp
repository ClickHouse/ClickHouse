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

    template <typename ParentBuilder>
    std::unique_ptr<StructBuilder> initStructBuilder(ParentBuilder & parent_builder, UInt32 offset_or_index, const capnp::_::StructSize & struct_size, size_t elements, const capnp::StructSchema & schema)
    {
        capnp::DynamicStruct::Builder builder_impl;
        if constexpr (std::is_same_v<ParentBuilder, capnp::DynamicStruct::Builder>)
            builder_impl = capnp::DynamicStruct::Builder(schema, parent_builder.getBuilderImpl().getPointerField(offset_or_index).initStruct(struct_size));
        else
            builder_impl = capnp::DynamicStruct::Builder(schema, parent_builder.getBuilderImpl().getStructElement(offset_or_index));
        return std::make_unique<StructBuilder>(std::move(builder_impl), elements);
    }

    class ICapnProtoSerializer
    {
    public:
        /// Write row as struct field.
        virtual void writeRow(
            const ColumnPtr & column,
            std::unique_ptr<FieldBuilder> & builder, /// Maybe unused for simple types, needed to initialize structs and lists.
            capnp::DynamicStruct::Builder & parent_struct_builder,
            UInt32 slot_offset,
            size_t row_num) = 0;

        /// Write row as list element.
        virtual void writeRow(
            const ColumnPtr & column,
            std::unique_ptr<FieldBuilder> & builder, /// Maybe unused for simple types, needed to initialize structs and lists.
            capnp::DynamicList::Builder & parent_list_builder,
            UInt32 array_index,
            size_t row_num) = 0;

        /// Read row from struct field at slot_offset.
        virtual void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) = 0;

        /// Read row from list element at array_index.
        virtual void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) = 0;

        virtual ~ICapnProtoSerializer() = default;
    };

    template <typename CHNumericType, typename CapnProtoNumericType, bool convert_to_bool_on_read>
    class CapnProtoIntegerSerializer : public ICapnProtoSerializer
    {
    public:
        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            parent_struct_builder.getBuilderImpl().setDataField<CapnProtoNumericType>(slot_offset, getValue(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            parent_list_builder.getBuilderImpl().setDataElement<CapnProtoNumericType>(array_index, getValue(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertValue(column, parent_struct_reader.getReaderImpl().getDataField<CapnProtoNumericType>(slot_offset));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertValue(column, parent_list_reader.getReaderImpl().getDataElement<CapnProtoNumericType>(array_index));
        }

    private:
        CapnProtoNumericType getValue(const ColumnPtr & column, size_t row_num)
        {
            return static_cast<CapnProtoNumericType>(assert_cast<const ColumnVector<CHNumericType> &>(*column).getElement(row_num));
        }

        void insertValue(IColumn & column, CapnProtoNumericType value)
        {
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
            parent_struct_builder.getBuilderImpl().setDataField<CapnProtoFloatType>(slot_offset, getValue(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            parent_list_builder.getBuilderImpl().setDataElement<CapnProtoFloatType>(array_index, getValue(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertValue(column, parent_struct_reader.getReaderImpl().getDataField<CapnProtoFloatType>(slot_offset));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertValue(column, parent_list_reader.getReaderImpl().getDataElement<CapnProtoFloatType>(array_index));
        }

    private:
        CapnProtoFloatType getValue(const ColumnPtr & column, size_t row_num)
        {
            return static_cast<CapnProtoFloatType>(assert_cast<const ColumnVector<CHFloatType> &>(*column).getElement(row_num));
        }

        void insertValue(IColumn & column, CapnProtoFloatType value)
        {
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

            const auto * enum_type = assert_cast<const DataTypeEnum<EnumType> *>(data_type.get());
            const auto & enum_values = dynamic_cast<const EnumValues<EnumType> &>(*enum_type);

            enum_schema = capnp_type.asEnum();
            auto enumerants = enum_schema.getEnumerants();
            if (enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_VALUES)
            {
                auto ch_enum_values = enum_values.getSetOfAllValues();
                std::unordered_set<UInt16> capn_enum_values;
                for (auto enumerant : enumerants)
                    capn_enum_values.insert(enumerant.getOrdinal());

                /// Check if ClickHouse values is a superset of CapnProto values.
                ch_enum_is_superset = true;
                /// In CapnProto Enum fields are numbered sequentially starting from zero.
                /// Check if max CapnProto value exceeds max ClickHouse value.
                constexpr auto max_value = std::is_same_v<EnumType, Int8> ? INT8_MAX : INT16_MAX;
                if (enumerants.size() > max_value)
                {
                    ch_enum_is_superset = false;
                }
                else
                {
                    for (auto capnp_value : capn_enum_values)
                    {
                        if (!ch_enum_values.contains(static_cast<EnumType>(capnp_value)))
                        {
                            ch_enum_is_superset = false;
                            break;
                        }
                    }
                }

                /// Check if CapnProto values is a superset of ClickHouse values.
                capnp_enum_is_superset = true;
                for (auto ch_value : ch_enum_values)
                {
                    /// Capnp doesn't support negative enum values.
                    if (ch_value < 0 || !capn_enum_values.contains(static_cast<UInt16>(ch_value)))
                    {
                        capnp_enum_is_superset = false;
                        break;
                    }
                }
            }
            else
            {
                bool to_lower = enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_NAMES_CASE_INSENSITIVE;

                auto all_values = enum_values.getValues();
                std::unordered_map<String, EnumType> ch_name_to_value;
                for (auto & [name, value] : all_values)
                    ch_name_to_value[to_lower ? boost::algorithm::to_lower_copy(name) : name] = value;

                std::unordered_map<String, UInt16> capnp_name_to_value;
                for (auto enumerant : enumerants)
                {
                    String capnp_name = enumerant.getProto().getName();
                    capnp_name_to_value[to_lower ? boost::algorithm::to_lower_copy(capnp_name) : capnp_name] = enumerant.getOrdinal();
                }

                /// Check if ClickHouse names is a superset of CapnProto names.
                ch_enum_is_superset = true;
                for (auto & [capnp_name, capnp_value] : capnp_name_to_value)
                {
                    auto it = ch_name_to_value.find(capnp_name);
                    if (it == ch_name_to_value.end())
                    {
                        ch_enum_is_superset = false;
                        break;
                    }
                    capnp_to_ch_values[capnp_value] = it->second;
                }

                /// Check if CapnProto names is a superset of ClickHouse names.
                capnp_enum_is_superset = true;

                for (auto & [ch_name, ch_value] : ch_name_to_value)
                {
                    auto it = capnp_name_to_value.find(ch_name);
                    if (it == capnp_name_to_value.end())
                    {
                        capnp_enum_is_superset = false;
                        break;
                    }
                    ch_to_capnp_values[ch_value] = it->second;
                }
            }
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            parent_struct_builder.getBuilderImpl().setDataField<UInt16>(slot_offset, getValue(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            parent_list_builder.getBuilderImpl().setDataElement<UInt16>(array_index, getValue(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertValue(column, parent_struct_reader.getReaderImpl().getDataField<UInt16>(slot_offset));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertValue(column, parent_list_reader.getReaderImpl().getDataElement<UInt16>(array_index));
        }

    private:
        UInt16 getValue(const ColumnPtr & column, size_t row_num)
        {
            if (!capnp_enum_is_superset)
                throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Cannot convert ClickHouse enum to CapnProto enum: CapnProto enum values/names is not a superset of ClickHouse enum values/names");

            EnumType enum_value = assert_cast<const ColumnVector<EnumType> &>(*column).getElement(row_num);
            if (enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_VALUES)
                return static_cast<UInt16>(enum_value);
            auto it = ch_to_capnp_values.find(enum_value);
            if (it == ch_to_capnp_values.end())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected value {} in ClickHouse enum", enum_value);

            return it->second;
        }

        void insertValue(IColumn & column, UInt16 capnp_enum_value)
        {
            if (!ch_enum_is_superset)
                throw Exception(ErrorCodes::CAPN_PROTO_BAD_CAST, "Cannot convert CapnProto enum to ClickHouse enum: ClickHouse enum values/names is not a superset of CapnProto enum values/names");

            if (enum_comparing_mode == FormatSettings::CapnProtoEnumComparingMode::BY_VALUES)
            {
                assert_cast<ColumnVector<EnumType> &>(column).insertValue(static_cast<EnumType>(capnp_enum_value));
            }
            else
            {
                auto it = capnp_to_ch_values.find(capnp_enum_value);
                if (it == capnp_to_ch_values.end())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected value {} in CapnProto enum", capnp_enum_value);

                assert_cast<ColumnVector<EnumType> &>(column).insertValue(it->second);
            }
        }

        DataTypePtr data_type;
        capnp::EnumSchema enum_schema;
        const FormatSettings::CapnProtoEnumComparingMode enum_comparing_mode;
        bool ch_enum_is_superset;
        bool capnp_enum_is_superset;
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
            parent_struct_builder.getBuilderImpl().setDataField<UInt16>(slot_offset, getValue(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            parent_list_builder.getBuilderImpl().setDataElement<UInt16>(array_index, getValue(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertValue(column, parent_struct_reader.getReaderImpl().getDataField<UInt16>(slot_offset));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertValue(column, parent_list_reader.getReaderImpl().getDataElement<UInt16>(array_index));
        }

    private:
        UInt16 getValue(const ColumnPtr & column, size_t row_num)
        {
            return assert_cast<const ColumnDate &>(*column).getElement(row_num);
        }

        void insertValue(IColumn & column, UInt16 value)
        {
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
            parent_struct_builder.getBuilderImpl().setDataField<Int32>(slot_offset, getValue(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            parent_list_builder.getBuilderImpl().setDataElement<Int32>(array_index, getValue(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertValue(column, parent_struct_reader.getReaderImpl().getDataField<Int32>(slot_offset));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertValue(column, parent_list_reader.getReaderImpl().getDataElement<Int32>(array_index));
        }

    private:
        Int32 getValue(const ColumnPtr & column, size_t row_num)
        {
            return assert_cast<const ColumnDate32 &>(*column).getElement(row_num);
        }

        void insertValue(IColumn & column, Int32 value)
        {
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
            parent_struct_builder.getBuilderImpl().setDataField<UInt32>(slot_offset, getValue(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            parent_list_builder.getBuilderImpl().setDataElement<UInt32>(array_index, getValue(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertValue(column, parent_struct_reader.getReaderImpl().getDataField<UInt32>(slot_offset));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertValue(column, parent_list_reader.getReaderImpl().getDataElement<UInt32>(array_index));
        }

    private:
        UInt32 getValue(const ColumnPtr & column, size_t row_num)
        {
            return assert_cast<const ColumnDateTime &>(*column).getElement(row_num);
        }

        void insertValue(IColumn & column, UInt32 value)
        {
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
            parent_struct_builder.getBuilderImpl().setDataField<Int64>(slot_offset, getValue(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            parent_list_builder.getBuilderImpl().setDataElement<Int64>(array_index, getValue(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertValue(column, parent_struct_reader.getReaderImpl().getDataField<Int64>(slot_offset));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertValue(column, parent_list_reader.getReaderImpl().getDataElement<Int64>(array_index));
        }

    private:
        Int64 getValue(const ColumnPtr & column, size_t row_num)
        {
            return assert_cast<const ColumnDateTime64 &>(*column).getElement(row_num);
        }

        void insertValue(IColumn & column, Int64 value)
        {
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
            parent_struct_builder.getBuilderImpl().setDataField<NativeType>(slot_offset, getValue(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            parent_list_builder.getBuilderImpl().setDataElement<NativeType>(array_index, getValue(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertValue(column, parent_struct_reader.getReaderImpl().getDataField<NativeType>(slot_offset));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertValue(column, parent_list_reader.getReaderImpl().getDataElement<NativeType>(array_index));
        }

    private:
        NativeType getValue(const ColumnPtr & column, size_t row_num)
        {
            return assert_cast<const ColumnDecimal<DecimalType> &>(*column).getElement(row_num);
        }

        void insertValue(IColumn & column, NativeType value)
        {
            assert_cast<ColumnDecimal<DecimalType> &>(column).insertValue(value);
        }
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
            parent_struct_builder.getBuilderImpl().setDataField<UInt32>(slot_offset, getValue(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            parent_list_builder.getBuilderImpl().setDataElement<UInt32>(array_index, getValue(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertValue(column, parent_struct_reader.getReaderImpl().getDataField<UInt32>(slot_offset));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertValue(column, parent_list_reader.getReaderImpl().getDataElement<UInt32>(array_index));
        }

    private:
        UInt32 getValue(const ColumnPtr & column, size_t row_num)
        {
            return assert_cast<const ColumnIPv4 &>(*column).getElement(row_num);
        }

        void insertValue(IColumn & column, UInt32 value)
        {
            assert_cast<ColumnIPv4 &>(column).insertValue(IPv4(value));
        }
    };

    template <typename T>
    class CapnProtoFixedSizeRawDataSerializer : public ICapnProtoSerializer
    {
    private:
        static constexpr size_t expected_value_size = sizeof(T);

    public:
        CapnProtoFixedSizeRawDataSerializer(const DataTypePtr & data_type_, const String & column_name, const capnp::Type & capnp_type) : data_type(data_type_)
        {
            if (!capnp_type.isData())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            parent_struct_builder.getBuilderImpl().getPointerField(slot_offset).setBlob<capnp::Data>(getData(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_struct_builder, UInt32 array_index, size_t row_num) override
        {
            parent_struct_builder.getBuilderImpl().getPointerElement(array_index).setBlob<capnp::Data>(getData(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertData(column, parent_struct_reader.getReaderImpl().getPointerField(slot_offset).getBlob<capnp::Data>(nullptr, 0));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertData(column, parent_list_reader.getReaderImpl().getPointerElement(array_index).getBlob<capnp::Data>(nullptr, 0));
        }

    private:
        capnp::Data::Reader getData(const ColumnPtr & column, size_t row_num)
        {
            auto data = column->getDataAt(row_num);
            return capnp::Data::Reader(reinterpret_cast<const kj::byte *>(data.data), data.size);
        }

        void insertData(IColumn & column, capnp::Data::Reader data)
        {
            if (data.size() != expected_value_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size of {} value: {}", data_type->getName(), data.size());

            column.insertData(reinterpret_cast<const char *>(data.begin()), data.size());
        }

        DataTypePtr data_type;
    };

    template <typename CapnpType>
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
            parent_struct_builder.getBuilderImpl().getPointerField(slot_offset).setBlob<CapnpType>(getData(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_struct_builder, UInt32 array_index, size_t row_num) override
        {
            parent_struct_builder.getBuilderImpl().getPointerElement(array_index).setBlob<CapnpType>(getData(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertData(column, parent_struct_reader.getReaderImpl().getPointerField(slot_offset).getBlob<CapnpType>(nullptr, 0));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertData(column, parent_list_reader.getReaderImpl().getPointerElement(array_index).getBlob<CapnpType>(nullptr, 0));
        }

    private:
        using Reader = typename CapnpType::Reader;

        Reader getData(const ColumnPtr & column, size_t row_num)
        {
            auto data = column->getDataAt(row_num);
            if constexpr (std::is_same_v<CapnpType, capnp::Data>)
                return Reader(reinterpret_cast<const kj::byte *>(data.data), data.size);
            else
                return Reader(data.data, data.size);
        }

        void insertData(IColumn & column, Reader data)
        {
            column.insertData(reinterpret_cast<const char *>(data.begin()), data.size());
        }
    };

    template <typename CapnpType>
    class CapnProtoFixedStringSerializer : public ICapnProtoSerializer
    {
    private:

    public:
        CapnProtoFixedStringSerializer(const DataTypePtr & data_type, const String & column_name, const capnp::Type & capnp_type_) : capnp_type(capnp_type_)
        {
            if (!capnp_type.isData() && !capnp_type.isText())
                throwCannotConvert(data_type, column_name, capnp_type);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicStruct::Builder & parent_struct_builder, UInt32 slot_offset, size_t row_num) override
        {
            parent_struct_builder.getBuilderImpl().getPointerField(slot_offset).setBlob<CapnpType>(getData(column, row_num));
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> &, capnp::DynamicList::Builder & parent_struct_builder, UInt32 array_index, size_t row_num) override
        {
            parent_struct_builder.getBuilderImpl().getPointerElement(array_index).setBlob<CapnpType>(getData(column, row_num));
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            insertData(column, parent_struct_reader.getReaderImpl().getPointerField(slot_offset).getBlob<CapnpType>(nullptr, 0));
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            insertData(column, parent_list_reader.getReaderImpl().getPointerElement(array_index).getBlob<CapnpType>(nullptr, 0));
        }

    private:
        using Reader = typename CapnpType::Reader;

        Reader getData(const ColumnPtr & column, size_t row_num)
        {
            auto data = column->getDataAt(row_num);
            if constexpr (std::is_same_v<CapnpType, capnp::Data>)
            {
                return Reader(reinterpret_cast<const kj::byte *>(data.data), data.size);
            }
            else
            {
                if (data.data[data.size - 1] == 0)
                    return Reader(data.data, data.size);

                /// In TEXT type data should be null-terminated, but ClickHouse FixedString data could not be.
                /// To make data null-terminated we should copy it to temporary String object and use it in capnp::Text::Reader.
                /// Note that capnp::Text::Reader works only with pointer to the data and it's size, so we should
                /// guarantee that new String object life time is longer than capnp::Text::Reader life time.
                tmp_string = data.toString();
                return Reader(tmp_string.data(), tmp_string.size());
            }
        }

        void insertData(IColumn & column, Reader data)
        {
            auto & fixed_string_column = assert_cast<ColumnFixedString &>(column);
            if (data.size() > fixed_string_column.getN())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read data with size {} to FixedString with size {}", data.size(), fixed_string_column.getN());

            fixed_string_column.insertData(reinterpret_cast<const char *>(data.begin()), data.size());
        }

        String tmp_string;
        capnp::Type capnp_type;
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
            writeRowImpl(column, field_builder, parent_struct_builder, slot_offset, row_num);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            writeRowImpl(column, field_builder, parent_list_builder, array_index, row_num);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            readRowImpl(column, parent_struct_reader, slot_offset);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            readRowImpl(column, parent_list_reader, array_index);
        }

    private:
        template <typename ParentBuilder>
        void writeRowImpl(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, ParentBuilder & parent_builder, UInt32 offset_or_index, size_t row_num)
        {
            const auto & low_cardinality_column = assert_cast<const ColumnLowCardinality &>(*column);
            size_t index = low_cardinality_column.getIndexAt(row_num);
            const auto & dict_column = low_cardinality_column.getDictionary().getNestedColumn();
            nested_serializer->writeRow(dict_column, field_builder, parent_builder, offset_or_index, index);
        }

        template <typename ParentReader>
        void readRowImpl(IColumn & column, const ParentReader & parent_reader, UInt32 offset_or_index)
        {
            auto & low_cardinality_column = assert_cast<ColumnLowCardinality &>(column);
            auto tmp_column = low_cardinality_column.getDictionary().getNestedColumn()->cloneEmpty();
            nested_serializer->readRow(*tmp_column, parent_reader, offset_or_index);
            low_cardinality_column.insertFromFullColumn(*tmp_column, 0);
        }

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
            writeRowImpl(column, field_builder, parent_struct_builder, slot_offset, row_num);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            writeRowImpl(column, field_builder, parent_list_builder, array_index, row_num);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, parent_struct_reader.getReaderImpl().getPointerField(slot_offset).getStruct(nullptr));
            readRowImpl(column, struct_reader);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, parent_list_reader.getReaderImpl().getStructElement(array_index));
            readRowImpl(column, struct_reader);
        }

    private:
        template <typename ParentBuilder>
        void writeRowImpl(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, ParentBuilder & parent_builder, UInt32 offset_or_index, size_t row_num)
        {
            if (!field_builder)
                field_builder = initStructBuilder(parent_builder, offset_or_index, struct_size, 1, struct_schema);

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

        void readRowImpl(IColumn & column, capnp::DynamicStruct::Reader & struct_reader)
        {
            auto & nullable_column = assert_cast<ColumnNullable &>(column);
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
            writeRowImpl(column, field_builder, parent_struct_builder, slot_offset, row_num);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            writeRowImpl(column, field_builder, parent_list_builder, array_index, row_num);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            auto list_reader = capnp::DynamicList::Reader(list_schema, parent_struct_reader.getReaderImpl().getPointerField(slot_offset).getList(element_size, nullptr));
            readRowImpl(column, list_reader);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            auto list_reader = capnp::DynamicList::Reader(list_schema, parent_list_reader.getReaderImpl().getPointerElement(array_index).getList(element_size, nullptr));
            readRowImpl(column, list_reader);
        }

    private:
        template <typename ParentBuilder>
        void writeRowImpl(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, ParentBuilder & parent_builder, UInt32 offset_or_index, size_t row_num)
        {
            const auto * array_column = assert_cast<const ColumnArray *>(column.get());
            const auto & nested_column = array_column->getDataPtr();
            const auto & offsets = array_column->getOffsets();
            auto offset = offsets[row_num - 1];
            UInt32 size = static_cast<UInt32>(offsets[row_num] - offset);

            if (!field_builder)
                field_builder = std::make_unique<ListBuilder>(capnp::DynamicList::Builder(list_schema, initListBuilder(parent_builder, offset_or_index, size)), size);

            auto & list_builder = assert_cast<ListBuilder &>(*field_builder);
            for (UInt32 i = 0; i != size; ++i)
                nested_serializer->writeRow(nested_column, list_builder.nested_builders[i], list_builder.impl, i, offset + i);
        }

        template <typename ParentBuilder>
        capnp::_::ListBuilder initListBuilder(ParentBuilder & parent_builder, UInt32 offset_or_index, UInt32 size)
        {
            if (element_is_struct)
            {
                if constexpr (std::is_same_v<ParentBuilder, capnp::DynamicStruct::Builder>)
                    return parent_builder.getBuilderImpl().getPointerField(offset_or_index).initStructList(size, element_struct_size);
                else
                    return parent_builder.getBuilderImpl().getPointerElement(offset_or_index).initStructList(size, element_struct_size);
            }

            if constexpr (std::is_same_v<ParentBuilder, capnp::DynamicStruct::Builder>)
                return parent_builder.getBuilderImpl().getPointerField(offset_or_index).initList(element_size, size);
            else
                return parent_builder.getBuilderImpl().getPointerElement(offset_or_index).initList(element_size, size);
        }

        void readRowImpl(IColumn & column, const capnp::DynamicList::Reader & list_reader)
        {
            UInt32 size = list_reader.size();
            auto & column_array = assert_cast<ColumnArray &>(column);
            auto & offsets = column_array.getOffsets();
            offsets.push_back(offsets.back() + list_reader.size());

            auto & nested_column = column_array.getData();
            for (UInt32 i = 0; i != size; ++i)
                nested_serializer->readRow(nested_column, list_reader, i);
        }

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
            writeRowImpl(column, field_builder, parent_struct_builder, slot_offset, row_num);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            writeRowImpl(column, field_builder, parent_list_builder, array_index, row_num);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, parent_struct_reader.getReaderImpl().getPointerField(slot_offset).getStruct(nullptr));
            readRowImpl(column, struct_reader);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, parent_list_reader.getReaderImpl().getStructElement(array_index));
            readRowImpl(column, struct_reader);
        }

    private:
        template <typename ParentBuilder>
        void writeRowImpl(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, ParentBuilder & parent_builder, UInt32 offset_or_index, size_t row_num)
        {
            if (!field_builder)
                field_builder = initStructBuilder(parent_builder, offset_or_index, struct_size, 1, struct_schema);

            auto & struct_builder = assert_cast<StructBuilder &>(*field_builder);
            const auto & entries_column = assert_cast<const ColumnMap *>(column.get())->getNestedColumnPtr();
            nested_serializer->writeRow(entries_column, struct_builder.field_builders[0], struct_builder.impl, entries_slot_offset, row_num);
        }

        void readRowImpl(IColumn & column, const capnp::DynamicStruct::Reader & struct_reader)
        {
            auto & entries_column = assert_cast<ColumnMap &>(column).getNestedColumn();
            nested_serializer->readRow(entries_column, struct_reader, entries_slot_offset);
        }

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
            writeRowImpl(column, field_builder, parent_struct_builder, slot_offset, row_num);
        }

        void writeRow(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, capnp::DynamicList::Builder & parent_list_builder, UInt32 array_index, size_t row_num) override
        {
            writeRowImpl(column, field_builder, parent_list_builder, array_index, row_num);
        }

        /// Method for writing root struct.
        void writeRow(const Columns & columns, StructBuilder & struct_builder, size_t row_num)
        {
            for (size_t i = 0; i != columns.size(); ++i)
                fields_serializers[i]->writeRow(columns[i], struct_builder.field_builders[fields_indexes[i]], struct_builder.impl, fields_offsets[i], row_num);
        }

        void readRow(IColumn & column, const capnp::DynamicStruct::Reader & parent_struct_reader, UInt32 slot_offset) override
        {
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, parent_struct_reader.getReaderImpl().getPointerField(slot_offset).getStruct(nullptr));
            readRowImpl(column, struct_reader);
        }

        void readRow(IColumn & column, const capnp::DynamicList::Reader & parent_list_reader, UInt32 array_index) override
        {
            auto struct_reader = capnp::DynamicStruct::Reader(struct_schema, parent_list_reader.getReaderImpl().getStructElement(array_index));
            readRowImpl(column, struct_reader);
        }

        /// Method for reading from root struct.
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

        template <typename ParentBuilder>
        void writeRowImpl(const ColumnPtr & column, std::unique_ptr<FieldBuilder> & field_builder, ParentBuilder & parent_builder, UInt32 offset_or_index, size_t row_num)
        {
            if (!field_builder)
                field_builder = initStructBuilder(parent_builder, offset_or_index, struct_size, fields_count, struct_schema);

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

        void readRowImpl(IColumn & column, const capnp::DynamicStruct::Reader & struct_reader)
        {
            if (auto * tuple_column = typeid_cast<ColumnTuple *>(&column))
            {
                for (size_t i = 0; i != tuple_column->tupleSize(); ++i)
                    fields_serializers[i]->readRow(tuple_column->getColumn(i), struct_reader, fields_offsets[i]);
            }
            else
                fields_serializers[0]->readRow(column, struct_reader, fields_offsets[0]);
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
                    return std::make_unique<CapnProtoStringSerializer<capnp::Data>>(type, name, capnp_type);
                return std::make_unique<CapnProtoStringSerializer<capnp::Text>>(type, name, capnp_type);
            case TypeIndex::FixedString:
                if (capnp_type.isData())
                    return std::make_unique<CapnProtoFixedStringSerializer<capnp::Data>>(type, name, capnp_type);
                return std::make_unique<CapnProtoFixedStringSerializer<capnp::Text>>(type, name, capnp_type);
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
