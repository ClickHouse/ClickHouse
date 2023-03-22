#include "CapnProtoRowInputFormat.h"
#if USE_CAPNP

#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSchemaInfo.h>
#include <capnp/serialize.h>
#include <capnp/dynamic.h>
#include <capnp/common.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnDecimal.h>

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

CapnProtoRowInputFormat::CapnProtoRowInputFormat(ReadBuffer & in_, Block header, Params params_, const FormatSchemaInfo & info, const FormatSettings & format_settings_)
    : IRowInputFormat(std::move(header), in_, std::move(params_))
    , parser(std::make_shared<CapnProtoSchemaParser>())
    , format_settings(format_settings_)
    , column_types(getPort().getHeader().getDataTypes())
    , column_names(getPort().getHeader().getNames())
{
    // Parse the schema and fetch the root object
    root = parser->getMessageSchema(info);
    checkCapnProtoSchemaStructure(root, getPort().getHeader(), format_settings.capn_proto.enum_comparing_mode);
}

kj::Array<capnp::word> CapnProtoRowInputFormat::readMessage()
{
    uint32_t segment_count;
    in->readStrict(reinterpret_cast<char*>(&segment_count), sizeof(uint32_t));
    /// Don't allow large amount of segments as it's done in capnproto library:
    /// https://github.com/capnproto/capnproto/blob/931074914eda9ca574b5c24d1169c0f7a5156594/c%2B%2B/src/capnp/serialize.c%2B%2B#L181
    /// Large amount of segments can indicate that corruption happened.
    if (segment_count >= 512)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Message has too many segments. Most likely, data was corrupted");

    // one for segmentCount and one because segmentCount starts from 0
    const auto prefix_size = (2 + segment_count) * sizeof(uint32_t);
    const auto words_prefix_size = (segment_count + 1) / 2 + 1;
    auto prefix = kj::heapArray<capnp::word>(words_prefix_size);
    auto prefix_chars = prefix.asChars();
    ::memcpy(prefix_chars.begin(), &segment_count, sizeof(uint32_t));

    // read size of each segment
    for (size_t i = 0; i <= segment_count; ++i)
        in->readStrict(prefix_chars.begin() + ((i + 1) * sizeof(uint32_t)), sizeof(uint32_t));

    // calculate size of message
    const auto expected_words = capnp::expectedSizeInWordsFromPrefix(prefix);
    const auto expected_bytes = expected_words * sizeof(capnp::word);
    const auto data_size = expected_bytes - prefix_size;
    auto msg = kj::heapArray<capnp::word>(expected_words);
    auto msg_chars = msg.asChars();

    // read full message
    ::memcpy(msg_chars.begin(), prefix_chars.begin(), prefix_size);
    in->readStrict(msg_chars.begin() + prefix_size, data_size);

    return msg;
}

static void insertSignedInteger(IColumn & column, const DataTypePtr & column_type, Int64 value)
{
    switch (column_type->getTypeId())
    {
        case TypeIndex::Int8:
            assert_cast<ColumnInt8 &>(column).insertValue(value);
            break;
        case TypeIndex::Int16:
            assert_cast<ColumnInt16 &>(column).insertValue(value);
            break;
        case TypeIndex::Int32:
            assert_cast<ColumnInt32 &>(column).insertValue(value);
            break;
        case TypeIndex::Int64:
            assert_cast<ColumnInt64 &>(column).insertValue(value);
            break;
        case TypeIndex::DateTime64:
            assert_cast<ColumnDecimal<DateTime64> &>(column).insertValue(value);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column type is not a signed integer.");
    }
}

static void insertUnsignedInteger(IColumn & column, const DataTypePtr & column_type, UInt64 value)
{
    switch (column_type->getTypeId())
    {
        case TypeIndex::UInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(value);
            break;
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(value);
            break;
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(value);
            break;
        case TypeIndex::UInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(value);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column type is not an unsigned integer.");
    }
}

static void insertFloat(IColumn & column, const DataTypePtr & column_type, Float64 value)
{
    switch (column_type->getTypeId())
    {
        case TypeIndex::Float32:
            assert_cast<ColumnFloat32 &>(column).insertValue(value);
            break;
        case TypeIndex::Float64:
            assert_cast<ColumnFloat64 &>(column).insertValue(value);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column type is not a float.");
    }
}

template <typename Value>
static void insertString(IColumn & column, Value value)
{
    column.insertData(reinterpret_cast<const char *>(value.begin()), value.size());
}

template <typename ValueType>
static void insertEnum(IColumn & column, const DataTypePtr & column_type, const capnp::DynamicEnum & enum_value, FormatSettings::EnumComparingMode enum_comparing_mode)
{
    auto enumerant = *kj::_::readMaybe(enum_value.getEnumerant());
    auto enum_type = assert_cast<const DataTypeEnum<ValueType> *>(column_type.get());
    DataTypePtr nested_type = std::make_shared<DataTypeNumber<ValueType>>();
    switch (enum_comparing_mode)
    {
        case FormatSettings::EnumComparingMode::BY_VALUES:
            insertSignedInteger(column, nested_type, Int64(enumerant.getOrdinal()));
            return;
        case FormatSettings::EnumComparingMode::BY_NAMES:
            insertSignedInteger(column, nested_type, Int64(enum_type->getValue(String(enumerant.getProto().getName()))));
            return;
        case FormatSettings::EnumComparingMode::BY_NAMES_CASE_INSENSITIVE:
        {
            /// Find the same enum name case insensitive.
            String enum_name = enumerant.getProto().getName();
            for (auto & name : enum_type->getAllRegisteredNames())
            {
                if (compareEnumNames(name, enum_name, enum_comparing_mode))
                {
                    insertSignedInteger(column, nested_type, Int64(enum_type->getValue(name)));
                    break;
                }
            }
        }
    }
}

static void insertValue(IColumn & column, const DataTypePtr & column_type, const capnp::DynamicValue::Reader & value, FormatSettings::EnumComparingMode enum_comparing_mode)
{
    if (column_type->lowCardinality())
    {
        auto & lc_column = assert_cast<ColumnLowCardinality &>(column);
        auto tmp_column = lc_column.getDictionary().getNestedColumn()->cloneEmpty();
        auto dict_type = assert_cast<const DataTypeLowCardinality *>(column_type.get())->getDictionaryType();
        insertValue(*tmp_column, dict_type, value, enum_comparing_mode);
        lc_column.insertFromFullColumn(*tmp_column, 0);
        return;
    }

    switch (value.getType())
    {
        case capnp::DynamicValue::Type::INT:
            insertSignedInteger(column, column_type, value.as<Int64>());
            break;
        case capnp::DynamicValue::Type::UINT:
            insertUnsignedInteger(column, column_type, value.as<UInt64>());
            break;
        case capnp::DynamicValue::Type::FLOAT:
            insertFloat(column, column_type, value.as<Float64>());
            break;
        case capnp::DynamicValue::Type::BOOL:
            insertUnsignedInteger(column, column_type, UInt64(value.as<bool>()));
            break;
        case capnp::DynamicValue::Type::DATA:
            insertString(column, value.as<capnp::Data>());
            break;
        case capnp::DynamicValue::Type::TEXT:
            insertString(column, value.as<capnp::Text>());
            break;
        case capnp::DynamicValue::Type::ENUM:
            if (column_type->getTypeId() == TypeIndex::Enum8)
                insertEnum<Int8>(column, column_type, value.as<capnp::DynamicEnum>(), enum_comparing_mode);
            else
                insertEnum<Int16>(column, column_type, value.as<capnp::DynamicEnum>(), enum_comparing_mode);
            break;
        case capnp::DynamicValue::LIST:
        {
            auto list_value = value.as<capnp::DynamicList>();
            auto & column_array = assert_cast<ColumnArray &>(column);
            auto & offsets = column_array.getOffsets();
            offsets.push_back(offsets.back() + list_value.size());

            auto & nested_column = column_array.getData();
            auto nested_type = assert_cast<const DataTypeArray *>(column_type.get())->getNestedType();
            for (const auto & nested_value : list_value)
                insertValue(nested_column, nested_type, nested_value, enum_comparing_mode);
            break;
        }
        case capnp::DynamicValue::Type::STRUCT:
        {
            auto struct_value = value.as<capnp::DynamicStruct>();
            if (column_type->isNullable())
            {
                auto & nullable_column = assert_cast<ColumnNullable &>(column);
                auto field = *kj::_::readMaybe(struct_value.which());
                if (field.getType().isVoid())
                    nullable_column.insertDefault();
                else
                {
                    auto & nested_column = nullable_column.getNestedColumn();
                    auto nested_type = assert_cast<const DataTypeNullable *>(column_type.get())->getNestedType();
                    auto nested_value = struct_value.get(field);
                    insertValue(nested_column, nested_type, nested_value, enum_comparing_mode);
                    nullable_column.getNullMapData().push_back(0);
                }
            }
            else
            {
                auto & tuple_column = assert_cast<ColumnTuple &>(column);
                const auto * tuple_type = assert_cast<const DataTypeTuple *>(column_type.get());
                for (size_t i = 0; i != tuple_column.tupleSize(); ++i)
                    insertValue(
                        tuple_column.getColumn(i),
                        tuple_type->getElements()[i],
                        struct_value.get(tuple_type->getElementNames()[i]),
                        enum_comparing_mode);
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CapnProto value type.");
    }
}

bool CapnProtoRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in->eof())
        return false;

    auto array = readMessage();

#if CAPNP_VERSION >= 7000 && CAPNP_VERSION < 8000
    capnp::UnalignedFlatArrayMessageReader msg(array);
#else
    capnp::FlatArrayMessageReader msg(array);
#endif

    auto root_reader = msg.getRoot<capnp::DynamicStruct>(root);

    for (size_t i = 0; i != columns.size(); ++i)
    {
        auto value = getReaderByColumnName(root_reader, column_names[i]);
        insertValue(*columns[i], column_types[i], value, format_settings.capn_proto.enum_comparing_mode);
    }

    return true;
}

CapnProtoSchemaReader::CapnProtoSchemaReader(const FormatSettings & format_settings_) : format_settings(format_settings_)
{
}

NamesAndTypesList CapnProtoSchemaReader::readSchema()
{
    auto schema_info = FormatSchemaInfo(
        format_settings.schema.format_schema,
        "CapnProto",
        true,
        format_settings.schema.is_server,
        format_settings.schema.format_schema_path);

    auto schema_parser = CapnProtoSchemaParser();
    auto schema = schema_parser.getMessageSchema(schema_info);
    return capnProtoSchemaToCHSchema(schema);
}

void registerInputFormatCapnProto(FormatFactory & factory)
{
    factory.registerInputFormat(
        "CapnProto",
        [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
        {
            return std::make_shared<CapnProtoRowInputFormat>(buf, sample, std::move(params),
                       FormatSchemaInfo(settings, "CapnProto", true), settings);
        });
    factory.registerFileExtension("capnp", "CapnProto");
}

void registerCapnProtoSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("CapnProto", [](const FormatSettings & settings)
    {
       return std::make_shared<CapnProtoSchemaReader>(settings);
    });
}

}

#else

namespace DB
{
    class FormatFactory;
    void registerInputFormatCapnProto(FormatFactory &) {}
    void registerCapnProtoSchemaReader(FormatFactory &) {}
}

#endif // USE_CAPNP
