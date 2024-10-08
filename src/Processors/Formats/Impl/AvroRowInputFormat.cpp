#include "AvroRowInputFormat.h"
#include "DataTypes/DataTypeLowCardinality.h"
#if USE_AVRO

#include <numeric>

#include <Core/Field.h>

#include <Common/CacheBase.h>

#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromString.h>

#include <Formats/FormatFactory.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeFactory.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>

#include <Compiler.hh>
#include <DataFile.hh>
#include <Decoder.hh>
#include <Node.hh>
#include <NodeConcepts.hh>
#include <NodeImpl.hh>
#include <Types.hh>
#include <ValidSchema.hh>

#include <Poco/Buffer.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPCredentials.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int THERE_IS_NO_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int ILLEGAL_COLUMN;
    extern const int TYPE_MISMATCH;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_READ_ALL_DATA;
}

bool AvroInputStreamReadBufferAdapter::next(const uint8_t ** data, size_t * len)
{
    if (in.eof())
    {
        *len = 0;
        return false;
    }

    *data = reinterpret_cast<const uint8_t *>(in.position());
    *len = in.available();

    in.position() += in.available();
    return true;
}

void AvroInputStreamReadBufferAdapter::backup(size_t len)
{
    in.position() -= len;
}

void AvroInputStreamReadBufferAdapter::skip(size_t len)
{
    in.tryIgnore(len);
}

size_t AvroInputStreamReadBufferAdapter::byteCount() const
{
    return in.count();
}

/// Insert value with conversion to the column of target type.
template <typename T>
static void insertNumber(IColumn & column, WhichDataType type, T value)
{
    switch (type.idx)
    {
        case TypeIndex::UInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(static_cast<UInt8>(value));
            break;
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
            assert_cast<ColumnUInt16 &>(column).insertValue(static_cast<UInt16>(value));
            break;
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(value));
            break;
        case TypeIndex::UInt64:
            assert_cast<ColumnUInt64 &>(column).insertValue(static_cast<UInt64>(value));
            break;
        case TypeIndex::Int8:
            assert_cast<ColumnInt8 &>(column).insertValue(static_cast<Int8>(value));
            break;
        case TypeIndex::Int16:
            assert_cast<ColumnInt16 &>(column).insertValue(static_cast<Int16>(value));
            break;
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::Int32:
            assert_cast<ColumnInt32 &>(column).insertValue(static_cast<Int32>(value));
            break;
        case TypeIndex::Int64:
            assert_cast<ColumnInt64 &>(column).insertValue(static_cast<Int64>(value));
            break;
        case TypeIndex::Float32:
            assert_cast<ColumnFloat32 &>(column).insertValue(static_cast<Float32>(value));
            break;
        case TypeIndex::Float64:
            assert_cast<ColumnFloat64 &>(column).insertValue(static_cast<Float64>(value));
            break;
        case TypeIndex::Decimal32:
            assert_cast<ColumnDecimal<Decimal32> &>(column).insertValue(static_cast<Int32>(value));
            break;
        case TypeIndex::Decimal64:
            assert_cast<ColumnDecimal<Decimal64> &>(column).insertValue(static_cast<Int64>(value));
            break;
        case TypeIndex::DateTime64:
            assert_cast<ColumnDecimal<DateTime64> &>(column).insertValue(static_cast<Int64>(value));
            break;
        case TypeIndex::IPv4:
            assert_cast<ColumnIPv4 &>(column).insertValue(IPv4(static_cast<UInt32>(value)));
            break;
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type is not compatible with Avro");
    }
}

template <typename DecimalType>
static AvroDeserializer::DeserializeFn createDecimalDeserializeFn(const avro::NodePtr & root_node, const DataTypePtr & target_type, bool is_fixed)
{
    auto logical_type = root_node->logicalType();
    size_t fixed_size = 0;
    if (is_fixed)
        fixed_size = root_node->fixedSize();
    const auto & decimal_type = assert_cast<const DecimalType &>(*target_type);
    if (decimal_type.getScale() != static_cast<UInt32>(logical_type.scale()) || decimal_type.getPrecision() != static_cast<UInt32>(logical_type.precision()))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot insert Avro decimal with scale {} and precision {} to ClickHouse type {} with scale {} and precision {}",
            logical_type.scale(),
            logical_type.precision(),
            target_type->getName(),
            decimal_type.getScale(),
            decimal_type.getPrecision());

    return [tmp = std::vector<uint8_t>(), target_type, fixed_size](IColumn & column, avro::Decoder & decoder) mutable
    {
        static constexpr size_t field_type_size = sizeof(typename DecimalType::FieldType);
        if (fixed_size)
            tmp = decoder.decodeFixed(fixed_size);
        else
            tmp = decoder.decodeBytes();

        if (tmp.size() > field_type_size || tmp.empty())
            throw Exception(
                ErrorCodes::CANNOT_PARSE_UUID,
                "Cannot parse type {}, expected non-empty binary data with size equal to or less than {}, got {}",
                target_type->getName(),
                field_type_size,
                tmp.size());
        if (tmp.size() != field_type_size)
        {
            /// Extent value to required size by adding padding.
            /// Check if value is negative or positive.
            std::vector<uint8_t> padding;
            if (tmp[0] & 128)
                padding = std::vector<uint8_t>(field_type_size - tmp.size(), 0xff);
            else
                padding = std::vector<uint8_t>(field_type_size - tmp.size(), 0);
            tmp.insert(tmp.begin(), padding.begin(), padding.end());
        }

        typename DecimalType::FieldType field;
        ReadBufferFromString buf(tmp);
        readBinaryBigEndian(field.value, buf);
        assert_cast<typename DecimalType::ColumnType &>(column).insertValue(field);
        return true;
    };
}

static std::string nodeToJSON(avro::NodePtr root_node)
{
    std::ostringstream ss;      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ss.exceptions(std::ios::failbit);
    root_node->printJson(ss, 0);
    return ss.str();
}

static std::string nodeName(avro::NodePtr node)
{
    if (node->hasName())
        return node->name().simpleName();
    return avro::toString(node->type());
}

static bool canBeDeserializedFromFixed(const DataTypePtr & target_type, size_t fixed_size)
{
    switch (target_type->getTypeId())
    {
        case TypeIndex::String:
            return true;
        case TypeIndex::FixedString: [[fallthrough]];
        case TypeIndex::IPv6: [[fallthrough]];
        case TypeIndex::Int128: [[fallthrough]];
        case TypeIndex::UInt128: [[fallthrough]];
        case TypeIndex::Int256: [[fallthrough]];
        case TypeIndex::UInt256:
            return target_type->getSizeOfValueInMemory() == fixed_size;
        default:
            return false;
    }
}

AvroDeserializer::DeserializeFn AvroDeserializer::createDeserializeFn(const avro::NodePtr & root_node, const DataTypePtr & target_type)
{
    if (target_type->lowCardinality())
    {
        const auto * lc_type = assert_cast<const DataTypeLowCardinality *>(target_type.get());
        auto dict_deserialize = createDeserializeFn(root_node, lc_type->getDictionaryType());
        return [dict_deserialize](IColumn & column, avro::Decoder & decoder)
        {
            auto & lc_column = assert_cast<ColumnLowCardinality &>(column);
            auto tmp_column = lc_column.getDictionary().getNestedColumn()->cloneEmpty();
            auto res = dict_deserialize(*tmp_column, decoder);
            lc_column.insertFromFullColumn(*tmp_column, 0);
            return res;
        };
    }

    const WhichDataType target = WhichDataType(target_type);

    switch (root_node->type())
    {
        case avro::AVRO_STRING: [[fallthrough]];
        case avro::AVRO_BYTES:
            if (target.isUUID())
            {
                return [tmp = std::string()](IColumn & column, avro::Decoder & decoder) mutable
                {
                    decoder.decodeString(tmp);
                    if (tmp.length() != 36)
                        throw Exception(ErrorCodes::CANNOT_PARSE_UUID, "Cannot parse uuid {}", tmp);

                    const UUID uuid = parseUUID({reinterpret_cast<const UInt8 *>(tmp.data()), tmp.length()});
                    assert_cast<DataTypeUUID::ColumnType &>(column).insertValue(uuid);
                    return true;
                };
            }
            if (target.isString() || target.isFixedString())
            {
                return [tmp = std::string()](IColumn & column, avro::Decoder & decoder) mutable
                {
                    decoder.decodeString(tmp);
                    column.insertData(tmp.c_str(), tmp.length());
                    return true;
                };
            }
            if (target.isDecimal32())
                return createDecimalDeserializeFn<DataTypeDecimal32>(root_node, target_type, false);
            if (target.isDecimal64())
                return createDecimalDeserializeFn<DataTypeDecimal64>(root_node, target_type, false);
            if (target.isDecimal128())
                return createDecimalDeserializeFn<DataTypeDecimal128>(root_node, target_type, false);
            if (target.isDecimal256())
                return createDecimalDeserializeFn<DataTypeDecimal256>(root_node, target_type, false);
            if (target.isDateTime64())
                return createDecimalDeserializeFn<DataTypeDateTime64>(root_node, target_type, false);
            break;
        case avro::AVRO_INT:
            if (target_type->isValueRepresentedByNumber())
            {
                return [target](IColumn & column, avro::Decoder & decoder)
                {
                    insertNumber(column, target, decoder.decodeInt());
                    return true;
                };
            }
            break;
        case avro::AVRO_LONG:
            if (target_type->isValueRepresentedByNumber())
            {
                return [target](IColumn & column, avro::Decoder & decoder)
                {
                    insertNumber(column, target, decoder.decodeLong());
                    return true;
                };
            }
            break;
        case avro::AVRO_FLOAT:
            if (target_type->isValueRepresentedByNumber())
            {
                return [target](IColumn & column, avro::Decoder & decoder)
                {
                    insertNumber(column, target, decoder.decodeFloat());
                    return true;
                };
            }
            break;
        case avro::AVRO_DOUBLE:
            if (target_type->isValueRepresentedByNumber())
            {
                return [target](IColumn & column, avro::Decoder & decoder)
                {
                    insertNumber(column, target, decoder.decodeDouble());
                    return true;
                };
            }
            break;
        case avro::AVRO_BOOL:
            if (target_type->isValueRepresentedByNumber())
            {
                return [target](IColumn & column, avro::Decoder & decoder)
                {
                    insertNumber(column, target, decoder.decodeBool());
                    return true;
                };
            }
            break;
        case avro::AVRO_ARRAY:
            if (target.isArray())
            {
                auto nested_source_type = root_node->leafAt(0);
                auto nested_target_type = assert_cast<const DataTypeArray &>(*target_type).getNestedType();
                auto nested_deserialize = createDeserializeFn(nested_source_type, nested_target_type);
                return [nested_deserialize](IColumn & column, avro::Decoder & decoder)
                {
                    ColumnArray & column_array = assert_cast<ColumnArray &>(column);
                    ColumnArray::Offsets & offsets = column_array.getOffsets();
                    IColumn & nested_column = column_array.getData();
                    size_t total = 0;
                    for (size_t n = decoder.arrayStart(); n != 0; n = decoder.arrayNext())
                    {
                        total += n;
                        for (size_t i = 0; i < n; ++i)
                        {
                            nested_deserialize(nested_column, decoder);
                        }
                    }
                    offsets.push_back(offsets.back() + total);
                    return true;
                };
            }
            break;
        case avro::AVRO_UNION:
        {
            if (root_node->leaves() == 1)
            {
                auto nested_deserialize = createDeserializeFn(root_node->leafAt(0), target_type);
                return [nested_deserialize](IColumn & column, avro::Decoder & decoder)
                {
                    decoder.decodeUnionIndex();
                    nested_deserialize(column, decoder);
                    return true;
                };
            }
            /// FIXME Support UNION has more than two datatypes.
            if (root_node->leaves() == 2
                && (root_node->leafAt(0)->type() == avro::AVRO_NULL || root_node->leafAt(1)->type() == avro::AVRO_NULL))
            {
                int non_null_union_index = root_node->leafAt(0)->type() == avro::AVRO_NULL ? 1 : 0;
                if (target.isNullable())
                {
                    auto nested_deserialize = createDeserializeFn(root_node->leafAt(non_null_union_index), removeNullable(target_type));
                    return [non_null_union_index, nested_deserialize](IColumn & column, avro::Decoder & decoder)
                    {
                        ColumnNullable & col = assert_cast<ColumnNullable &>(column);
                        int union_index = static_cast<int>(decoder.decodeUnionIndex());
                        if (union_index == non_null_union_index)
                        {
                            nested_deserialize(col.getNestedColumn(), decoder);
                            col.getNullMapData().push_back(0);
                        }
                        else
                        {
                            col.insertDefault();
                        }
                        return true;
                    };
                }
                if (null_as_default)
                {
                    auto nested_deserialize = createDeserializeFn(root_node->leafAt(non_null_union_index), target_type);
                    return [non_null_union_index, nested_deserialize](IColumn & column, avro::Decoder & decoder)
                    {
                        int union_index = static_cast<int>(decoder.decodeUnionIndex());
                        if (union_index == non_null_union_index)
                        {
                            nested_deserialize(column, decoder);
                            return true;
                        }
                        column.insertDefault();
                        return false;
                    };
                }

                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot insert Avro Union(Null, {}) into non-nullable type {}. To use default value on NULL, enable setting "
                    "input_format_null_as_default",
                    avro::toString(root_node->leafAt(non_null_union_index)->type()),
                    target_type->getName());
            }
            break;
        }
        case avro::AVRO_NULL:
            if (target.isNullable())
            {
                auto nested_type = removeNullable(target_type);
                if (nested_type->getTypeId() == TypeIndex::Nothing)
                {
                    return [](IColumn &, avro::Decoder & decoder)
                    {
                        decoder.decodeNull();
                        return true;
                    };
                }

                return [](IColumn & column, avro::Decoder & decoder)
                {
                    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
                    decoder.decodeNull();
                    col.insertDefault();
                    return true;
                };
            }
            else if (null_as_default)
            {
                return [](IColumn & column, avro::Decoder & decoder)
                {
                    decoder.decodeNull();
                    column.insertDefault();
                    return false;
                };
            }
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot insert Avro Null into non-nullable type {}. To use default value on NULL, enable setting "
                    "input_format_null_as_default", target_type->getName());
            }
        case avro::AVRO_ENUM:
            if (target.isString())
            {
                std::vector<std::string> symbols;
                symbols.reserve(root_node->names());
                for (int i = 0; i < static_cast<int>(root_node->names()); ++i)
                {
                    symbols.push_back(root_node->nameAt(i));
                }
                return [symbols](IColumn & column, avro::Decoder & decoder)
                {
                    size_t enum_index = decoder.decodeEnum();
                    const auto & enum_symbol = symbols[enum_index];
                    column.insertData(enum_symbol.c_str(), enum_symbol.length());
                    return true;
                };
            }
            if (target.isEnum())
            {
                const auto & enum_type = dynamic_cast<const IDataTypeEnum &>(*target_type);
                Row symbol_mapping;
                for (int i = 0; i < static_cast<int>(root_node->names()); ++i)
                {
                    symbol_mapping.push_back(enum_type.castToValue(root_node->nameAt(i)));
                }
                return [symbol_mapping](IColumn & column, avro::Decoder & decoder)
                {
                    size_t enum_index = decoder.decodeEnum();
                    column.insert(symbol_mapping[enum_index]);
                    return true;
                };
            }
            break;
        case avro::AVRO_FIXED:
        {
            size_t fixed_size = root_node->fixedSize();
            if (canBeDeserializedFromFixed(target_type, fixed_size))
            {
                return [tmp_fixed = std::vector<uint8_t>(fixed_size)](IColumn & column, avro::Decoder & decoder) mutable
                {
                    decoder.decodeFixed(tmp_fixed.size(), tmp_fixed);
                    column.insertData(reinterpret_cast<const char *>(tmp_fixed.data()), tmp_fixed.size());
                    return true;
                };
            }
            if (target.isUUID())
            {
                return [tmp = std::vector<uint8_t>(), fixed_size](IColumn & column, avro::Decoder & decoder) mutable
                {
                    decoder.decodeFixed(fixed_size, tmp);
                    if (tmp.size() != 36)
                        throw Exception(ErrorCodes::CANNOT_PARSE_UUID, "Cannot parse UUID from type Fixed, because it's size ({}) is not equal to the size of UUID (36)", fixed_size);

                    const UUID uuid = parseUUID({reinterpret_cast<const UInt8 *>(tmp.data()), tmp.size()});
                    assert_cast<DataTypeUUID::ColumnType &>(column).insertValue(uuid);
                    return true;
                };
            }
            if (target.isDecimal32())
                return createDecimalDeserializeFn<DataTypeDecimal32>(root_node, target_type, true);
            if (target.isDecimal64())
                return createDecimalDeserializeFn<DataTypeDecimal64>(root_node, target_type, true);
            if (target.isDecimal128())
                return createDecimalDeserializeFn<DataTypeDecimal128>(root_node, target_type, true);
            if (target.isDecimal256())
                return createDecimalDeserializeFn<DataTypeDecimal256>(root_node, target_type, true);
            if (target.isDateTime64())
                return createDecimalDeserializeFn<DataTypeDateTime64>(root_node, target_type, true);
            break;
        }
        case avro::AVRO_SYMBOLIC:
            return createDeserializeFn(avro::resolveSymbol(root_node), target_type);
        case avro::AVRO_RECORD:
        {
            if (target.isTuple())
            {
                const DataTypeTuple & tuple_type = assert_cast<const DataTypeTuple &>(*target_type);
                const auto & nested_types = tuple_type.getElements();
                std::vector<std::pair<DeserializeFn, size_t>> nested_deserializers;
                nested_deserializers.reserve(root_node->leaves());
                if (root_node->leaves() != nested_types.size())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "The number of leaves in record doesn't match the number of elements in tuple");

                for (int i = 0; i != static_cast<int>(root_node->leaves()); ++i)
                {
                    const auto & name = root_node->nameAt(i);
                    size_t pos = tuple_type.getPositionByName(name);
                    auto nested_deserializer = createDeserializeFn(root_node->leafAt(i), nested_types[pos]);
                    nested_deserializers.emplace_back(nested_deserializer, pos);
                }

                return [nested_deserializers](IColumn & column, avro::Decoder & decoder)
                {
                    ColumnTuple & column_tuple = assert_cast<ColumnTuple &>(column);
                    auto nested_columns = column_tuple.getColumns();
                    for (const auto & [nested_deserializer, pos] : nested_deserializers)
                        nested_deserializer(*nested_columns[pos], decoder);
                    return true;
                };
            }
            break;
        }
        case avro::AVRO_MAP:
        {
            if (target.isMap())
            {
                const auto & map_type = assert_cast<const DataTypeMap &>(*target_type);
                const auto & keys_type = map_type.getKeyType();
                const auto & values_type = map_type.getValueType();
                auto keys_source_type = root_node->leafAt(0);
                auto values_source_type = root_node->leafAt(1);
                auto keys_deserializer = [keys_type, this](IColumn & column, avro::Decoder & decoder)
                {
                    String key = decoder.decodeString();
                    ReadBufferFromString buf(key);
                    keys_type->getDefaultSerialization()->deserializeWholeText(column, buf, settings);
                    return true;
                };

                auto values_deserializer = createDeserializeFn(values_source_type, values_type);
                return [keys_deserializer, values_deserializer](IColumn & column, avro::Decoder & decoder)
                {
                    ColumnMap & column_map = assert_cast<ColumnMap &>(column);
                    ColumnArray & column_array = column_map.getNestedColumn();
                    ColumnArray::Offsets & offsets = column_array.getOffsets();
                    ColumnTuple & nested_columns = column_map.getNestedData();
                    IColumn & keys_column = nested_columns.getColumn(0);
                    IColumn & values_column = nested_columns.getColumn(1);
                    size_t total = 0;
                    for (size_t n = decoder.mapStart(); n != 0; n = decoder.mapNext())
                    {
                        total += n;
                        for (size_t i = 0; i < n; ++i)
                        {
                            keys_deserializer(keys_column, decoder);
                            values_deserializer(values_column, decoder);
                        }
                    }
                    offsets.push_back(offsets.back() + total);
                    return true;
                };
            }
            break;
        }
        default:
            break;
    }

    if (target.isNullable())
    {
        auto nested_deserialize = createDeserializeFn(root_node, removeNullable(target_type));
        return [nested_deserialize](IColumn & column, avro::Decoder & decoder)
        {
            ColumnNullable & col = assert_cast<ColumnNullable &>(column);
            nested_deserialize(col.getNestedColumn(), decoder);
            col.getNullMapData().push_back(0);
            return true;
        };
    }

    throw Exception(ErrorCodes::ILLEGAL_COLUMN,
        "Type {} is not compatible with Avro {}:\n{}",
        target_type->getName(), avro::toString(root_node->type()), nodeToJSON(root_node));
}

AvroDeserializer::SkipFn AvroDeserializer::createSkipFn(const avro::NodePtr & root_node)
{
    switch (root_node->type())
    {
        case avro::AVRO_STRING:
            return [](avro::Decoder & decoder) { decoder.skipString(); };
        case avro::AVRO_BYTES:
            return [](avro::Decoder & decoder) { decoder.skipBytes(); };
        case avro::AVRO_INT:
            return [](avro::Decoder & decoder) { decoder.decodeInt(); };
        case avro::AVRO_LONG:
            return [](avro::Decoder & decoder) { decoder.decodeLong(); };
        case avro::AVRO_FLOAT:
            return [](avro::Decoder & decoder) { decoder.decodeFloat(); };
        case avro::AVRO_DOUBLE:
            return [](avro::Decoder & decoder) { decoder.decodeDouble(); };
        case avro::AVRO_BOOL:
            return [](avro::Decoder & decoder) { decoder.decodeBool(); };
        case avro::AVRO_ARRAY:
        {
            auto nested_skip_fn = createSkipFn(root_node->leafAt(0));
            return [nested_skip_fn](avro::Decoder & decoder)
            {
                for (size_t n = decoder.arrayStart(); n != 0; n = decoder.arrayNext())
                {
                    for (size_t i = 0; i < n; ++i)
                    {
                        nested_skip_fn(decoder);
                    }
                }
            };
        }
        case avro::AVRO_UNION:
        {
            std::vector<SkipFn> union_skip_fns;
            union_skip_fns.reserve(root_node->leaves());
            for (int i = 0; i < static_cast<int>(root_node->leaves()); ++i)
            {
                union_skip_fns.push_back(createSkipFn(root_node->leafAt(i)));
            }
            return [union_skip_fns](avro::Decoder & decoder)
            {
                auto index = decoder.decodeUnionIndex();
                if (index >= union_skip_fns.size())
                {
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Union index out of boundary");
                }
                union_skip_fns[index](decoder);
            };
        }
        case avro::AVRO_NULL:
            return [](avro::Decoder & decoder) { decoder.decodeNull(); };
        case avro::AVRO_ENUM:
            return [](avro::Decoder & decoder) { decoder.decodeEnum(); };
        case avro::AVRO_FIXED:
        {
            auto fixed_size = root_node->fixedSize();
            return [fixed_size](avro::Decoder & decoder) { decoder.skipFixed(fixed_size); };
        }
        case avro::AVRO_MAP:
        {
            auto value_skip_fn = createSkipFn(root_node->leafAt(1));
            return [value_skip_fn](avro::Decoder & decoder)
            {
                for (size_t n = decoder.mapStart(); n != 0; n = decoder.mapNext())
                {
                    for (size_t i = 0; i < n; ++i)
                    {
                        decoder.skipString();
                        value_skip_fn(decoder);
                    }
                }
            };
        }
        case avro::AVRO_RECORD:
        {
            std::vector<SkipFn> field_skip_fns;
            field_skip_fns.reserve(root_node->leaves());
            for (int i = 0; i < static_cast<int>(root_node->leaves()); ++i)
            {
                field_skip_fns.push_back(createSkipFn(root_node->leafAt(i)));
            }
            return [field_skip_fns](avro::Decoder & decoder)
            {
                for (const auto & skip_fn : field_skip_fns)
                    skip_fn(decoder);
            };
        }
        case avro::AVRO_SYMBOLIC:
        {
            auto [it, inserted] = symbolic_skip_fn_map.emplace(root_node->name(), SkipFn{});
            if (inserted)
            {
                it->second = createSkipFn(avro::resolveSymbol(root_node));
            }
            return [&skip_fn = it->second](avro::Decoder & decoder)
            {
                skip_fn(decoder);
            };
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unsupported Avro type {} ({})", root_node->name().fullname(), int(root_node->type()));
    }
}

void AvroDeserializer::Action::deserializeNested(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const
{
    /// We should deserialize all nested columns together, because
    /// in avro we have single row Array(Record) and we can
    /// deserialize it once.

    std::vector<ColumnArray::Offsets *> arrays_offsets;
    arrays_offsets.reserve(nested_column_indexes.size());
    std::vector<IColumn *> nested_columns;
    nested_columns.reserve(nested_column_indexes.size());
    for (size_t index : nested_column_indexes)
    {
        ColumnArray & column_array = assert_cast<ColumnArray &>(*columns[index]);
        arrays_offsets.push_back(&column_array.getOffsets());
        nested_columns.push_back(&column_array.getData());
    }

    size_t total = 0;
    for (size_t n = decoder.arrayStart(); n != 0; n = decoder.arrayNext())
    {
        total += n;
        for (size_t i = 0; i < n; ++i)
        {
            for (size_t j = 0; j != nested_deserializers.size(); ++j)
                ext.read_columns[nested_column_indexes[j]] = nested_deserializers[j](*nested_columns[j], decoder);
        }
    }

    for (auto & offsets : arrays_offsets)
        offsets->push_back(offsets->back() + total);
}

static inline std::string concatPath(const std::string & a, const std::string & b)
{
    return a.empty() ? b : a + "." + b;
}

AvroDeserializer::Action AvroDeserializer::createAction(const Block & header, const avro::NodePtr & node, const std::string & current_path)
{
    if (node->type() == avro::AVRO_SYMBOLIC)
    {
        /// continue traversal only if some column name starts with current_path
        auto keep_going = std::any_of(header.begin(), header.end(), [&current_path](const ColumnWithTypeAndName & col)
        {
            return col.name.starts_with(current_path);
        });
        auto resolved_node = avro::resolveSymbol(node);
        if (keep_going)
            return createAction(header, resolved_node, current_path);
        return AvroDeserializer::Action(createSkipFn(resolved_node));
    }

    if (header.has(current_path))
    {
        auto target_column_idx = header.getPositionByName(current_path);
        const auto & column = header.getByPosition(target_column_idx);
        try
        {
            AvroDeserializer::Action action(static_cast<int>(target_column_idx), createDeserializeFn(node, column.type));
            column_found[target_column_idx] = true;
            return action;
        }
        catch (Exception & e)
        {
            e.addMessage("column " + column.name);
            throw;
        }
    }
    else if (node->type() == avro::AVRO_RECORD)
    {
        std::vector<AvroDeserializer::Action> field_actions(node->leaves());
        for (int i = 0; i < static_cast<int>(node->leaves()); ++i)
        {
            const auto & field_node = node->leafAt(i);
            const auto & field_name = node->nameAt(i);
            field_actions[i] = createAction(header, field_node, concatPath(current_path, field_name));
        }
        return AvroDeserializer::Action::recordAction(field_actions);
    }
    else if (node->type() == avro::AVRO_UNION)
    {
        std::vector<AvroDeserializer::Action> branch_actions(node->leaves());
        for (int i = 0; i < static_cast<int>(node->leaves()); ++i)
        {
            const auto & branch_node = node->leafAt(i);
            const auto & branch_name = nodeName(branch_node);
            branch_actions[i] = createAction(header, branch_node, concatPath(current_path, branch_name));
        }
        return AvroDeserializer::Action::unionAction(branch_actions);
    }
    else if (node->type() == avro::AVRO_ARRAY)
    {
        /// If header doesn't have column with current_path name and node is Array(Record),
        /// check if we have a flattened Nested table with such name.
        Names nested_names = Nested::getAllNestedColumnsForTable(header, current_path);
        auto nested_avro_node = node->leafAt(0);
        if (nested_names.empty() || nested_avro_node->type() != avro::AVRO_RECORD)
            return AvroDeserializer::Action(createSkipFn(node));

        /// Check that all nested columns are Arrays.
        std::unordered_map<String, DataTypePtr> nested_types;
        for (const auto & name : nested_names)
        {
            auto type = header.getByName(name).type;
            if (!isArray(type))
                return AvroDeserializer::Action(createSkipFn(node));
            nested_types[Nested::splitName(name).second] = assert_cast<const DataTypeArray *>(type.get())->getNestedType();
        }

        /// Create nested deserializer for each nested column.
        std::vector<DeserializeFn> nested_deserializers;
        std::vector<size_t> nested_indexes;
        for (int i = 0; i != static_cast<int>(nested_avro_node->leaves()); ++i)
        {
            const auto & name = nested_avro_node->nameAt(i);
            if (!nested_types.contains(name))
                return AvroDeserializer::Action(createSkipFn(node));
            size_t nested_column_index = header.getPositionByName(Nested::concatenateName(current_path, name));
            column_found[nested_column_index] = true;
            auto nested_deserializer = createDeserializeFn(nested_avro_node->leafAt(i), nested_types[name]);
            nested_deserializers.emplace_back(nested_deserializer);
            nested_indexes.push_back(nested_column_index);
        }

        return AvroDeserializer::Action(nested_indexes, nested_deserializers);
    }
    else
    {
        return AvroDeserializer::Action(createSkipFn(node));
    }
}

AvroDeserializer::AvroDeserializer(const Block & header, avro::ValidSchema schema, bool allow_missing_fields, bool null_as_default_, const FormatSettings & settings_)
    : null_as_default(null_as_default_), settings(settings_)
{
    const auto & schema_root = schema.root();
    if (schema_root->type() != avro::AVRO_RECORD)
    {
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Root schema must be a record");
    }

    column_found.resize(header.columns());
    row_action = createAction(header, schema_root);
    // fail on missing fields when allow_missing_fields = false
    if (!allow_missing_fields)
    {
        for (size_t i = 0; i < header.columns(); ++i)
        {
            if (!column_found[i])
            {
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Field {} not found in Avro schema", header.getByPosition(i).name);
            }
        }
    }
}

void AvroDeserializer::deserializeRow(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const
{
    size_t row = columns[0]->size() + 1;
    ext.read_columns.assign(columns.size(), false);
    row_action.execute(columns, decoder, ext);
    for (size_t i = 0; i < ext.read_columns.size(); ++i)
    {
        /// Insert default in missing columns.
        if (columns[i]->size() != row)
            columns[i]->insertDefault();
    }
}


AvroRowInputFormat::AvroRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_), format_settings(format_settings_)
{
}

void AvroRowInputFormat::readPrefix()
{
    file_reader_ptr = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*in));
    deserializer_ptr = std::make_unique<AvroDeserializer>(
        output.getHeader(), file_reader_ptr->dataSchema(), format_settings.avro.allow_missing_fields, format_settings.null_as_default, format_settings);
    file_reader_ptr->init();
}

bool AvroRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &ext)
{
    if (file_reader_ptr->hasMore())
    {
        file_reader_ptr->decr();
        deserializer_ptr->deserializeRow(columns, file_reader_ptr->decoder(), ext);
        return true;
    }
    return false;
}

size_t AvroRowInputFormat::countRows(size_t max_block_size)
{
    size_t num_rows = 0;
    while (file_reader_ptr->hasMore() && num_rows < max_block_size)
    {
        file_reader_ptr->decr();
        file_reader_ptr->decoder().drain();
        ++num_rows;
    }

    return num_rows;
}

class AvroConfluentRowInputFormat::SchemaRegistry
{
public:
    explicit SchemaRegistry(const std::string & base_url_, size_t schema_cache_max_size = 1000)
        : base_url(base_url_), schema_cache(schema_cache_max_size)
    {
        if (base_url.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty Schema Registry URL");
    }

    avro::ValidSchema getSchema(uint32_t id)
    {
        auto [schema, loaded] = schema_cache.getOrSet(
            id,
            [this, id](){ return std::make_shared<avro::ValidSchema>(fetchSchema(id)); }
        );
        return *schema;
    }

private:
    avro::ValidSchema fetchSchema(uint32_t id)
    {
        try
        {
            try
            {
                Poco::URI url(base_url, base_url.getPath() + "/schemas/ids/" + std::to_string(id));
                LOG_TRACE((getLogger("AvroConfluentRowInputFormat")), "Fetching schema id = {} from url {}", id, url.toString());

                /// One second for connect/send/receive. Just in case.
                auto timeouts = ConnectionTimeouts()
                    .withConnectionTimeout(1)
                    .withSendTimeout(1)
                    .withReceiveTimeout(1);

                Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, url.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
                request.setHost(url.getHost());

                if (!url.getUserInfo().empty())
                {
                    Poco::Net::HTTPCredentials http_credentials;
                    Poco::Net::HTTPBasicCredentials http_basic_credentials;

                    http_credentials.fromUserInfo(url.getUserInfo());

                    std::string decoded_username;
                    Poco::URI::decode(http_credentials.getUsername(), decoded_username);
                    http_basic_credentials.setUsername(decoded_username);

                    if (!http_credentials.getPassword().empty())
                    {
                        std::string decoded_password;
                        Poco::URI::decode(http_credentials.getPassword(), decoded_password);
                        http_basic_credentials.setPassword(decoded_password);
                    }

                    http_basic_credentials.authenticate(request);
                }

                auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, url, timeouts);
                session->sendRequest(request);

                Poco::Net::HTTPResponse response;
                std::istream * response_body = receiveResponse(*session, request, response, false);

                Poco::JSON::Parser parser;
                auto json_body = parser.parse(*response_body).extract<Poco::JSON::Object::Ptr>();


                auto schema = json_body->getValue<std::string>("schema");
                LOG_TRACE((getLogger("AvroConfluentRowInputFormat")), "Successfully fetched schema id = {}\n{}", id, schema);
                return avro::compileJsonSchemaFromString(schema);
            }
            catch (const Exception &)
            {
                throw;
            }
            catch (const Poco::Exception & e)
            {
                throw Exception(Exception::CreateFromPocoTag{}, e);
            }
            catch (const avro::Exception & e)
            {
                throw Exception::createDeprecated(e.what(), ErrorCodes::INCORRECT_DATA);
            }
        }
        catch (Exception & e)
        {
            e.addMessage("while fetching schema id = " + std::to_string(id));
            throw;
        }
    }

    Poco::URI base_url;
    CacheBase<uint32_t, avro::ValidSchema> schema_cache;
};

using ConfluentSchemaRegistry = AvroConfluentRowInputFormat::SchemaRegistry;
#define SCHEMA_REGISTRY_CACHE_MAX_SIZE 1000
/// Cache of Schema Registry URL -> SchemaRegistry
static CacheBase<std::string, ConfluentSchemaRegistry> schema_registry_cache(SCHEMA_REGISTRY_CACHE_MAX_SIZE);

static std::shared_ptr<ConfluentSchemaRegistry> getConfluentSchemaRegistry(const FormatSettings & format_settings)
{
    const auto & base_url = format_settings.avro.schema_registry_url;
    auto [schema_registry, loaded] = schema_registry_cache.getOrSet(
        base_url,
        [base_url]()
        {
            return std::make_shared<ConfluentSchemaRegistry>(base_url);
        }
    );
    return schema_registry;
}

static uint32_t readConfluentSchemaId(ReadBuffer & in)
{
    uint8_t magic;
    uint32_t schema_id;

    try
    {
        readBinaryBigEndian(magic, in);
        readBinaryBigEndian(schema_id, in);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
        {
            /* empty or incomplete message without Avro Confluent magic number or schema id */
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing AvroConfluent magic byte or schema identifier.");
        }
        throw;
    }

    if (magic != 0x00)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid magic byte before AvroConfluent schema identifier. "
            "Must be zero byte, found {} instead", int(magic));
    }

    return schema_id;
}

AvroConfluentRowInputFormat::AvroConfluentRowInputFormat(
    const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_)
    , schema_registry(getConfluentSchemaRegistry(format_settings_))
    , format_settings(format_settings_)

{
}

void AvroConfluentRowInputFormat::readPrefix()
{
    input_stream = std::make_unique<AvroInputStreamReadBufferAdapter>(*in);
    decoder = avro::binaryDecoder();
    decoder->init(*input_stream);
}

bool AvroConfluentRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (in->eof())
    {
        return false;
    }
    // skip tombstone records (kafka messages with null value)
    if (in->available() == 0)
    {
        return false;
    }
    SchemaId schema_id = readConfluentSchemaId(*in);
    const auto & deserializer = getOrCreateDeserializer(schema_id);
    deserializer.deserializeRow(columns, *decoder, ext);
    decoder->drain();
    return true;
}

void AvroConfluentRowInputFormat::syncAfterError()
{
    // skip until the end of current kafka message
    in->tryIgnore(in->available());
}

const AvroDeserializer & AvroConfluentRowInputFormat::getOrCreateDeserializer(SchemaId schema_id)
{
    auto it = deserializer_cache.find(schema_id);
    if (it == deserializer_cache.end())
    {
        auto schema = schema_registry->getSchema(schema_id);
        AvroDeserializer deserializer(
            output.getHeader(), schema, format_settings.avro.allow_missing_fields, format_settings.null_as_default, format_settings);
        it = deserializer_cache.emplace(schema_id, deserializer).first;
    }
    return it->second;
}

AvroSchemaReader::AvroSchemaReader(ReadBuffer & in_, bool confluent_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), confluent(confluent_), format_settings(format_settings_)
{
}

NamesAndTypesList AvroSchemaReader::readSchema()
{
    avro::NodePtr root_node;
    if (confluent)
    {
        UInt32 schema_id = readConfluentSchemaId(in);
        root_node = getConfluentSchemaRegistry(format_settings)->getSchema(schema_id).root();
    }
    else
    {
        auto file_reader_ptr = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(in));
        root_node = file_reader_ptr->dataSchema().root();
    }

    if (root_node->type() != avro::Type::AVRO_RECORD)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Root schema must be a record");

    NamesAndTypesList names_and_types;
    for (int i = 0; i != static_cast<int>(root_node->leaves()); ++i)
        names_and_types.emplace_back(root_node->nameAt(i), avroNodeToDataType(root_node->leafAt(i)));

    return names_and_types;
}

DataTypePtr AvroSchemaReader::avroNodeToDataType(avro::NodePtr node)
{
    switch (node->type())
    {
        case avro::Type::AVRO_INT:
        {
            if (node->logicalType().type() == avro::LogicalType::DATE)
                return {std::make_shared<DataTypeDate32>()};

            return {std::make_shared<DataTypeInt32>()};
        }
        case avro::Type::AVRO_LONG:
        {
            auto logical_type = node->logicalType();
            if (logical_type.type() == avro::LogicalType::TIMESTAMP_MILLIS)
                return {std::make_shared<DataTypeDateTime64>(3)};
            if (logical_type.type() == avro::LogicalType::TIMESTAMP_MICROS)
                return {std::make_shared<DataTypeDateTime64>(6)};

            return std::make_shared<DataTypeInt64>();
        }
        case avro::Type::AVRO_BOOL:
            return DataTypeFactory::instance().get("Bool");
        case avro::Type::AVRO_FLOAT:
            return std::make_shared<DataTypeFloat32>();
        case avro::Type::AVRO_DOUBLE:
            return std::make_shared<DataTypeFloat64>();
        case avro::Type::AVRO_STRING: [[fallthrough]];
        case avro::Type::AVRO_BYTES:
        {
            auto logical_type = node->logicalType();
            if (logical_type.type() == avro::LogicalType::UUID)
                return std::make_shared<DataTypeUUID>();

            if (logical_type.type() == avro::LogicalType::DECIMAL)
                return createDecimal<DataTypeDecimal>(logical_type.precision(), logical_type.scale());

            return std::make_shared<DataTypeString>();
        }
        case avro::Type::AVRO_ENUM:
        {
            if (node->names() < 128)
            {
                EnumValues<Int8>::Values values;
                for (int i = 0; i != static_cast<int>(node->names()); ++i)
                    values.emplace_back(node->nameAt(i), i);
                return std::make_shared<DataTypeEnum8>(std::move(values));
            }
            if (node->names() < 32768)
            {
                EnumValues<Int16>::Values values;
                for (int i = 0; i != static_cast<int>(node->names()); ++i)
                    values.emplace_back(node->nameAt(i), i);
                return std::make_shared<DataTypeEnum16>(std::move(values));
            }

            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ClickHouse supports only 8 and 16-bit Enum.");
        }
        case avro::Type::AVRO_FIXED:
        {
            auto logical_type = node->logicalType();
            if (logical_type.type() == avro::LogicalType::UUID)
                return std::make_shared<DataTypeUUID>();

            if (logical_type.type() == avro::LogicalType::DECIMAL)
                return createDecimal<DataTypeDecimal>(logical_type.precision(), logical_type.scale());

            return std::make_shared<DataTypeFixedString>(node->fixedSize());
        }
        case avro::Type::AVRO_ARRAY:
            return std::make_shared<DataTypeArray>(avroNodeToDataType(node->leafAt(0)));
        case avro::Type::AVRO_NULL:
            return std::make_shared<DataTypeNothing>();
        case avro::Type::AVRO_UNION:
            if (node->leaves() == 1)
            {
                return avroNodeToDataType(node->leafAt(0));
            }
            else if (
                node->leaves() == 2
                && (node->leafAt(0)->type() == avro::Type::AVRO_NULL || node->leafAt(1)->type() == avro::Type::AVRO_NULL))
            {
                int nested_leaf_index = node->leafAt(0)->type() == avro::Type::AVRO_NULL ? 1 : 0;
                auto nested_type = avroNodeToDataType(node->leafAt(nested_leaf_index));
                return nested_type->canBeInsideNullable() ? makeNullable(nested_type) : nested_type;
            }
            /// FIXME Support UNION has more than two datatypes.
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Avro type  UNION is not supported for inserting.");
        case avro::Type::AVRO_SYMBOLIC:
            return avroNodeToDataType(avro::resolveSymbol(node));
        case avro::Type::AVRO_RECORD:
        {
            DataTypes nested_types;
            nested_types.reserve(node->leaves());
            Names nested_names;
            nested_names.reserve(node->leaves());
            for (int i = 0; i != static_cast<int>(node->leaves()); ++i)
            {
                nested_types.push_back(avroNodeToDataType(node->leafAt(i)));
                nested_names.push_back(node->nameAt(i));
            }
            return std::make_shared<DataTypeTuple>(nested_types, nested_names);
        }
        case avro::Type::AVRO_MAP:
            return std::make_shared<DataTypeMap>(avroNodeToDataType(node->leafAt(0)), avroNodeToDataType(node->leafAt(1)));
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Avro column {} is not supported for inserting.", nodeName(node));
    }
}

void registerInputFormatAvro(FormatFactory & factory)
{
    factory.registerInputFormat("Avro", [](
        ReadBuffer & buf,
        const Block & sample,
        const RowInputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<AvroRowInputFormat>(sample, buf, params, settings);
    });

    factory.markFormatSupportsSubsetOfColumns("Avro");

    factory.registerInputFormat("AvroConfluent",[](
        ReadBuffer & buf,
        const Block & sample,
        const RowInputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<AvroConfluentRowInputFormat>(sample, buf, params, settings);
    });

    factory.markFormatSupportsSubsetOfColumns("AvroConfluent");
}

void registerAvroSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("Avro", [](ReadBuffer & buf, const FormatSettings & settings)
    {
           return std::make_shared<AvroSchemaReader>(buf, false, settings);
    });

    factory.registerSchemaReader("AvroConfluent", [](ReadBuffer & buf, const FormatSettings & settings)
    {
        return std::make_shared<AvroSchemaReader>(buf, true, settings);
    });

}


}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatAvro(FormatFactory &)
{
}

void registerAvroSchemaReader(FormatFactory &) {}
}

#endif
