#include <IO/ReadBufferFromString.h>

#include <Common/checkStackSize.h>
#include <Core/CaseAwareBlockNameMap.h>

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/BSONTypes.h>
#include <Formats/EscapingRuleUtils.h>
#include <Processors/Formats/Impl/BSONEachRowRowInputFormat.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <DataTypes/Serializations/SerializationMap.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int UNKNOWN_TYPE;
    extern const int TYPE_MISMATCH;
    extern const int TOO_DEEP_RECURSION;
}

namespace
{
    enum
    {
        UNKNOWN_FIELD = size_t(-1),
        NOT_INITIALIZED = size_t(-2)
    };
}

BSONEachRowRowInputFormat::BSONEachRowRowInputFormat(
    ReadBuffer & in_, SharedHeader header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, std::move(params_))
    , format_settings(format_settings_)
    , name_map(format_settings_.input_format_column_matching_case_sensitivity)
    , prev_positions(header_->columns(), {std::string_view{}, NOT_INITIALIZED})
    , types(header_->getDataTypes())
{
    name_map.initFromBlock(getPort().getHeader());
}

inline size_t BSONEachRowRowInputFormat::columnIndex(std::string_view name, size_t key_index)
{
    /// Optimization by caching the order of fields (which is almost always the same)
    /// and a quick check to match the next expected field, instead of searching the hash table.

    if (prev_positions.size() > key_index && prev_positions[key_index].second != NOT_INITIALIZED
        && name_map.equal(name, prev_positions[key_index].first))
    {
        return prev_positions[key_index].second;
    }

    auto position = name_map.get(name);
    if (position != CaseAwareBlockNameMap::NOT_FOUND)
    {
        if (key_index < prev_positions.size() && position < getPort().getHeader().columns())
            prev_positions[key_index] = {getPort().getHeader().getByPosition(position).name, position};

        return position;
    }
    return UNKNOWN_FIELD;
}

/// Read the field name. Resulting std::string_view is valid only before next read from buf.
static std::string_view readBSONKeyName(ReadBuffer & in, String & key_holder)
{
    // This is just an optimization: try to avoid copying the name into key_holder

    if (!in.eof())
    {
        char * next_pos = find_first_symbols<0>(in.position(), in.buffer().end());

        if (next_pos != in.buffer().end())
        {
            std::string_view res(in.position(), next_pos - in.position());
            in.position() = next_pos + 1;
            return res;
        }
    }

    key_holder.clear();
    readNullTerminated(key_holder, in);
    return key_holder;
}

static UInt8 readBSONType(ReadBuffer & in)
{
    UInt8 type = 0;
    readBinary(type, in);
    return type;
}

static size_t readBSONSize(ReadBuffer & in)
{
    BSONSizeT size = 0;
    readBinaryLittleEndian(size, in);
    return size;
}

template <typename T>
static void readAndInsertInteger(ReadBuffer & in, IColumn & column, const DataTypePtr & data_type, BSONType bson_type)
{
    /// We allow to read any integer into any integer column.
    /// For example we can read BSON Int32 into ClickHouse UInt8.

    if (bson_type == BSONType::INT32)
    {
        UInt32 value = 0;
        readBinaryLittleEndian(value, in);
        assert_cast<ColumnVector<T> &>(column).insertValue(static_cast<T>(value));
    }
    else if (bson_type == BSONType::INT64)
    {
        UInt64 value = 0;
        readBinaryLittleEndian(value, in);
        assert_cast<ColumnVector<T> &>(column).insertValue(static_cast<T>(value));
    }
    else if (bson_type == BSONType::BOOL)
    {
        UInt8 value = 0;
        readBinaryLittleEndian(value, in);
        assert_cast<ColumnVector<T> &>(column).insertValue(static_cast<T>(value));
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into column with type {}",
                        getBSONTypeName(bson_type), data_type->getName());
    }
}

static void readAndInsertIPv4(ReadBuffer & in, IColumn & column, BSONType bson_type)
{
    /// We expect BSON type Int32 as IPv4 value.
    if (bson_type != BSONType::INT32)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON Int32 into column with type IPv4");

    UInt32 value = 0;
    readBinaryLittleEndian(value, in);
    assert_cast<ColumnIPv4 &>(column).insertValue(IPv4(value));
}

template <typename T>
static void readAndInsertDouble(ReadBuffer & in, IColumn & column, const DataTypePtr & data_type, BSONType bson_type)
{
    if (bson_type != BSONType::DOUBLE)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into column with type {}",
                        getBSONTypeName(bson_type), data_type->getName());

    Float64 value = 0;
    readBinaryLittleEndian(value, in);
    assert_cast<ColumnVector<T> &>(column).insertValue(static_cast<T>(value));
}

template <typename DecimalType, BSONType expected_bson_type>
static void readAndInsertSmallDecimal(ReadBuffer & in, IColumn & column, const DataTypePtr & data_type, BSONType bson_type)
{
    if (bson_type != expected_bson_type)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into column with type {}",
                        getBSONTypeName(bson_type), data_type->getName());

    DecimalType value{};
    readBinaryLittleEndian(value, in);
    assert_cast<ColumnDecimal<DecimalType> &>(column).insertValue(value);
}

static void readAndInsertDateTime64(ReadBuffer & in, IColumn & column, BSONType bson_type)
{
    if (bson_type != BSONType::INT64 && bson_type != BSONType::DATETIME)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into DateTime64 column", getBSONTypeName(bson_type));

    DateTime64 value;
    readBinaryLittleEndian(value, in);
    assert_cast<DataTypeDateTime64::ColumnType &>(column).insertValue(value);
}

template <typename ColumnType>
static void readAndInsertBigInteger(ReadBuffer & in, IColumn & column, const DataTypePtr & data_type, BSONType bson_type)
{
    if (bson_type != BSONType::BINARY)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into column with type {}",
                        getBSONTypeName(bson_type), data_type->getName());

    auto size = readBSONSize(in);
    auto subtype = getBSONBinarySubtype(readBSONType(in));
    if (subtype != BSONBinarySubtype::BINARY)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON Binary subtype {} into column with type {}",
                        getBSONBinarySubtypeName(subtype), data_type->getName());

    using ValueType = typename ColumnType::ValueType;

    if (size != sizeof(ValueType))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Cannot parse value of type {}, size of binary data is not equal to the binary size of expected value: {} != {}",
            data_type->getName(),
            size,
            sizeof(ValueType));

    ValueType value;
    readBinaryLittleEndian(value, in);
    assert_cast<ColumnType &>(column).insertValue(value);
}

template <bool is_fixed_string>
static void readAndInsertStringImpl(ReadBuffer & in, IColumn & column, size_t size)
{
    if constexpr (is_fixed_string)
    {
        auto & fixed_string_column = assert_cast<ColumnFixedString &>(column);
        size_t n = fixed_string_column.getN();
        if (size > n)
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string for FixedString column");

        auto & data = fixed_string_column.getChars();

        size_t old_size = data.size();
        data.resize_fill(old_size + n);

        try
        {
            in.readStrict(reinterpret_cast<char *>(data.data() + old_size), size);
        }
        catch (...)
        {
            /// Restore column state in case of any exception.
            data.resize_assume_reserved(old_size);
            throw;
        }
    }
    else
    {
        auto & column_string = assert_cast<ColumnString &>(column);
        auto & data = column_string.getChars();
        auto & offsets = column_string.getOffsets();

        size_t old_chars_size = data.size();
        size_t offset = old_chars_size + size;
        offsets.push_back(offset);

        try
        {
            data.resize(offset);
            in.readStrict(reinterpret_cast<char *>(&data[offset - size]), size);
        }
        catch (...)
        {
            /// Restore column state in case of any exception.
            offsets.pop_back();
            data.resize_assume_reserved(old_chars_size);
            throw;
        }
    }
}

template <bool is_fixed_string>
static void readAndInsertString(ReadBuffer & in, IColumn & column, BSONType bson_type)
{
    if (bson_type == BSONType::STRING || bson_type == BSONType::SYMBOL || bson_type == BSONType::JAVA_SCRIPT_CODE)
    {
        auto size = readBSONSize(in);
        if (size == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect size of a string (zero) in BSON");
        readAndInsertStringImpl<is_fixed_string>(in, column, size - 1);
        assertChar(0, in);
    }
    else if (bson_type == BSONType::BINARY)
    {
        auto size = readBSONSize(in);
        auto subtype = getBSONBinarySubtype(readBSONType(in));
        if (subtype == BSONBinarySubtype::BINARY || subtype == BSONBinarySubtype::BINARY_OLD)
            readAndInsertStringImpl<is_fixed_string>(in, column, size);
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Cannot insert BSON Binary subtype {} into String column",
                getBSONBinarySubtypeName(subtype));
    }
    else if (bson_type == BSONType::OBJECT_ID)
    {
        readAndInsertStringImpl<is_fixed_string>(in, column, BSON_OBJECT_ID_SIZE);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into String column", getBSONTypeName(bson_type));
    }
}

static void readAndInsertIPv6(ReadBuffer & in, IColumn & column, BSONType bson_type)
{
    if (bson_type != BSONType::BINARY)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into IPv6 column", getBSONTypeName(bson_type));

    auto size = readBSONSize(in);
    auto subtype = getBSONBinarySubtype(readBSONType(in));
    if (subtype != BSONBinarySubtype::BINARY)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON Binary subtype {} into IPv6 column", getBSONBinarySubtypeName(subtype));

    if (size != sizeof(IPv6))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Cannot parse value of type IPv6, size of binary data is not equal to the binary size of IPv6 value: {} != {}",
            size,
            sizeof(IPv6));

    IPv6 value;
    readBinary(value, in);
    assert_cast<ColumnIPv6 &>(column).insertValue(value);
}


static void readAndInsertUUID(ReadBuffer & in, IColumn & column, BSONType bson_type)
{
    if (bson_type != BSONType::BINARY)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into UUID column", getBSONTypeName(bson_type));

    auto size = readBSONSize(in);
    auto subtype = getBSONBinarySubtype(readBSONType(in));
    if (subtype != BSONBinarySubtype::UUID && subtype != BSONBinarySubtype::UUID_OLD)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Cannot insert BSON Binary subtype {} into UUID column",
            getBSONBinarySubtypeName(subtype));

    if (size != sizeof(UUID))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Cannot parse value of type UUID, size of binary data is not equal to the binary size of UUID value: {} != {}",
            size,
            sizeof(UUID));

    UUID value;
    readBinaryLittleEndian(value, in);
    assert_cast<ColumnUUID &>(column).insertValue(value);
}

void BSONEachRowRowInputFormat::readArray(IColumn & column, const DataTypePtr & data_type, BSONType bson_type)
{
    /// Nested Array/Tuple/Map recurse through readField; the depth is bounded by the declared column
    /// type, but guard the native stack here (on container entry, not on every primitive field)
    /// against a pathologically deep declared type.
    checkStackSize();

    if (bson_type != BSONType::ARRAY)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into Array column", getBSONTypeName(bson_type));

    const auto * data_type_array = assert_cast<const DataTypeArray *>(data_type.get());
    const auto & nested_type = data_type_array->getNestedType();
    auto & array_column = assert_cast<ColumnArray &>(column);
    auto & nested_column = array_column.getData();

    size_t document_start = in->count();
    BSONSizeT document_size = 0;
    readBinaryLittleEndian(document_size, *in);
    if (document_size < sizeof(BSONSizeT) + sizeof(BSON_DOCUMENT_END))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid document size: {}", document_size);

    auto read_array = [&]()
    {
        while (in->count() - document_start + sizeof(BSON_DOCUMENT_END) != document_size)
        {
            auto nested_bson_type = getBSONType(readBSONType(*in));
            readBSONKeyName(*in, current_key_name);
            readField(nested_column, nested_type, nested_bson_type);
        }

        assertChar(BSON_DOCUMENT_END, *in);
        array_column.getOffsets().push_back(array_column.getData().size());
    };

    SerializationArray::readArraySafe(column, read_array);
}

void BSONEachRowRowInputFormat::readTuple(IColumn & column, const DataTypePtr & data_type, BSONType bson_type)
{
    checkStackSize();

    if (bson_type != BSONType::ARRAY && bson_type != BSONType::DOCUMENT)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into Tuple column", getBSONTypeName(bson_type));

    /// When BSON type is ARRAY, names in nested document are not useful
    /// (most likely they are just sequential numbers).
    bool use_key_names = bson_type == BSONType::DOCUMENT;

    const auto * data_type_tuple = assert_cast<const DataTypeTuple *>(data_type.get());
    auto & tuple_column = assert_cast<ColumnTuple &>(column);
    size_t read_nested_columns = 0;

    size_t document_start = in->count();
    BSONSizeT document_size = 0;
    readBinaryLittleEndian(document_size, *in);
    if (document_size < sizeof(BSONSizeT) + sizeof(BSON_DOCUMENT_END))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid document size: {}", document_size);

    auto read_tuple = [&]()
    {
        while (in->count() - document_start + sizeof(BSON_DOCUMENT_END) != document_size)
        {
            auto nested_bson_type = getBSONType(readBSONType(*in));
            auto name = readBSONKeyName(*in, current_key_name);

            size_t index = read_nested_columns;
            if (use_key_names)
            {
                auto try_get_index = data_type_tuple->tryGetPositionByName(name);
                if (!try_get_index)
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Cannot parse tuple column with type {} from BSON array/embedded document field: "
                        "tuple doesn't have element with name \"{}\"",
                        data_type->getName(),
                        name);
                index = *try_get_index;
            }

            if (index >= data_type_tuple->getElements().size())
                throw Exception(
                                ErrorCodes::INCORRECT_DATA,
                                "Cannot parse tuple column with type {} from BSON array/embedded document field: "
                                "the number of fields BSON document exceeds the number of fields in tuple",
                                data_type->getName());

            readField(tuple_column.getColumn(index), data_type_tuple->getElement(index), nested_bson_type);
            ++read_nested_columns;
        }

        assertChar(BSON_DOCUMENT_END, *in);

        const auto elements_size = data_type_tuple->getElements().size();
        if (read_nested_columns != elements_size)
            throw Exception(
                            ErrorCodes::INCORRECT_DATA,
                            "Cannot parse tuple column with type {} from BSON array/embedded document field, "
                            "the number of fields in tuple and BSON document doesn't match: {} != {}",
                            data_type->getName(),
                            elements_size,
                            read_nested_columns);
    };

    SerializationTuple::readElementsSafe(column, read_tuple);
}

void BSONEachRowRowInputFormat::readMap(IColumn & column, const DataTypePtr & data_type, BSONType bson_type)
{
    checkStackSize();

    if (bson_type != BSONType::DOCUMENT)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert BSON {} into Map column", getBSONTypeName(bson_type));

    const auto * data_type_map = assert_cast<const DataTypeMap *>(data_type.get());
    const auto & key_data_type = data_type_map->getKeyType();
    const auto & value_data_type = data_type_map->getValueType();
    auto & column_map = assert_cast<ColumnMap &>(column);
    auto & key_column = column_map.getNestedData().getColumn(0);
    auto & value_column = column_map.getNestedData().getColumn(1);
    auto & offsets = column_map.getNestedColumn().getOffsets();

    size_t document_start = in->count();
    BSONSizeT document_size = 0;
    readBinaryLittleEndian(document_size, *in);
    if (document_size < sizeof(BSONSizeT) + sizeof(BSON_DOCUMENT_END))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid document size: {}", document_size);

    auto read_map = [&]()
    {
        while (in->count() - document_start + sizeof(BSON_DOCUMENT_END) != document_size)
        {
            auto nested_bson_type = getBSONType(readBSONType(*in));
            auto name = readBSONKeyName(*in, current_key_name);
            ReadBufferFromMemory buf(name);
            key_data_type->getDefaultSerialization()->deserializeWholeText(key_column, buf, format_settings);
            readField(value_column, value_data_type, nested_bson_type);
        }

        assertChar(BSON_DOCUMENT_END, *in);
        offsets.push_back(key_column.size());
    };

    SerializationMap::readMapSafe(column, read_map);
}


bool BSONEachRowRowInputFormat::readField(IColumn & column, const DataTypePtr & data_type, BSONType bson_type)
{
    if (bson_type == BSONType::NULL_VALUE)
    {
        if (data_type->isNullable())
        {
            column.insertDefault();
            return true;
        }

        if (!format_settings.null_as_default)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Cannot insert BSON Null value into non-nullable column with type {}",
                            data_type->getName());

        column.insertDefault();
        return false;
    }

    switch (data_type->getTypeId())
    {
        case TypeIndex::Nullable:
        {
            auto & nullable_column = assert_cast<ColumnNullable &>(column);
            auto & nested_column = nullable_column.getNestedColumn();
            const auto & nested_type = assert_cast<const DataTypeNullable *>(data_type.get())->getNestedType();
            /// Read into the nested column first. If `readField` throws, we must not leave
            /// the `null_map` out of sync with the nested column — otherwise subsequent
            /// rollback (e.g. `SerializationArray::readArraySafe` calling
            /// `ColumnNullable::popBack`) would trip an assertion because
            /// `ColumnNullable::size()` reflects `null_map.size()`.
            auto result = readField(nested_column, nested_type, bson_type);
            nullable_column.getNullMapColumn().insertValue(0);
            return result;
        }
        case TypeIndex::LowCardinality:
        {
            auto & lc_column = assert_cast<ColumnLowCardinality &>(column);
            auto tmp_column = lc_column.getDictionary().getNestedColumn()->cloneEmpty();
            const auto & dict_type = assert_cast<const DataTypeLowCardinality *>(data_type.get())->getDictionaryType();
            auto res = readField(*tmp_column, dict_type, bson_type);
            lc_column.insertFromFullColumn(*tmp_column, 0);
            return res;
        }
        case TypeIndex::Enum8: [[fallthrough]];
        case TypeIndex::Int8:
        {
            readAndInsertInteger<Int8>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::UInt8:
        {
            readAndInsertInteger<UInt8>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Enum16: [[fallthrough]];
        case TypeIndex::Int16:
        {
            readAndInsertInteger<Int16>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
        {
            readAndInsertInteger<UInt16>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::Int32:
        {
            readAndInsertInteger<Int32>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
        {
            readAndInsertInteger<UInt32>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Int64:
        {
            readAndInsertInteger<Int64>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::UInt64:
        {
            readAndInsertInteger<UInt64>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Int128:
        {
            readAndInsertBigInteger<ColumnInt128>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::UInt128:
        {
            readAndInsertBigInteger<ColumnUInt128>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Int256:
        {
            readAndInsertBigInteger<ColumnInt256>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::UInt256:
        {
            readAndInsertBigInteger<ColumnUInt256>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Float32:
        {
            readAndInsertDouble<Float32>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Float64:
        {
            readAndInsertDouble<Float64>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Decimal32:
        {
            readAndInsertSmallDecimal<Decimal32, BSONType::INT32>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Decimal64:
        {
            readAndInsertSmallDecimal<Decimal64, BSONType::INT64>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Decimal128:
        {
            readAndInsertBigInteger<ColumnDecimal<Decimal128>>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Decimal256:
        {
            readAndInsertBigInteger<ColumnDecimal<Decimal256>>(*in, column, data_type, bson_type);
            return true;
        }
        case TypeIndex::DateTime64:
        {
            readAndInsertDateTime64(*in, column, bson_type);
            return true;
        }
        case TypeIndex::FixedString:
        {
            readAndInsertString<true>(*in, column, bson_type);
            return true;
        }
        case TypeIndex::String:
        {
            readAndInsertString<false>(*in, column, bson_type);
            return true;
        }
        case TypeIndex::IPv4:
        {
            readAndInsertIPv4(*in, column, bson_type);
            return true;
        }
        case TypeIndex::IPv6:
        {
            readAndInsertIPv6(*in, column, bson_type);
            return true;
        }
        case TypeIndex::UUID:
        {
            readAndInsertUUID(*in, column, bson_type);
            return true;
        }
        case TypeIndex::Array:
        {
            readArray(column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Tuple:
        {
            readTuple(column, data_type, bson_type);
            return true;
        }
        case TypeIndex::Map:
        {
            readMap(column, data_type, bson_type);
            return true;
        }
        default:
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported for output in BSON format", data_type->getName());
        }
    }
}

static void skipBSONField(ReadBuffer & in, BSONType type)
{
    switch (type)
    {
        case BSONType::DOUBLE:
        {
            in.ignore(sizeof(Float64));
            break;
        }
        case BSONType::BOOL:
        {
            in.ignore(sizeof(UInt8));
            break;
        }
        case BSONType::INT64: [[fallthrough]];
        case BSONType::DATETIME: [[fallthrough]];
        case BSONType::TIMESTAMP:
        {
            in.ignore(sizeof(UInt64));
            break;
        }
        case BSONType::INT32:
        {
            in.ignore(sizeof(Int32));
            break;
        }
        case BSONType::JAVA_SCRIPT_CODE: [[fallthrough]];
        case BSONType::SYMBOL: [[fallthrough]];
        case BSONType::STRING:
        {
            BSONSizeT size = 0;
            readBinaryLittleEndian(size, in);
            in.ignore(size);
            break;
        }
        case BSONType::DOCUMENT: [[fallthrough]];
        case BSONType::ARRAY:
        {
            BSONSizeT size = 0;
            readBinaryLittleEndian(size, in);
            if (size < sizeof(BSONSizeT) + sizeof(BSON_DOCUMENT_END))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid document size: {}", size);
            in.ignore(size - sizeof(size));
            break;
        }
        case BSONType::BINARY:
        {
            BSONSizeT size = 0;
            readBinaryLittleEndian(size, in);
            in.ignore(size + 1);
            break;
        }
        case BSONType::MIN_KEY: [[fallthrough]];
        case BSONType::MAX_KEY: [[fallthrough]];
        case BSONType::UNDEFINED: [[fallthrough]];
        case BSONType::NULL_VALUE:
        {
            break;
        }
        case BSONType::OBJECT_ID:
        {
            in.ignore(BSON_OBJECT_ID_SIZE);
            break;
        }
        case BSONType::REGEXP:
        {
            skipNullTerminated(in);
            skipNullTerminated(in);
            break;
        }
        case BSONType::DB_POINTER:
        {
            BSONSizeT size = 0;
            readBinaryLittleEndian(size, in);
            in.ignore(size + BSON_DB_POINTER_SIZE);
            break;
        }
        case BSONType::JAVA_SCRIPT_CODE_W_SCOPE:
        {
            BSONSizeT size = 0;
            readBinaryLittleEndian(size, in);
            if (size < sizeof(BSONSizeT))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid java code_w_scope size: {}", size);
            in.ignore(size - sizeof(size));
            break;
        }
        case BSONType::DECIMAL128:
        {
            in.ignore(16);
            break;
        }
    }
}

void BSONEachRowRowInputFormat::skipUnknownField(BSONType type, const String & key_name)
{
    if (!format_settings.skip_unknown_fields)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown field found while parsing BSONEachRow format: {}", key_name);

    skipBSONField(*in, type);
}

void BSONEachRowRowInputFormat::syncAfterError()
{
    /// Skip all remaining bytes in current document
    size_t already_read_bytes = in->count() - current_document_start;
    in->ignore(current_document_size - already_read_bytes);
}

bool BSONEachRowRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    size_t num_columns = columns.size();

    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);

    if (in->eof())
        return false;

    size_t key_index = 0;

    current_document_start = in->count();
    readBinaryLittleEndian(current_document_size, *in);
    if (current_document_size < sizeof(BSONSizeT) + sizeof(BSON_DOCUMENT_END))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid document size: {}", current_document_size);

    while (in->count() - current_document_start + sizeof(BSON_DOCUMENT_END) != current_document_size)
    {
        auto type = getBSONType(readBSONType(*in));
        auto name = readBSONKeyName(*in, current_key_name);
        auto index = columnIndex(name, key_index);

        if (index == UNKNOWN_FIELD)
        {
            current_key_name.assign(name.data(), name.size());
            skipUnknownField(BSONType(type), current_key_name);
        }
        else
        {
            if (seen_columns[index])
                throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate field found while parsing BSONEachRow format: {}", name);

            seen_columns[index] = true;
            read_columns[index] = readField(*columns[index], types[index], BSONType(type));
        }

        ++key_index;
    }

    assertChar(BSON_DOCUMENT_END, *in);

    const auto & header = getPort().getHeader();
    /// Fill non-visited columns with the default values.
    for (size_t i = 0; i < num_columns; ++i)
        if (!seen_columns[i])
        {
            const auto & type = header.getByPosition(i).type;
            if (format_settings.force_null_for_omitted_fields && !isNullableOrLowCardinalityNullable(type))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot insert NULL value into a column of type '{}' at index {}", type->getName(), i);
            type->insertDefaultInto(*columns[i]);
        }

    if (format_settings.defaults_for_omitted_fields)
        ext.read_columns = read_columns;
    else
        ext.read_columns.assign(read_columns.size(), true);

    return true;
}

void BSONEachRowRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    read_columns.clear();
    seen_columns.clear();
    prev_positions.clear();
}

size_t BSONEachRowRowInputFormat::countRows(size_t max_block_size)
{
    size_t num_rows = 0;
    BSONSizeT document_size = 0;
    while (!in->eof() && num_rows < max_block_size)
    {
        readBinaryLittleEndian(document_size, *in);
        if (document_size < sizeof(BSONSizeT) + sizeof(BSON_DOCUMENT_END))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid document size: {}", document_size);
        in->ignore(document_size - sizeof(BSONSizeT));
        ++num_rows;
    }

    return num_rows;
}

BSONEachRowSchemaReader::BSONEachRowSchemaReader(ReadBuffer & in_, const FormatSettings & settings_)
    : IRowWithNamesSchemaReader(in_, settings_)
{
}

DataTypePtr BSONEachRowSchemaReader::getDataTypeFromBSONField(BSONType type, bool allow_to_skip_unsupported_types, bool & skip, size_t depth)
{
    switch (type)
    {
        case BSONType::DOUBLE:
        {
            in.ignore(sizeof(Float64));
            return std::make_shared<DataTypeFloat64>();
        }
        case BSONType::BOOL:
        {
            in.ignore(sizeof(UInt8));
            return DataTypeFactory::instance().get("Bool");
        }
        case BSONType::INT64:
        {
            in.ignore(sizeof(Int64));
            return std::make_shared<DataTypeInt64>();
        }
        case BSONType::DATETIME:
        {
            in.ignore(sizeof(Int64));
            return std::make_shared<DataTypeDateTime64>(6, "UTC");
        }
        case BSONType::INT32:
        {
            in.ignore(sizeof(Int32));
            return std::make_shared<DataTypeInt32>();
        }
        case BSONType::SYMBOL: [[fallthrough]];
        case BSONType::JAVA_SCRIPT_CODE: [[fallthrough]];
        case BSONType::STRING:
        {
            BSONSizeT size = 0;
            readBinaryLittleEndian(size, in);
            in.ignore(size);
            return std::make_shared<DataTypeString>();
        }
        case BSONType::OBJECT_ID:
        {
            in.ignore(BSON_OBJECT_ID_SIZE);
            return makeNullable(std::make_shared<DataTypeFixedString>(BSON_OBJECT_ID_SIZE));
        }
        case BSONType::DOCUMENT:
        {
            auto nested_names_and_types = getDataTypesFromBSONDocument(false, depth + 1);
            auto nested_types = nested_names_and_types.getTypes();
            bool types_are_equal = true;
            if (nested_types.empty() || !nested_types[0])
                return nullptr;

            for (size_t i = 1; i != nested_types.size(); ++i)
            {
                if (!nested_types[i])
                    return nullptr;

                types_are_equal &= nested_types[i]->equals(*nested_types[0]);
            }

            if (types_are_equal)
                return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), nested_types[0]);

            return std::make_shared<DataTypeTuple>(std::move(nested_types), nested_names_and_types.getNames());

        }
        case BSONType::ARRAY:
        {
            auto nested_types = getDataTypesFromBSONDocument(false, depth + 1).getTypes();
            bool types_are_equal = true;
            if (nested_types.empty() || !nested_types[0])
                return nullptr;

            for (size_t i = 1; i != nested_types.size(); ++i)
            {
                if (!nested_types[i])
                    return nullptr;

                types_are_equal &= nested_types[i]->equals(*nested_types[0]);
            }

            if (types_are_equal)
                return std::make_shared<DataTypeArray>(nested_types[0]);

            return std::make_shared<DataTypeTuple>(std::move(nested_types));
        }
        case BSONType::BINARY:
        {
            BSONSizeT size = 0;
            readBinaryLittleEndian(size, in);
            auto subtype = getBSONBinarySubtype(readBSONType(in));
            in.ignore(size);
            switch (subtype)
            {
                case BSONBinarySubtype::BINARY_OLD: [[fallthrough]];
                case BSONBinarySubtype::BINARY:
                    return std::make_shared<DataTypeString>();
                case BSONBinarySubtype::UUID_OLD: [[fallthrough]];
                case BSONBinarySubtype::UUID:
                    return std::make_shared<DataTypeUUID>();
                default:
                    throw Exception(ErrorCodes::UNKNOWN_TYPE, "BSON binary subtype {} is not supported", getBSONBinarySubtypeName(subtype));
            }
        }
        case BSONType::NULL_VALUE:
        {
            return nullptr;
        }
        default:
        {
            if (!allow_to_skip_unsupported_types)
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "BSON type {} is not supported", getBSONTypeName(type));

            skip = true;
            skipBSONField(in, type);
            return nullptr;
        }
    }
}

NamesAndTypesList BSONEachRowSchemaReader::getDataTypesFromBSONDocument(bool allow_to_skip_unsupported_types, size_t depth)
{
    /// BSON documents and arrays can be nested arbitrarily deep. Reject deep nesting early (before
    /// building the type) with an explicit limit, so inference stays cheap and interruptible instead
    /// of overflowing the native stack in this recursive descent. checkStackSize is a last-resort
    /// backstop for when max_parser_depth is raised above the default.
    /// max_parser_depth == 0 means unlimited (matching the SQL parser), leaving only checkStackSize.
    if (format_settings.max_parser_depth != 0 && depth > format_settings.max_parser_depth)
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Too deep recursion while inferring the BSON schema: the nesting depth exceeds the limit ({}). "
            "It can be raised with the setting 'max_parser_depth', but a very deep schema is rarely intentional",
            format_settings.max_parser_depth);
    checkStackSize();

    size_t document_start = in.count();
    BSONSizeT document_size = 0;
    readBinaryLittleEndian(document_size, in);
    NamesAndTypesList names_and_types;
    while (in.count() - document_start + sizeof(BSON_DOCUMENT_END) != document_size)
    {
        auto bson_type = getBSONType(readBSONType(in));
        String name;
        readNullTerminated(name, in);
        bool skip = false;
        auto type = getDataTypeFromBSONField(bson_type, allow_to_skip_unsupported_types, skip, depth);
        if (!skip)
            names_and_types.emplace_back(name, type);
    }

    assertChar(BSON_DOCUMENT_END, in);

    return names_and_types;
}

NamesAndTypesList BSONEachRowSchemaReader::readRowAndGetNamesAndDataTypes(bool & eof)
{
    if (in.eof())
    {
        eof = true;
        return {};
    }

    return getDataTypesFromBSONDocument(format_settings.bson.skip_fields_with_unsupported_types_in_schema_inference, 1);
}

void BSONEachRowSchemaReader::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    DataTypes types = {type, new_type};
    /// For example for integer conversion Int32,
    auto least_supertype = tryGetLeastSupertype(types);
    if (least_supertype)
        type = new_type = least_supertype;
}

static std::pair<bool, size_t>
fileSegmentationEngineBSONEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
{
    size_t number_of_rows = 0;

    while (!in.eof() && memory.size() < min_bytes && number_of_rows < max_rows)
    {
        BSONSizeT document_size = 0;
        readBinaryLittleEndian(document_size, in);

        if (document_size < sizeof(document_size))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Size of BSON document is invalid");

        if (min_bytes != 0 && document_size > 10 * min_bytes)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Size of BSON document is extremely large. Expected not greater than {} bytes, but current is {} bytes per row. Increase "
                "the value setting 'min_chunk_bytes_for_parallel_parsing' or check your data manually, most likely BSON is malformed",
                min_bytes, document_size);

        if (document_size < sizeof(document_size))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Size of BSON document is invalid");

        size_t old_size = memory.size();
        memory.resize(old_size + document_size);
        unalignedStoreLittleEndian<BSONSizeT>(memory.data() + old_size, document_size);
        in.readStrict(memory.data() + old_size + sizeof(document_size), document_size - sizeof(document_size));
        ++number_of_rows;
    }

    return {!in.eof(), number_of_rows};
}

void registerInputFormatBSONEachRow(FormatFactory & factory);
void registerInputFormatBSONEachRow(FormatFactory & factory)
{
    factory.registerInputFormat(
        "BSONEachRow",
        [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
        { return std::make_shared<BSONEachRowRowInputFormat>(buf, std::make_shared<const Block>(sample), std::move(params), settings); });
    factory.registerFileExtension("bson", "BSONEachRow");

    factory.setDocumentation("BSONEachRow", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `BSONEachRow` format parses data as a sequence of Binary JSON (BSON) documents without any separator between them.
Each row is formatted as a single document and each column is formatted as a single BSON document field with the column name as a key.

## Data types matching {#data-types-matching}

For output it uses the following correspondence between ClickHouse types and BSON types:

| ClickHouse type                                                                                                       | BSON Type                                                                                                     |
|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| [Bool](/sql-reference/data-types/boolean.md)                                                                  | `\x08` boolean                                                                                                |
| [Int8/UInt8](/sql-reference/data-types/int-uint.md)/[Enum8](/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                                  |
| [Int16/UInt16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md)      | `\x10` int32                                                                                                  |
| [Int32](/sql-reference/data-types/int-uint.md)                                                                | `\x10` int32                                                                                                  |
| [UInt32](/sql-reference/data-types/int-uint.md)                                                               | `\x12` int64                                                                                                  |
| [Int64/UInt64](/sql-reference/data-types/int-uint.md)                                                         | `\x12` int64                                                                                                  |
| [Float32/Float64](/sql-reference/data-types/float.md)                                                         | `\x01` double                                                                                                 |
| [Date](/sql-reference/data-types/date.md)/[Date32](/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                                  |
| [DateTime](/sql-reference/data-types/datetime.md)                                                             | `\x12` int64                                                                                                  |
| [DateTime64](/sql-reference/data-types/datetime64.md)                                                         | `\x09` datetime                                                                                               |
| [Decimal32](/sql-reference/data-types/decimal.md)                                                             | `\x10` int32                                                                                                  |
| [Decimal64](/sql-reference/data-types/decimal.md)                                                             | `\x12` int64                                                                                                  |
| [Decimal128](/sql-reference/data-types/decimal.md)                                                            | `\x05` binary, `\x00` binary subtype, size = 16                                                               |
| [Decimal256](/sql-reference/data-types/decimal.md)                                                            | `\x05` binary, `\x00` binary subtype, size = 32                                                               |
| [Int128/UInt128](/sql-reference/data-types/int-uint.md)                                                       | `\x05` binary, `\x00` binary subtype, size = 16                                                               |
| [Int256/UInt256](/sql-reference/data-types/int-uint.md)                                                       | `\x05` binary, `\x00` binary subtype, size = 32                                                               |
| [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md) | `\x05` binary, `\x00` binary subtype or \x02 string if setting output_format_bson_string_as_string is enabled |
| [UUID](/sql-reference/data-types/uuid.md)                                                                     | `\x05` binary, `\x04` uuid subtype, size = 16                                                                 |
| [Array](/sql-reference/data-types/array.md)                                                                   | `\x04` array                                                                                                  |
| [Tuple](/sql-reference/data-types/tuple.md)                                                                   | `\x04` array                                                                                                  |
| [Named Tuple](/sql-reference/data-types/tuple.md)                                                             | `\x03` document                                                                                               |
| [Map](/sql-reference/data-types/map.md)                                                                       | `\x03` document                                                                                               |
| [IPv4](/sql-reference/data-types/ipv4.md)                                                                     | `\x10` int32                                                                                                  |
| [IPv6](/sql-reference/data-types/ipv6.md)                                                                     | `\x05` binary, `\x00` binary subtype                                                                          |

For input it uses the following correspondence between BSON types and ClickHouse types:

| BSON Type                                | ClickHouse Type                                                                                                                                                                                                                             |
|------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `\x01` double                            | [Float32/Float64](/sql-reference/data-types/float.md)                                                                                                                                                                               |
| `\x02` string                            | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x03` document                          | [Map](/sql-reference/data-types/map.md)/[Named Tuple](/sql-reference/data-types/tuple.md)                                                                                                                                   |
| `\x04` array                             | [Array](/sql-reference/data-types/array.md)/[Tuple](/sql-reference/data-types/tuple.md)                                                                                                                                     |
| `\x05` binary, `\x00` binary subtype     | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)/[IPv6](/sql-reference/data-types/ipv6.md)                                                             |
| `\x05` binary, `\x02` old binary subtype | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x05` binary, `\x03` old uuid subtype   | [UUID](/sql-reference/data-types/uuid.md)                                                                                                                                                                                           |
| `\x05` binary, `\x04` uuid subtype       | [UUID](/sql-reference/data-types/uuid.md)                                                                                                                                                                                           |
| `\x07` ObjectId                          | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x08` boolean                           | [Bool](/sql-reference/data-types/boolean.md)                                                                                                                                                                                        |
| `\x09` datetime                          | [DateTime64](/sql-reference/data-types/datetime64.md)                                                                                                                                                                               |
| `\x0A` null value                        | [NULL](/sql-reference/data-types/nullable.md)                                                                                                                                                                                       |
| `\x0D` JavaScript code                   | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x0E` symbol                            | [String](/sql-reference/data-types/string.md)/[FixedString](/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x10` int32                             | [Int32/UInt32](/sql-reference/data-types/int-uint.md)/[Decimal32](/sql-reference/data-types/decimal.md)/[IPv4](/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/sql-reference/data-types/enum.md) |
| `\x12` int64                             | [Int64/UInt64](/sql-reference/data-types/int-uint.md)/[Decimal64](/sql-reference/data-types/decimal.md)/[DateTime64](/sql-reference/data-types/datetime64.md)                                                       |

Other BSON types are not supported. Additionally, it performs conversion between different integer types. 
For example, it is possible to insert a BSON `int32` value into ClickHouse as [`UInt8`](../../sql-reference/data-types/int-uint.md).

Big integers and decimals such as `Int128`/`UInt128`/`Int256`/`UInt256`/`Decimal128`/`Decimal256` can be parsed from a BSON Binary value with the `\x00` binary subtype. 
In this case, the format will validate that the size of the binary data equals the size of the expected value.

:::note
This format does not work properly on Big-Endian platforms.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a BSON file with the following data, named as `football.bson`:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.bson' FORMAT BSONEachRow;
```

### Reading data {#reading-data}

Read data using the `BSONEachRow` format:

```sql
SELECT *
FROM football INTO OUTFILE 'docs_data/bson/football.bson'
FORMAT BSONEachRow
```

:::tip
BSON is a binary format that does not display in a human-readable form on the terminal. Use the `INTO OUTFILE` to output BSON files.
:::

## Format settings {#format-settings}

| Setting                                                                                                                                                                                               | Description                                                                                  | Default  |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|----------|
| [`output_format_bson_string_as_string`](../../operations/settings/settings-formats.md/#output_format_bson_string_as_string)                                                                           | Use BSON String type instead of Binary for String columns.                                   | `false`  |
| [`input_format_bson_skip_fields_with_unsupported_types_in_schema_inference`](../../operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) | Allow skipping columns with unsupported types while schema inference for format BSONEachRow. | `false`  |
)DOCS_MD"});
}

void registerFileSegmentationEngineBSONEachRow(FormatFactory & factory);
void registerFileSegmentationEngineBSONEachRow(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("BSONEachRow", &fileSegmentationEngineBSONEachRow);
}

void registerBSONEachRowSchemaReader(FormatFactory & factory);
void registerBSONEachRowSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("BSONEachRow", [](ReadBuffer & buf, const FormatSettings & settings)
    {
        return std::make_unique<BSONEachRowSchemaReader>(buf, settings);
    });
    factory.registerAdditionalInfoForSchemaCacheGetter("BSONEachRow", [](const FormatSettings & settings)
    {
         String result = getAdditionalFormatInfoForAllRowBasedFormats(settings);
         return result + fmt::format(", skip_fields_with_unsupported_types_in_schema_inference={}",
                                     settings.bson.skip_fields_with_unsupported_types_in_schema_inference);
    });
}

}
