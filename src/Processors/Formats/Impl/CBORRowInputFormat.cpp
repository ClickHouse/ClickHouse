#include <Processors/Formats/Impl/CBORRowInputFormat.h>

#if USE_CBOR

#    include <cstdlib>
#    include <IO/ReadBufferFromMemory.h>
#    include <IO/ReadHelpers.h>
#    include <Common/assert_cast.h>

#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeLowCardinality.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <DataTypes/DataTypeUUID.h>

#    include <Columns/ColumnArray.h>
#    include <Columns/ColumnLowCardinality.h>
#    include <Columns/ColumnMap.h>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnTuple.h>
#    include <Columns/ColumnsNumber.h>

#    include <Formats/EscapingRuleUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int ILLEGAL_COLUMN;
}

static void insertCommonInteger(IColumn & column, DataTypePtr type, UInt64 value)
{
    switch (type->getTypeId())
    {
        case TypeIndex::UInt8: {
            assert_cast<ColumnUInt8 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::UInt16: {
            assert_cast<ColumnUInt16 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::UInt32: {
            assert_cast<ColumnUInt32 &>(column).insertValue(static_cast<UInt32>(value));
            break;
        }
        case TypeIndex::UInt64: {
            assert_cast<ColumnUInt64 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::IPv4: {
            assert_cast<ColumnIPv4 &>(column).insertValue(IPv4(static_cast<UInt32>(value)));
            break;
        }
        case TypeIndex::Int8: {
            assert_cast<ColumnInt8 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Int16: {
            assert_cast<ColumnInt16 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Int32: {
            assert_cast<ColumnInt32 &>(column).insertValue(static_cast<Int32>(value));
            break;
        }
        case TypeIndex::Int64: {
            assert_cast<ColumnInt64 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::DateTime64: {
            assert_cast<DataTypeDateTime64::ColumnType &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Decimal32: {
            assert_cast<ColumnDecimal<Decimal32> &>(column).insertValue(static_cast<Int32>(value));
            break;
        }
        case TypeIndex::Decimal64: {
            assert_cast<ColumnDecimal<Decimal64> &>(column).insertValue(value);
            break;
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR integer into column with type {}.", type->getName());
    }
}

[[maybe_unused]] static void insertFloat32(IColumn & column, DataTypePtr type, float value)
{
    if (!WhichDataType(type).isFloat32())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR float32 into column with type {}.", type->getName());
    assert_cast<ColumnFloat32 &>(column).insertValue(value);
}

[[maybe_unused]] static void insertDouble(IColumn & column, DataTypePtr type, double value)
{
    if (!WhichDataType(type).isFloat64())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR double into column with type {}.", type->getName());
    assert_cast<ColumnFloat64 &>(column).insertValue(value);
}

static void insertFloat32(IColumn & column, DataTypePtr type, unsigned int data)
{
    if (!WhichDataType(type).isFloat32())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR double into column with type {}.", type->getName());
    const char raw_data[4]
        = {static_cast<char>(data & 0xFF),
           static_cast<char>((data >> 8) & 0xFF),
           static_cast<char>((data >> 16) & 0xFF),
           static_cast<char>((data >> 24) & 0xFF)};

    assert_cast<ColumnFloat32 &>(column).insertData(raw_data, sizeof(float));
}

static void insertDouble(IColumn & column, DataTypePtr type, unsigned long long data)
{
    if (!WhichDataType(type).isFloat64())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR double into column with type {}.", type->getName());
    const char raw_data[8]
        = {static_cast<char>(data & 0xFF),
           static_cast<char>((data >> 8) & 0xFF),
           static_cast<char>((data >> 16) & 0xFF),
           static_cast<char>((data >> 24) & 0xFF),
           static_cast<char>((data >> 32) & 0xFF),
           static_cast<char>((data >> 40) & 0xFF),
           static_cast<char>((data >> 48) & 0xFF),
           static_cast<char>((data >> 56) & 0xFF)};
    assert_cast<ColumnFloat64 &>(column).insertData(raw_data, sizeof(double));
}

static void insertBool(IColumn & column, DataTypePtr type, bool value)
{
    // TODO: can we write bool value into other integer types?
    if (!WhichDataType(type).isUInt8())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR bool into column with type {}.", type->getName());
    assert_cast<ColumnUInt8 &>(column).insertValue(value);
}

template <typename ColumnType>
static void insertFromBinaryRepresentation(IColumn & column, DataTypePtr type, const char * value, size_t size)
{
    constexpr size_t column_type_size = sizeof(typename ColumnType::ValueType);
    if (size > column_type_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size of {} value: {}", type->getName(), size);

    char data[column_type_size] = {0};
    //    size_t init_index = column_type_size - size;
    for (size_t i = 0; i < size; ++i)
        data[i] = value[i];
    assert_cast<ColumnType &>(column).insertData(data, column_type_size);
}

static void insertBigUnsignedInteger(IColumn & column, DataTypePtr type, const char * data, int size)
{
    switch (type->getTypeId())
    {
        case TypeIndex::IPv6:
            insertFromBinaryRepresentation<ColumnIPv6>(column, type, data, size);
            return;
        case TypeIndex::UInt128:
            insertFromBinaryRepresentation<ColumnUInt128>(column, type, data, size);
            return;
        case TypeIndex::UInt256:
            insertFromBinaryRepresentation<ColumnUInt256>(column, type, data, size);
            return;
        case TypeIndex::Decimal128:
            insertFromBinaryRepresentation<ColumnDecimal<Decimal128>>(column, type, data, size);
            return;
        case TypeIndex::Decimal256:
            insertFromBinaryRepresentation<ColumnDecimal<Decimal256>>(column, type, data, size);
            return;
        default:
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR big unsigned integer into column with type {}.", type->getName());
    }
}

static void insertBigNegativeInteger(IColumn & column, DataTypePtr type, const char * data, int size)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Int128:
            insertFromBinaryRepresentation<ColumnInt128>(column, type, data, size);
            return;
        case TypeIndex::Int256:
            insertFromBinaryRepresentation<ColumnInt256>(column, type, data, size);
            return;
        case TypeIndex::Decimal128:
            insertFromBinaryRepresentation<ColumnDecimal<Decimal128>>(column, type, data, size);
            return;
        case TypeIndex::Decimal256:
            insertFromBinaryRepresentation<ColumnDecimal<Decimal256>>(column, type, data, size);
            return;
        default:
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR big unsigned integer into column with type {}.", type->getName());
    }
}

static void insertString(IColumn & column, DataTypePtr type, const std::string & str)
{
    switch (type->getTypeId())
    {
        case TypeIndex::String: {
            assert_cast<ColumnString &>(column).insertData(str.data(), str.size());
            break;
        }
        case TypeIndex::FixedString: {
            assert_cast<ColumnFixedString &>(column).insertData(str.data(), str.size());
            break;
        }
        case TypeIndex::UUID: {
            ReadBufferFromMemory buf(str.data(), str.size());
            UUID uuid;
            readUUIDText(uuid, buf);
            assert_cast<ColumnUUID &>(column).insertValue(uuid);
            break;
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR string into column with type {}.", type->getName());
    }
}

static void insertNull(IColumn & column, DataTypePtr type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Nothing: {
            assert_cast<ColumnNullable &>(column).insertDefault();
            break;
        }
        case TypeIndex::Nullable: {
            if (type->isNullable())
            {
                auto & nullable_column = assert_cast<ColumnNullable &>(column);
                //                auto & nested_column = nullable_column.getNestedColumn();
                //                const auto & nested_type = assert_cast<const DataTypeNullable *>(type.get())->getNestedType();
                nullable_column.getNullMapColumn().insertValue(0);
            }
            else
            {
                assert_cast<ColumnNullable &>(column).insertDefault();
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR null into column with type {}.", type->getName());
    }
}


WriteToDBListener::WriteToDBListener(MutableColumns & columns_, DataTypes & data_types_)
{
    // We fill in the reverse order, because we want the first element to be read to be at the top of the stack
    // and the order is correct
    for (ssize_t column_index = columns_.size() - 1; column_index >= 0; --column_index)
        set_info(*columns_[column_index], data_types_[column_index]);
}

void WriteToDBListener::set_info(IColumn & column, DataTypePtr type)
{
    stack_info.emplace(column, type);
}

[[maybe_unused]] bool WriteToDBListener::info_empty() const
{
    return stack_info.empty();
}

void WriteToDBListener::on_integer(int value)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    insertCommonInteger(column, type, value);
}

void WriteToDBListener::on_float32(float value)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    insertFloat32(column, type, value);
}

void WriteToDBListener::on_double(double value)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    insertDouble(column, type, value);
}

void WriteToDBListener::on_bytes(unsigned char * data, int size)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    // Dispatch tag
    switch (current_tag)
    {
        case CBORTagTypes::UNSIGNED_BIGNUM: {
            insertBigUnsignedInteger(column, type, const_cast<const char *>(reinterpret_cast<char *>(data)), size);
            break;
        }
        case CBORTagTypes::NEGATIVE_BIGNUM: {
            insertBigNegativeInteger(column, type, const_cast<const char *>(reinterpret_cast<char *>(data)), size);
            break;
        }
        default:
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot insert CBOR byte string with {} tag.", current_tag);
    }
}

void WriteToDBListener::on_string(std::string & str)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    insertString(column, type, str);
}

void WriteToDBListener::on_array(int size)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    if (isArray(type))
    {
        auto nested_type = assert_cast<const DataTypeArray &>(*type).getNestedType();
        ColumnArray & column_array = assert_cast<ColumnArray &>(column);
        //        ColumnArray::Offsets & offsets = column_array.getOffsets();
        IColumn & nested_column = column_array.getData();
        if (size > 0)
            stack_info.emplace(nested_column, nested_type);
    }
    else if (isTuple(type))
    {
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*type);
        const auto & nested_types = tuple_type.getElements();
        if (static_cast<size_t>(size) != nested_types.size())
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Cannot insert CBOR array with size {} into Tuple column with {} elements",
                size,
                nested_types.size());
        ColumnTuple & column_tuple = assert_cast<ColumnTuple &>(column);
        /// Push nested columns into stack in reverse order.
        for (ssize_t i = nested_types.size() - 1; i >= 0; --i)
            stack_info.emplace(column_tuple.getColumn(i), nested_types[i]);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR array into column with type {}", type->getName());
    }
}

void WriteToDBListener::on_map(int)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    if (!isMap(type))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert CBOR map into column with type {}.", type->getName());
    // ColumnArray & column_array = assert_cast<ColumnMap &>(column).getNestedColumn();
}

void WriteToDBListener::on_tag(unsigned int tag)
{
    current_tag = static_cast<CBORTagTypes>(tag);
}

void WriteToDBListener::on_special(unsigned int code)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    insertFloat32(column, type, code);
    // Special (simple values 0 - 20)
    // and Simple value (value 32..255 in following byte),  f16, f32
}

void WriteToDBListener::on_bool(bool value)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    insertBool(column, type, value);
}

void WriteToDBListener::on_null()
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    insertNull(column, type);
}

void WriteToDBListener::on_undefined()
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Clickhouse does not support CBOR undefined value.");
}

void WriteToDBListener::on_error(const char * error)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "CBOR decoder error: {}", error);
}

void WriteToDBListener::on_extra_integer(unsigned long long value, int sign)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    if (sign == 1)
        insertCommonInteger(column, type, value);
    else
        insertCommonInteger(column, type, -value);
}

void WriteToDBListener::on_extra_tag(unsigned long long tag)
{
    current_tag = static_cast<CBORTagTypes>(tag);
}

void WriteToDBListener::on_extra_special(unsigned long long tag)
{
    auto [column, type] = stack_info.top();
    stack_info.pop();
    insertDouble(column, type, tag);
}

CBORRowInputFormat::CBORRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_)
    : IRowInputFormat(header_, in_, std::move(params_)), reader(std::make_unique<CBORReader>(in_)), data_types(header_.getDataTypes())
{
}

void CBORRowInputFormat::readPrefix()
{
    reader->readAndCheckPrefix();
}

bool CBORRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    WriteToDBListener listener(columns, data_types);
    return reader->readRow(columns.size(), listener);
}

void registerInputFormatCBOR(FormatFactory & factory)
{
    factory.registerInputFormat(
        "CBOR",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings &)
        { return std::make_shared<CBORRowInputFormat>(sample, buf, params); });
    factory.registerFileExtension("cbor", "CBOR");
}
}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatMsgPack(FormatFactory &)
{
}

}

#endif
