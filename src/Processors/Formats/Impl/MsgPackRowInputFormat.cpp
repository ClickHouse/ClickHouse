#include <Processors/Formats/Impl/MsgPackRowInputFormat.h>

#if USE_MSGPACK

#include <cstdlib>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
}

MsgPackRowInputFormat::MsgPackRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_)
    : IRowInputFormat(header_, in_, std::move(params_)), buf(*in), parser(visitor), data_types(header_.getDataTypes())  {}

void MsgPackRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    buf.reset();
    visitor.reset();
}

void MsgPackVisitor::set_info(IColumn & column, DataTypePtr type) // NOLINT
{
    while (!info_stack.empty())
    {
        info_stack.pop();
    }
    info_stack.push(Info{column, type});
}

void MsgPackVisitor::reset()
{
    info_stack = {};
}

template <typename InsertFunc>
static bool checkAndInsertNullable(IColumn & column, DataTypePtr type, InsertFunc insert_func)
{
    if (type->isNullable())
    {
        auto & nullable_column = assert_cast<ColumnNullable &>(column);
        auto & nested_column = nullable_column.getNestedColumn();
        const auto & nested_type = assert_cast<const DataTypeNullable *>(type.get())->getNestedType();
        insert_func(nested_column, nested_type);
        nullable_column.getNullMapColumn().insertValue(0);
        return true;
    }

    return false;
}

template <typename InsertFunc>
static bool checkAndInsertLowCardinality(IColumn & column, DataTypePtr type, InsertFunc insert_func)
{
    if (type->lowCardinality())
    {
        auto & lc_column = assert_cast<ColumnLowCardinality &>(column);
        auto tmp_column = lc_column.getDictionary().getNestedColumn()->cloneEmpty();
        auto dict_type = assert_cast<const DataTypeLowCardinality *>(type.get())->getDictionaryType();
        insert_func(*tmp_column, dict_type);
        lc_column.insertFromFullColumn(*tmp_column, 0);
        return true;
    }
    return false;
}

static void insertInteger(IColumn & column, DataTypePtr type, UInt64 value)
{
    auto insert_func = [&](IColumn & column_, DataTypePtr type_)
    {
        insertInteger(column_, type_, value);
    };

    if (checkAndInsertNullable(column, type, insert_func) || checkAndInsertLowCardinality(column, type, insert_func))
        return;

    switch (type->getTypeId())
    {
        case TypeIndex::UInt8:
        {
            assert_cast<ColumnUInt8 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
        {
            assert_cast<ColumnUInt16 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
        {
            assert_cast<ColumnUInt32 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::UInt64:
        {
            assert_cast<ColumnUInt64 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Int8:
        {
            assert_cast<ColumnInt8 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Int16:
        {
            assert_cast<ColumnInt16 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Int32:
        {
            assert_cast<ColumnInt32 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::Int64:
        {
            assert_cast<ColumnInt64 &>(column).insertValue(value);
            break;
        }
        case TypeIndex::DateTime64:
        {
            assert_cast<DataTypeDateTime64::ColumnType &>(column).insertValue(value);
            break;
        }
        default:
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack integer into column with type {}.", type->getName());
    }
}

static void insertString(IColumn & column, DataTypePtr type, const char * value, size_t size)
{
    auto insert_func = [&](IColumn & column_, DataTypePtr type_)
    {
        insertString(column_, type_, value, size);
    };

    if (checkAndInsertNullable(column, type, insert_func) || checkAndInsertLowCardinality(column, type, insert_func))
        return;

    if (!isStringOrFixedString(type))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack string into column with type {}.", type->getName());

    column.insertData(value, size);
}

static void insertFloat32(IColumn & column, DataTypePtr type, Float32 value) // NOLINT
{
    auto insert_func = [&](IColumn & column_, DataTypePtr type_)
    {
        insertFloat32(column_, type_, value);
    };

    if (checkAndInsertNullable(column, type, insert_func) || checkAndInsertLowCardinality(column, type, insert_func))
        return;

    if (!WhichDataType(type).isFloat32())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack float32 into column with type {}.", type->getName());

    assert_cast<ColumnFloat32 &>(column).insertValue(value);
}

static void insertFloat64(IColumn & column, DataTypePtr type, Float64 value) // NOLINT
{
    auto insert_func = [&](IColumn & column_, DataTypePtr type_)
    {
        insertFloat64(column_, type_, value);
    };

    if (checkAndInsertNullable(column, type, insert_func) || checkAndInsertLowCardinality(column, type, insert_func))
        return;

    if (!WhichDataType(type).isFloat64())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack float64 into column with type {}.", type->getName());

    assert_cast<ColumnFloat64 &>(column).insertValue(value);
}

static void insertNull(IColumn & column, DataTypePtr type)
{
    auto insert_func = [&](IColumn & column_, DataTypePtr type_)
    {
        insertNull(column_, type_);
    };

    /// LowCardinality(Nullable(...))
    if (checkAndInsertLowCardinality(column, type, insert_func))
        return;

    if (!type->isNullable())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack null into non-nullable column with type {}.", type->getName());

    assert_cast<ColumnNullable &>(column).insertDefault();
}

bool MsgPackVisitor::visit_positive_integer(UInt64 value) // NOLINT
{
    insertInteger(info_stack.top().column, info_stack.top().type, value);
    return true;
}

bool MsgPackVisitor::visit_negative_integer(Int64 value) // NOLINT
{
    insertInteger(info_stack.top().column, info_stack.top().type, value);
    return true;
}

bool MsgPackVisitor::visit_str(const char * value, size_t size) // NOLINT
{
    insertString(info_stack.top().column, info_stack.top().type, value, size);
    return true;
}

bool MsgPackVisitor::visit_bin(const char * value, size_t size) // NOLINT
{
    insertString(info_stack.top().column, info_stack.top().type, value, size);
    return true;
}

bool MsgPackVisitor::visit_float32(Float32 value) // NOLINT
{
    insertFloat32(info_stack.top().column, info_stack.top().type, value);
    return true;
}

bool MsgPackVisitor::visit_float64(Float64 value) // NOLINT
{
    insertFloat64(info_stack.top().column, info_stack.top().type, value);
    return true;
}

bool MsgPackVisitor::visit_boolean(bool value)
{
    insertInteger(info_stack.top().column, info_stack.top().type, UInt64(value));
    return true;
}

bool MsgPackVisitor::start_array(size_t size) // NOLINT
{
    if (!isArray(info_stack.top().type))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack array into column with type {}.", info_stack.top().type->getName());

    auto nested_type = assert_cast<const DataTypeArray &>(*info_stack.top().type).getNestedType();
    ColumnArray & column_array = assert_cast<ColumnArray &>(info_stack.top().column);
    ColumnArray::Offsets & offsets = column_array.getOffsets();
    IColumn & nested_column = column_array.getData();
    offsets.push_back(offsets.back() + size);
    info_stack.push(Info{nested_column, nested_type});
    return true;
}

bool MsgPackVisitor::end_array() // NOLINT
{
    info_stack.pop();
    return true;
}

bool MsgPackVisitor::start_map(uint32_t size) // NOLINT
{
    if (!isMap(info_stack.top().type))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert MessagePack map into column with type {}.", info_stack.top().type->getName());
    ColumnArray & column_array = assert_cast<ColumnMap &>(info_stack.top().column).getNestedColumn();
    ColumnArray::Offsets & offsets = column_array.getOffsets();
    offsets.push_back(offsets.back() + size);
    return true;
}

bool MsgPackVisitor::start_map_key() // NOLINT
{
    auto key_column = assert_cast<ColumnMap &>(info_stack.top().column).getNestedData().getColumns()[0];
    auto key_type = assert_cast<const DataTypeMap &>(*info_stack.top().type).getKeyType();
    info_stack.push(Info{*key_column, key_type});
    return true;
}

bool MsgPackVisitor::end_map_key() // NOLINT
{
    info_stack.pop();
    return true;
}

bool MsgPackVisitor::start_map_value() // NOLINT
{
    auto value_column = assert_cast<ColumnMap &>(info_stack.top().column).getNestedData().getColumns()[1];
    auto value_type = assert_cast<const DataTypeMap &>(*info_stack.top().type).getValueType();
    info_stack.push(Info{*value_column, value_type});
    return true;
}

bool MsgPackVisitor::end_map_value() // NOLINT
{
    info_stack.pop();
    return true;
}

bool MsgPackVisitor::visit_nil()
{
    insertNull(info_stack.top().column, info_stack.top().type);
    return true;
}

void MsgPackVisitor::parse_error(size_t, size_t) // NOLINT
{
    throw Exception("Error occurred while parsing msgpack data.", ErrorCodes::INCORRECT_DATA);
}

bool MsgPackRowInputFormat::readObject()
{
    if (buf.eof())
        return false;

    PeekableReadBufferCheckpoint checkpoint{buf};
    size_t offset = 0;
    while (!parser.execute(buf.position(), buf.available(), offset))
    {
        buf.position() = buf.buffer().end();
        if (buf.eof())
            throw Exception("Unexpected end of file while parsing msgpack object.", ErrorCodes::INCORRECT_DATA);
        buf.position() = buf.buffer().end();
        buf.makeContinuousMemoryFromCheckpointToPos();
        buf.rollbackToCheckpoint();
    }
    buf.position() += offset;
    return true;
}

bool MsgPackRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    size_t column_index = 0;
    bool has_more_data = true;
    for (; column_index != columns.size(); ++column_index)
    {
        visitor.set_info(*columns[column_index], data_types[column_index]);
        has_more_data = readObject();
        if (!has_more_data)
            break;
    }
    if (!has_more_data)
    {
        if (column_index != 0)
            throw Exception("Not enough values to complete the row.", ErrorCodes::INCORRECT_DATA);
        return false;
    }
    return true;
}

void registerInputFormatMsgPack(FormatFactory & factory)
{
    factory.registerInputFormat("MsgPack", [](
            ReadBuffer & buf,
            const Block & sample,
            const RowInputFormatParams & params,
            const FormatSettings &)
    {
        return std::make_shared<MsgPackRowInputFormat>(sample, buf, params);
    });
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
