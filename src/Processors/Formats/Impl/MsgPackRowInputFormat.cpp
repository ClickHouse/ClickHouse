#include <Processors/Formats/Impl/MsgPackRowInputFormat.h>

#if USE_MSGPACK

#include <cstdlib>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
}

MsgPackRowInputFormat::MsgPackRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_)
    : IRowInputFormat(header_, in_, std::move(params_)), buf(in), parser(visitor), data_types(header_.getDataTypes())  {}

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

void MsgPackVisitor::insert_integer(UInt64 value) // NOLINT
{
    Info & info = info_stack.top();
    switch (info.type->getTypeId())
    {
        case TypeIndex::UInt8:
        {
            assert_cast<ColumnUInt8 &>(info.column).insertValue(value);
            break;
        }
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
        {
            assert_cast<ColumnUInt16 &>(info.column).insertValue(value);
            break;
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
        {
            assert_cast<ColumnUInt32 &>(info.column).insertValue(value);
            break;
        }
        case TypeIndex::UInt64:
        {
            assert_cast<ColumnUInt64 &>(info.column).insertValue(value);
            break;
        }
        case TypeIndex::Int8:
        {
            assert_cast<ColumnInt8 &>(info.column).insertValue(value);
            break;
        }
        case TypeIndex::Int16:
        {
            assert_cast<ColumnInt16 &>(info.column).insertValue(value);
            break;
        }
        case TypeIndex::Int32:
        {
            assert_cast<ColumnInt32 &>(info.column).insertValue(value);
            break;
        }
        case TypeIndex::Int64:
        {
            assert_cast<ColumnInt64 &>(info.column).insertValue(value);
            break;
        }
        case TypeIndex::DateTime64:
        {
            assert_cast<DataTypeDateTime64::ColumnType &>(info.column).insertValue(value);
            break;
        }
        default:
            throw Exception("Type " + info.type->getName() + " is not supported for MsgPack input format", ErrorCodes::ILLEGAL_COLUMN);
    }
}

bool MsgPackVisitor::visit_positive_integer(UInt64 value) // NOLINT
{
    insert_integer(value);
    return true;
}

bool MsgPackVisitor::visit_negative_integer(Int64 value) // NOLINT
{
    insert_integer(value);
    return true;
}

bool MsgPackVisitor::visit_str(const char* value, size_t size) // NOLINT
{
    info_stack.top().column.insertData(value, size);
    return true;
}

bool MsgPackVisitor::visit_float32(Float32 value) // NOLINT
{
    assert_cast<ColumnFloat32 &>(info_stack.top().column).insertValue(value);
    return true;
}

bool MsgPackVisitor::visit_float64(Float64 value) // NOLINT
{
    assert_cast<ColumnFloat64 &>(info_stack.top().column).insertValue(value);
    return true;
}

bool MsgPackVisitor::start_array(size_t size) // NOLINT
{
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

void registerInputFormatProcessorMsgPack(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("MsgPack", [](
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
void registerInputFormatProcessorMsgPack(FormatFactory &)
{
}
}

#endif
