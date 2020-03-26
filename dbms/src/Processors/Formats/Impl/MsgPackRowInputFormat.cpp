#include <cstdlib>
#include <Processors/Formats/Impl/MsgPackRowInputFormat.h>
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
    : IRowInputFormat(header_, in_, std::move(params_)), data_types(header_.getDataTypes()) {}

bool MsgPackRowInputFormat::readObject()
{
    if (in.eof() && unpacker.nonparsed_size() == 0)
        return false;
    while (!unpacker.next(object_handle))
    {
        if (in.eof())
            throw Exception("Unexpected end of file while parsing MsgPack object.", ErrorCodes::INCORRECT_DATA);
        unpacker.reserve_buffer(in.available());
        memcpy(unpacker.buffer(), in.position(), in.available());
        unpacker.buffer_consumed(in.available());
        in.position() += in.available();
    }
    return true;
}

void MsgPackRowInputFormat::insertObject(IColumn & column, DataTypePtr data_type, const msgpack::object & object)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::UInt8:
        {
            assert_cast<ColumnUInt8 &>(column).insertValue(object.as<uint8_t>());
            return;
        }
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::UInt16:
        {
            assert_cast<ColumnUInt16 &>(column).insertValue(object.as<UInt16>());
            return;
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::UInt32:
        {
            assert_cast<ColumnUInt32 &>(column).insertValue(object.as<UInt32>());
            return;
        }
        case TypeIndex::UInt64:
        {
            assert_cast<ColumnUInt64 &>(column).insertValue(object.as<UInt64>());
            return;
        }
        case TypeIndex::Int8:
        {
            assert_cast<ColumnInt8 &>(column).insertValue(object.as<Int8>());
            return;
        }
        case TypeIndex::Int16:
        {
            assert_cast<ColumnInt16 &>(column).insertValue(object.as<Int16>());
            return;
        }
        case TypeIndex::Int32:
        {
            assert_cast<ColumnInt32 &>(column).insertValue(object.as<Int32>());
            return;
        }
        case TypeIndex::Int64:
        {
            assert_cast<ColumnInt64 &>(column).insertValue(object.as<Int64>());
            return;
        }
        case TypeIndex::Float32:
        {
            assert_cast<ColumnFloat32 &>(column).insertValue(object.as<Float32>());
            return;
        }
        case TypeIndex::Float64:
        {
            assert_cast<ColumnFloat64 &>(column).insertValue(object.as<Float64>());
            return;
        }
        case TypeIndex::DateTime64:
        {
            assert_cast<DataTypeDateTime64::ColumnType &>(column).insertValue(object.as<UInt64>());
            return;
        }
        case TypeIndex::FixedString: [[fallthrough]];
        case TypeIndex::String:
        {
            String str = object.as<String>();
            column.insertData(str.data(), str.size());
            return;
        }
        case TypeIndex::Array:
        {
            msgpack::object_array object_array = object.via.array;
            auto nested_type = assert_cast<const DataTypeArray &>(*data_type).getNestedType();
            ColumnArray & column_array = assert_cast<ColumnArray &>(column);
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();
            for (size_t i = 0; i != object_array.size; ++i)
            {
                insertObject(nested_column, nested_type, object_array.ptr[i]);
            }
            offsets.push_back(offsets.back() + object_array.size);
            return;
        }
        case TypeIndex::Nullable:
        {
            auto nested_type = removeNullable(data_type);
            ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(column);
            if (object.type == msgpack::type::NIL)
                column_nullable.insertDefault();
            else
                insertObject(column_nullable.getNestedColumn(), nested_type, object);
            return;
        }
        case TypeIndex::Nothing:
        {
            // Nothing to insert, MsgPack object is nil.
            return;
        }
        default:
            break;
    }
    throw Exception("Type " + data_type->getName() + " is not supported for MsgPack input format", ErrorCodes::ILLEGAL_COLUMN);
}

bool MsgPackRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    size_t column_index = 0;
    bool has_more_data = true;
    for (; column_index != columns.size(); ++column_index)
    {
        has_more_data = readObject();
        if (!has_more_data)
            break;
        insertObject(*columns[column_index], data_types[column_index], object_handle.get());
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
            ReadBuffer &buf,
            const Block &sample,
            const RowInputFormatParams &params,
            const FormatSettings &)
    {
        return std::make_shared<MsgPackRowInputFormat>(sample, buf, params);
    });
}

}
