#include <Processors/Formats/Impl/NpyOutputFormat.h>

#include <Common/assert_cast.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/FormatFactory.h>
#include <Processors/Port.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_COLUMNS;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

template <typename ColumnType, typename ValueType>
void writeNumpyNumbers(const ColumnPtr & column, WriteBuffer & buf)
{
    const auto * number_column = assert_cast<const ColumnType *>(column.get());
    for (size_t i = 0; i < number_column->size(); ++i)
        writeBinaryLittleEndian(ValueType(number_column->getElement(i)), buf);
}

template <typename ColumnType>
void writeNumpyStrings(const ColumnPtr & column, size_t length, WriteBuffer & buf)
{
    const auto * string_column = assert_cast<const ColumnType *>(column.get());
    for (size_t i = 0; i < string_column->size(); ++i)
    {
        auto data = string_column->getDataAt(i);
        buf.write(data.data, data.size);
        writeChar(0, length - data.size, buf);
    }
}

}

String NpyOutputFormat::shapeStr() const
{
    WriteBufferFromOwnString shape;
    writeIntText(num_rows, shape);
    writeChar(',', shape);
    for (UInt64 dim : numpy_shape)
    {
        writeIntText(dim, shape);
        writeChar(',', shape);
    }

    return shape.str();
}

NpyOutputFormat::NpyOutputFormat(WriteBuffer & out_, const Block & header_) : IOutputFormat(header_, out_)
{
    const auto & header = getPort(PortKind::Main).getHeader();
    auto data_types = header.getDataTypes();
    if (data_types.size() != 1)
        throw Exception(ErrorCodes::TOO_MANY_COLUMNS, "Expected single column for Npy output format, got {}", data_types.size());
    data_type = data_types[0];

    if (!getNumpyDataType(data_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Type {} is not supported for Npy output format", nested_data_type->getName());
}

bool NpyOutputFormat::getNumpyDataType(const DataTypePtr & type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Int8:
            numpy_data_type = std::make_shared<NumpyDataTypeInt>(NumpyDataType::Endianness::LITTLE, sizeof(Int8), true);
            break;
        case TypeIndex::Int16:
            numpy_data_type = std::make_shared<NumpyDataTypeInt>(NumpyDataType::Endianness::LITTLE, sizeof(Int16), true);
            break;
        case TypeIndex::Int32:
            numpy_data_type = std::make_shared<NumpyDataTypeInt>(NumpyDataType::Endianness::LITTLE, sizeof(Int32), true);
            break;
        case TypeIndex::Int64:
            numpy_data_type = std::make_shared<NumpyDataTypeInt>(NumpyDataType::Endianness::LITTLE, sizeof(Int64), true);
            break;
        case TypeIndex::UInt8:
            numpy_data_type = std::make_shared<NumpyDataTypeInt>(NumpyDataType::Endianness::LITTLE, sizeof(UInt8), false);
            break;
        case TypeIndex::UInt16:
            numpy_data_type = std::make_shared<NumpyDataTypeInt>(NumpyDataType::Endianness::LITTLE, sizeof(UInt16), false);
            break;
        case TypeIndex::UInt32:
            numpy_data_type = std::make_shared<NumpyDataTypeInt>(NumpyDataType::Endianness::LITTLE, sizeof(UInt32), false);
            break;
        case TypeIndex::UInt64:
            numpy_data_type = std::make_shared<NumpyDataTypeInt>(NumpyDataType::Endianness::LITTLE, sizeof(UInt64), false);
            break;
        case TypeIndex::Float32:
            numpy_data_type = std::make_shared<NumpyDataTypeFloat>(NumpyDataType::Endianness::LITTLE, sizeof(Float32));
            break;
        case TypeIndex::Float64:
            numpy_data_type = std::make_shared<NumpyDataTypeFloat>(NumpyDataType::Endianness::LITTLE, sizeof(Float64));
            break;
        case TypeIndex::FixedString:
            numpy_data_type = std::make_shared<NumpyDataTypeString>(
                NumpyDataType::Endianness::NONE, assert_cast<const DataTypeFixedString *>(type.get())->getN());
            break;
        case TypeIndex::String:
            numpy_data_type = std::make_shared<NumpyDataTypeString>(NumpyDataType::Endianness::NONE, 0);
            break;
        case TypeIndex::Array:
            return getNumpyDataType(assert_cast<const DataTypeArray *>(type.get())->getNestedType());
        default:
            nested_data_type = type;
            return false;
    }

    nested_data_type = type;
    return true;
}

void NpyOutputFormat::consume(Chunk chunk)
{
    if (!invalid_shape)
    {
        num_rows += chunk.getNumRows();
        const auto & column = chunk.getColumns()[0];

        if (!is_initialized)
        {
            initShape(column);
            is_initialized = true;
        }

        ColumnPtr nested_column = column;
        checkShape(nested_column);
        updateSizeIfTypeString(nested_column);
        columns.push_back(nested_column);
    }
}

void NpyOutputFormat::initShape(const ColumnPtr & column)
{
    ColumnPtr nested_column = column;
    while (const auto * array_column = typeid_cast<const ColumnArray *>(nested_column.get()))
    {
        auto dim = array_column->getOffsets()[0];
        invalid_shape = dim == 0;
        numpy_shape.push_back(dim);
        nested_column = array_column->getDataPtr();
    }

    if (invalid_shape)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Shape ({}) is invalid, as dimension size cannot be 0", shapeStr());
}

void NpyOutputFormat::checkShape(ColumnPtr & column)
{
    int dim = 0;
    while (const auto * array_column = typeid_cast<const ColumnArray *>(column.get()))
    {
        const auto & array_offset = array_column->getOffsets();

        for (size_t i = 0; i < array_offset.size(); ++i)
            if (array_offset[i] - array_offset[i - 1] != numpy_shape[dim])
            {
                invalid_shape = true;
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ClickHouse doesn't support object types, cannot format ragged nested sequences (which is a list of arrays with different shapes)");
            }

        column = array_column->getDataPtr();
        dim += 1;
    }
}

void NpyOutputFormat::updateSizeIfTypeString(const ColumnPtr & column)
{
    if (nested_data_type->getTypeId() == TypeIndex::String)
    {
        const auto & string_offsets = assert_cast<const ColumnString *>(column.get())->getOffsets();
        for (size_t i = 0; i < string_offsets.size(); ++i)
        {
            size_t string_length = static_cast<size_t>(string_offsets[i] - 1 - string_offsets[i - 1]);
            if (numpy_data_type->getSize() < string_length)
                numpy_data_type->setSize(string_length);
        }
    }
}

void NpyOutputFormat::finalizeImpl()
{
    if (!invalid_shape)
    {
        writeHeader();
        writeColumns();
    }
}

void NpyOutputFormat::writeHeader()
{
    String dict = "{'descr':'" + numpy_data_type->str() + "','fortran_order':False,'shape':(" + shapeStr() + "),}";
    String padding = "\n";

    /// completes the length of the header, which is divisible by 64.
    size_t dict_length = dict.length() + 1;
    size_t header_length = STATIC_HEADER_LENGTH + sizeof(UInt32) + dict_length;
    if (header_length % 64)
    {
        header_length = ((header_length / 64) + 1) * 64;
        dict_length = header_length - STATIC_HEADER_LENGTH - sizeof(UInt32);
        padding = std::string(dict_length - dict.length(), '\x20');
        padding.back() = '\n';
    }

    out.write(STATIC_HEADER, STATIC_HEADER_LENGTH);
    writeBinaryLittleEndian(static_cast<UInt32>(dict_length), out);
    out.write(dict.data(), dict.length());
    out.write(padding.data(), padding.length());
}

void NpyOutputFormat::writeColumns()
{
    for (const auto & column : columns)
    {
        switch (nested_data_type->getTypeId())
        {
            case TypeIndex::Int8: writeNumpyNumbers<ColumnInt8, Int8>(column, out); break;
            case TypeIndex::Int16: writeNumpyNumbers<ColumnInt16, Int16>(column, out); break;
            case TypeIndex::Int32: writeNumpyNumbers<ColumnInt32, Int32>(column, out); break;
            case TypeIndex::Int64: writeNumpyNumbers<ColumnInt64, Int64>(column, out); break;
            case TypeIndex::UInt8: writeNumpyNumbers<ColumnUInt8, UInt8>(column, out); break;
            case TypeIndex::UInt16: writeNumpyNumbers<ColumnUInt16, UInt16>(column, out); break;
            case TypeIndex::UInt32: writeNumpyNumbers<ColumnUInt32, UInt32>(column, out); break;
            case TypeIndex::UInt64: writeNumpyNumbers<ColumnUInt64, UInt64>(column, out); break;
            case TypeIndex::Float32: writeNumpyNumbers<ColumnFloat32, Float32>(column, out); break;
            case TypeIndex::Float64: writeNumpyNumbers<ColumnFloat64, Float64>(column, out); break;
            case TypeIndex::FixedString:
                writeNumpyStrings<ColumnFixedString>(column, numpy_data_type->getSize(), out);
                break;
            case TypeIndex::String:
                writeNumpyStrings<ColumnString>(column, numpy_data_type->getSize(), out);
                break;
            default:
                break;
        }
    }
}

void registerOutputFormatNpy(FormatFactory & factory)
{
    factory.registerOutputFormat("Npy",[](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings &)
    {
        return std::make_shared<NpyOutputFormat>(buf, sample);
    });
    factory.markFormatHasNoAppendSupport("Npy");
    factory.markOutputFormatNotTTYFriendly("Npy");
    factory.setContentType("Npy", "application/octet-stream");
}

}
