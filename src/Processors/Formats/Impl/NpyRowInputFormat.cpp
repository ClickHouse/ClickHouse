#include <IO/ReadHelpers.h>
#include <cstddef>
#include <iterator>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include <type_traits>
#include <unordered_map>
#include <Processors/Formats/Impl/NpyRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/IStorage.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <base/types.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}


DataTypePtr createDataType(size_t depth, DataTypePtr nested_type)
{
    DataTypePtr result_type = nested_type;

    assert(depth > 0);
    if (depth > 1)
    {
        for (size_t i = 0; i < depth - 1; ++i)
            result_type = std::make_shared<DataTypeArray>(std::move(result_type));
    }
    return result_type;
}

/*
Checks, in what endian format data was written.
return -1: if data is written in little-endian;

         1: if data is written in big-endian;

         0: if data is written in no-endian. */
int endianOrientation(String descr)
{
    if (descr.length() < 3)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Descr field length must be bigger or equal 3");
    if (descr[0] == '<')
        return -1;
    else if (descr[0] == '>')
        return 1;
    else if (descr[0] == '|')
        return 0;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong content of field descr");
}

DataTypePtr parseType(String type)
{
    if (type == "<i1")
        return std::make_shared<DataTypeInt8>();
    else if (type == "<i2")
        return std::make_shared<DataTypeInt16>();
    else if (type == "<i4")
        return std::make_shared<DataTypeInt32>();
    else if (type == "<i8")
        return std::make_shared<DataTypeInt64>();
    else if (type == "<u1")
        return std::make_shared<DataTypeUInt8>();
    else if (type == "<u2")
        return std::make_shared<DataTypeUInt16>();
    else if (type == "<u4")
        return std::make_shared<DataTypeUInt32>();
    else if (type == "<u8")
        return std::make_shared<DataTypeUInt64>();
    else if (type == "<f2")
        return std::make_shared<DataTypeFloat32>();
    else if (type == "<f4")
        return std::make_shared<DataTypeFloat32>();
    else if (type == "<f8")
        return std::make_shared<DataTypeFloat64>();
    else if (type == "<c8" || type == "<c16")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support complex numeric type");
    else if (type == "|b1")
        return std::make_shared<DataTypeInt8>();
    else if (type[1] == 'U' || type[1] == 'S')
        return std::make_shared<DataTypeString>();
    else if (type == "O")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support object types");
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while parsing data type");
}

std::vector<int> parseShape(String shapeString)
{
    shapeString.erase(std::remove(shapeString.begin(), shapeString.end(), '('), shapeString.end());
    shapeString.erase(std::remove(shapeString.begin(), shapeString.end(), ')'), shapeString.end());

    // Use a string stream to extract integers
    std::istringstream ss(shapeString);
    int value;
    char comma; // to handle commas between values

    std::vector<int> shape;

    while (ss >> value) {
        shape.push_back(value);
        ss >> comma; // read the comma
    }
    return shape;
}

std::unordered_map<String, String> parseHeader(ReadBuffer &buf)
{
    /// Check magic bytes
    const char * magic_string = "\x93NUMPY";
    assertString(magic_string, buf);

    /// Read npy version.
    UInt8 version_major;
    UInt8 version_minor;
    readBinary(version_major, buf);
    readBinary(version_minor, buf);

    /// Read header length.
    UInt32 header_length;
    /// In v1 header length is 2 bytes, in v2 - 4 bytes.
    if (version_major == 1)
    {
        UInt16 header_length_u16;
        readBinaryLittleEndian(header_length_u16, buf);
        header_length = header_length_u16;
    }
    else
    {
        readBinaryLittleEndian(header_length, buf);
    }

    /// Remember current count of read bytes to skip remaining
    /// bytes in header when we find all required fields.
    size_t header_start = buf.count();

    /// Start parsing header.
    String shape;
    String descr;

    assertChar('{', buf);
    skipWhitespaceIfAny(buf);
    bool first = true;
    while (!checkChar('}', buf))
    {
        /// Skip delimiter between key-value pairs.
        if (!first)
        {
            skipWhitespaceIfAny(buf);
        }
        else
        {
            first = false;
        }

        /// Read map key.
        String key;
        readQuotedField(key, buf);
        assertChar(':', buf);
        skipWhitespaceIfAny(buf);
        /// Read map value.
        String value;
        readQuotedField(value, buf);
        assertChar(',', buf);
        skipWhitespaceIfAny(buf);

        if (key == "'descr'")
            descr = value;
        else if (key == "'fortran_order'")
        {
            if (value != "false")
                throw Exception(ErrorCodes::INCORRECT_DATA, "Fortran order is not supported");
        }
        else if (key == "'shape'")
            shape = value;
    }

    if (shape.empty() || descr.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "npy file header doesn't contain required field 'shape' or 'descr'");

    size_t read_bytes = buf.count() - header_start;
    if (read_bytes > header_length)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Header size is incorrect");

    /// Ignore remaining header data.
    buf.ignore(header_length - read_bytes);

    if (descr[0] == '\'')
        descr = descr.substr(1, descr.length() - 1);
    if (descr[descr.length() - 1] == '\'')
        descr = descr.substr(0, descr.length() - 1);

    if (shape[0] == '\'')
        shape = shape.substr(1, shape.length() - 1);
    if (shape[shape.length() - 1] == '\'')
        shape = shape.substr(0, shape.length() - 1);

    std::unordered_map<String, String> header_data;
    header_data["shape"] = shape;
    header_data["descr"] = descr;

    return header_data;
}

int parseStringSize(const std::string type)
{
    int size;
    try
    {
        size = std::stoi(type.substr(2, type.length() - 2));
        return size;
    }
    catch (...)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid data type");
    }
}

NpyRowInputFormat::NpyRowInputFormat(ReadBuffer & in_, Block header_, Params params_)
    : IRowInputFormat(std::move(header_), in_, std::move(params_))
{
    header = parseHeader(*in);
    endian = endianOrientation(header["descr"]);
    shape = parseShape(header["shape"]);
    nestedType = parseType(header["descr"]);

    if (isString(nestedType))
        sizeForStrings = parseStringSize(header["descr"]);
}

void NpyRowInputFormat::readRows(MutableColumns & columns)
{
    auto & column = columns[0];
    IColumn * current_column = column.get();
    size_t elements_in_current_column = 1;
    for (size_t i = 1; i != shape.size(); ++i)
    {
        auto & array_column = assert_cast<ColumnArray &>(*current_column);
        /// Fill offsets of array columns.
        for (size_t j = 0; j != elements_in_current_column; ++j)
            array_column.getOffsets().push_back(array_column.getOffsets().back() + shape[i]);
        current_column = &array_column.getData();
        elements_in_current_column *= shape[i];
    }

    for (size_t i = 0; i != elements_in_current_column; ++i)
        readValueAndinsertIntoColumn(current_column->getPtr());
}

void NpyRowInputFormat::readValueAndinsertIntoColumn(MutableColumnPtr column)
{
    if (auto * column_int8 = typeid_cast<ColumnInt8 *>(column.get()))
    {
        Int8 value = 0;
        endian == 1 ? readBinaryBigEndian(value, *in) : readBinaryLittleEndian(value, *in);
        column_int8->insertValue(value);
    }
    else if (auto * column_int16 = typeid_cast<ColumnInt16 *>(column.get()))
    {
        Int16 value = 0;
        endian == 1 ? readBinaryBigEndian(value, *in) : readBinaryLittleEndian(value, *in);
        column_int16->insertValue(value);
    }
    else if (auto * column_int32 = typeid_cast<ColumnInt32 *>(column.get()))
    {
        Int32 value = 0;
        endian == 1 ? readBinaryBigEndian(value, *in) : readBinaryLittleEndian(value, *in);
        column_int32->insertValue(value);
    }
    else if (auto * column_int64 = typeid_cast<ColumnInt64 *>(column.get()))
    {
        Int64 value = 0;
        endian == 1 ? readBinaryBigEndian(value, *in) : readBinaryLittleEndian(value, *in);
        column_int64->insertValue(value);
    }
    else if (auto * column_uint8 = typeid_cast<ColumnUInt8 *>(column.get()))
    {
        UInt8 value = 0;
        endian == 1 ? readBinaryBigEndian(value, *in) : readBinaryLittleEndian(value, *in);
        column_uint8->insertValue(value);
    }
    else if (auto * column_uint16 = typeid_cast<ColumnUInt16 *>(column.get()))
    {
        UInt16 value = 0;
        endian == 1 ? readBinaryBigEndian(value, *in) : readBinaryLittleEndian(value, *in);
        column_uint16->insertValue(value);
    }
    else if (auto * column_uint32 = typeid_cast<ColumnUInt32 *>(column.get()))
    {
        UInt32 value = 0;
        endian == 1 ? readBinaryBigEndian(value, *in) : readBinaryLittleEndian(value, *in);
        column_uint32->insertValue(value);
    }
    else if (auto * column_uint64 = typeid_cast<ColumnUInt64 *>(column.get()))
    {
        UInt64 value = 0;
        endian == 1 ? readBinaryBigEndian(value, *in) : readBinaryLittleEndian(value, *in);
        column_uint64->insertValue(value);
    }
    else if (auto * column_float32 = typeid_cast<ColumnFloat32 *>(column.get()))
    {
        Float32 value = 0;
        endian == 1 ? readBinaryBigEndian(value, *in) : readBinaryLittleEndian(value, *in);
        column_float32->insertValue(value);
    }
    else if (auto * column_float64 = typeid_cast<ColumnFloat64 *>(column.get()))
    {
        Float64 value = 0;
        endian == 1 ? readBinaryBigEndian(value, *in) : readBinaryLittleEndian(value, *in);
        column_float64->insertValue(value);
    }
    else if (auto * column_string = typeid_cast<ColumnString *>(column.get()))
    {
        size_t size = sizeForStrings;
        String tmp;
        if (header["descr"][1] == 'U')
            size = sizeForStrings * 4;

        tmp.resize(size);
        in->readStrict(tmp.data(), size);
        tmp.erase(std::remove(tmp.begin(), tmp.end(), '\0'), tmp.end());
        column_string->insertData(tmp.c_str(), tmp.size());
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error while reading data");
}

bool NpyRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &  /*ext*/)
{
    if (in->eof())
        return false;

    if (unlikely(*in->position() == '\n'))
    {
        /// An empty string. It is permissible, but it is unclear why.
        ++in->position();
    }
    else
        readRows(columns);

    return true;
}

void NpyRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    shape.clear();
}

NpySchemaReader::NpySchemaReader(ReadBuffer & in_)
    : ISchemaReader(in_) {}

NamesAndTypesList NpySchemaReader::readSchema()
{
    if (first_row)
    {
        skipBOMIfExists(in);
        first_row = false;
    }

    if (in.eof())
    {
        return {};
    }

    if (*in.position() == '\n')
    {
        ++in.position();
        return {};
    }

    auto header = parseHeader(in);
    std::vector<int> shape = parseShape(header["shape"]);
    DataTypePtr nested_type = parseType(header["descr"]);

    DataTypePtr result_type = createDataType(shape.size(), nested_type);

    return {{"array", result_type}};
}

void registerInputFormatNpy(FormatFactory & factory)
{
    factory.registerInputFormat("Npy", [](
        ReadBuffer & buf,
        const Block & sample,
        IRowInputFormat::Params params,
        const FormatSettings &)
    {
        return std::make_shared<NpyRowInputFormat>(buf, sample, std::move(params));
    });

    factory.markFormatSupportsSubsetOfColumns("Npy");
}
void registerNpySchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("Npy", [](ReadBuffer & buf, const FormatSettings &)
    {
        return std::make_shared<NpySchemaReader>(buf);
    });
}

}
