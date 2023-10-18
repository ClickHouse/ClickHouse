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
#include <Common/Exception.h>
#include "Columns/ColumnArray.h"
#include "Storages/IStorage.h"
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

    assert(depth > 1);
    for (size_t i = 0; i < depth - 1; ++i)
        result_type = std::make_shared<DataTypeArray>(std::move(result_type));
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
    else if (type == "<U10" || type == "<U20" || type == "<U21")
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

NpyRowInputFormat::NpyRowInputFormat(ReadBuffer & in_, Block header_, Params params_)
    : IRowInputFormat(std::move(header_), in_, std::move(params_))
{
    header = parseHeader(*in);
    endian = endianOrientation(header["descr"]);
    nestedType = parseType(header["descr"]);
}

void NpyRowInputFormat::readRows(MutableColumns & columns)
{
    auto & column = columns[0];
    IColumn * current_column = column.get();
    size_t total_elements_to_read = 1;
    for (size_t i = 1; i != shape.size() - 1; ++i)
    {
        total_elements_to_read *= shape[i];
        auto & array_column = assert_cast<ColumnArray &>(*column);
        /// Fill offsets of array columns.
        array_column.getOffsets().push_back(shape[i]);
        current_column = &array_column.getData();
    }

    for (int i = 0; i != shape[0]; ++i)
    {
        for (size_t j = 0; j != total_elements_to_read; ++j)
            readValueAndinsertIntoColumn(*current_column);
        auto a = ColumnArray::create(current_column->getPtr());
        columns.push_back(a->getPtr());
    }
}

void NpyRowInputFormat::readValueAndinsertIntoColumn(IColumn& column)
{
    if (header["descr"] == "<i1")
    {
        DataTypeInt8 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }
    else if (header["descr"] == "<i2")
    {
        DataTypeInt16 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }
    else if (header["descr"] == "<i4")
    {
        DataTypeInt32 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }
    else if (header["descr"] == "<i8")
    {
        DataTypeInt64 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }
    else if (header["descr"] == "<u1")
    {
        DataTypeUInt8 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }
    else if (header["descr"] == "<u2")
    {
        DataTypeUInt16 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }
    else if (header["descr"] == "<u4")
    {
        DataTypeUInt32 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }
    else if (header["descr"] == "<u8")
    {
        DataTypeUInt64 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }
    else if (header["descr"] == "<f2")
    {
        DataTypeFloat32 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }
    else if (header["descr"] == "<f4")
    {
        DataTypeFloat32 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }                                       /// we dont support size of one of floats here.
    else if (header["descr"] == "<f8")
    {
        DataTypeFloat64 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }
    else if (header["descr"] == "<c8" || header["descr"] == "<c16")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support complex numeric type");
    else if (header["descr"] == "|b1")
    {
        DataTypeInt8 value;
        if (endian == -1)
            readBinaryLittleEndian(value, *in);
        else if (endian == 1)
            readBinaryBigEndian(value, *in);
        // else if (endian == 0)
            // readBinary(value, *in);
        column.insertData(value);
    }                /// Not sure that its good idea
    else if (header["descr"] == "<U10" || header["descr"] == "<U20" || header["descr"] == "<U21")
    {
        String value;
        if (endian == -1)
            readStringBinary(value, *in);
        column.insert(value);
    }
    else if (header["descr"] == "O")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support object types");
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while parsing data type");

}


void NpyRowInputFormat::readFromBuffer(MutableColumns & columns)
{
    while (*in->position() != '\n')
        ++in->position();
    ++in->position();
    size_t total_size = 1;
    for (int dim_size : shape)
        total_size *= dim_size;

    for (size_t i = 0; i < total_size; i++)
    {
        if (in->eof())
        {
            throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream in Npy format");
        }
        else if (*in->position() == '\t')
        {
            ++in->position();
            continue;
        }
        else if (*in->position() == '\n')
        {
            ++in->position();
            break;
        }
        
        readRows(columns);
    }
}

bool NpyRowInputFormat::readRow([[maybe_unused]]MutableColumns & columns, RowReadExtension &  /*ext*/)
{
    if (in->eof())
        return false;

    while (*in->position() != '\n')
        ++in->position();
    ++in->position();

    if (unlikely(*in->position() == '\n'))
    {
        /// An empty string. It is permissible, but it is unclear why.
        ++in->position();
    }
    else
        readFromBuffer(columns);

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
    factory.registerInputFormat("npy", [](
        ReadBuffer & buf,
        const Block & sample,
        IRowInputFormat::Params params,
        const FormatSettings &)
    {
        return std::make_shared<NpyRowInputFormat>(buf, sample, std::move(params));
    });

    factory.markFormatSupportsSubsetOfColumns("npy");
}
void registerNpySchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("Npy", [](ReadBuffer & buf, const FormatSettings &)
    {
        return std::make_shared<NpySchemaReader>(buf);
    });
}

}
