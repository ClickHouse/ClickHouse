#include <IO/ReadHelpers.h>
#include <iterator>
#include <memory>
#include <string>
#include <vector>
#include <type_traits>
#include <unordered_map>
#include <Processors/Formats/Impl/NpyRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeString.h>
#include "Common/Exception.h"
#include "Columns/IColumn.h"
#include "Core/Field.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/IDataType.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "IO/ReadBuffer.h"
#include "IO/WriteHelpers.h"
#include "Processors/Formats/IRowInputFormat.h"
#include "base/types.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}


NpyRowInputFormat::NpyRowInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(std::move(header_), in_, std::move(params_)), format_settings(format_settings_), name_map(getPort().getHeader().columns())
{
    const auto & sample_block = getPort().getHeader();
    size_t num_columns = sample_block.columns();
    for (size_t i = 0; i < num_columns; ++i)
        name_map[sample_block.getByPosition(i).name] = i;
}

template <typename T>
void readFromBuffer(ReadBuffer &in, MutableColumns &  /*columns*/, std::vector<int> shape)
{
    while (*in.position() != '\n')
        ++in.position();
    ++in.position();
    size_t total_size = 1;
    for (int dim_size : shape)
        total_size *= dim_size;

    for (size_t i = 0; i < total_size; i++)
    {
        if (in.eof())
        {
            throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream in Npy format");
        }
        else if (*in.position() == '\t')
        {
            ++in.position();
            continue;
        }
        else if (*in.position() == '\n')
        {
            ++in.position();
            break;
        }
        
        T value;
        readBinaryLittleEndian(value, in);
    }
}

template <typename T>
void readStringFromBuffer(ReadBuffer &in, std::vector<int> shape)
{
    while (*in.position() != '\n')
        ++in.position();
    size_t total_size = 1;
    for (int dim_size : shape)
        total_size *= dim_size;

    for (size_t i = 0; i < total_size; i++)
    {
        if (in.eof())
        {
            throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream in Npy format");
        }
        else if (*in.position() == '\t')
        {
            ++in.position();
            continue;
        }
        // else if (*in.position() == '\n')
        // {
        //     ++in.position();
        //     break;
        // }

        T value;
        readStringBinary(value, in);
        std::cout << value << std::endl;
    }
}

void readAndParseType(String type, ReadBuffer &in, MutableColumns & columns, std::vector<int> shape) //is ok
{
    if (type == "<i1")
        readFromBuffer<Int8>(in, columns, shape);
    else if (type == "<i2")
        readFromBuffer<Int16>(in, columns, shape);
    else if (type == "<i4")
        readFromBuffer<Int32>(in, columns, shape);
    else if (type == "<i8")
        readFromBuffer<Int64>(in, columns, shape);
    else if (type == "<u1")
        readFromBuffer<UInt8>(in, columns, shape);
    else if (type == "<u2")
        readFromBuffer<UInt16>(in, columns, shape);
    else if (type == "<u4")
        readFromBuffer<UInt32>(in, columns, shape);
    else if (type == "<u8")
        readFromBuffer<UInt64>(in, columns, shape);
    else if (type == "<f2")
        readFromBuffer<Float32>(in, columns, shape);
    else if (type == "<f4")
        readFromBuffer<Float32>(in, columns, shape);
    else if (type == "<f8")
        readFromBuffer<Float64>(in, columns, shape);
    else if (type == "<c8" || type == "<c16")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support complex numeric type");
    else if (type == "|b1")
        readFromBuffer<Int8>(in, columns, shape);
    else if (type == "<U10" || type == "<U20" || type == "<U21")
        readStringFromBuffer<String>(in, shape);
    else if (type == "O")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support object types");
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while parsing data type");
}

bool NpyRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &  /*ext*/)
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
        readAndParseType(header["descr"], *in, columns, shape);

    return true;
}

void NpyRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

void NpyRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    read_columns.clear();
    seen_columns.clear();
    name_buf.clear();
}

size_t NpyRowInputFormat::countRows(size_t max_block_size)
{
    size_t num_rows = 0;
    while (!in->eof() && num_rows < max_block_size)
    {
        skipToUnescapedNextLineOrEOF(*in);
        ++num_rows;
    }

    return num_rows;
}

NpySchemaReader::NpySchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowWithNamesSchemaReader(in_, format_settings_, getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule::Escaped))
{
}

[[maybe_unused]]static size_t readNpySize(ReadBuffer & in)
{
    NpySizeT size;
    readBinaryLittleEndian(size, in);
    return size;
}

[[maybe_unused]]static String readNpyHeader(ReadBuffer & in)
{
    String header;
    readBinary(header, in);
    return header;
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

void NpyRowInputFormat::readPrefix()
{
    const char * begin_pos = find_first_symbols<'\''>(in->position(), in->buffer().end());
    String text(begin_pos);
    std::unordered_map<String, String> header_map;

    // Finding fortran_order
    size_t loc1 = text.find("fortran_order");
    if (loc1 == std::string::npos)
        throw Exception(ErrorCodes::INCORRECT_DATA, "failed to find header keyword 'fortran_order'");
    header_map["fortran_order"] = (text.substr(loc1+16, 4) == "True" ? "true" : "false");

    // Finding shape
    loc1 = text.find('(');
    size_t loc2 = text.find(')');
    if (loc1 == std::string::npos || loc2 == std::string::npos)
        throw Exception(ErrorCodes::INCORRECT_DATA, "failed to find header keyword '(' or ')'");
    header_map["shape"] = text.substr(loc1, loc2 - loc1 + 1);

    // Finding descr
    loc1 = text.find("descr");
    loc2 = loc1 + 9;
    while (text[loc2] != '\'')
        loc2++;
    if (loc1 == std::string::npos)
        throw Exception(ErrorCodes::INCORRECT_DATA, "failed to find header keyword 'descr'");
    header_map["descr"] = (text.substr(loc1+9, loc2 - loc1 - 9));

    header = header_map;
    shape = parseShape(header_map["shape"]);
}

NamesAndTypesList NpySchemaReader::readRowAndGetNamesAndDataTypes(bool & eof)
{
    if (first_row)
    {
        skipBOMIfExists(in);
        first_row = false;
    }

    if (in.eof())
    {
        eof = true;
        return {};
    }

    if (*in.position() == '\n')
    {
        ++in.position();
        return {};
    }

    return {};
}

size_t nthSubstr(int n, const String& s,
              const String& p)
{
   String::size_type i = s.find(p);     // Find the first occurrence

   int j;
   for (j = 1; j < n && i != String::npos; ++j)
      i = s.find(p, i+1); // Find the next occurrence

   if (j == n)
     return(i);
   else
     return(-1);
}

// String NpySchemaReader::readHeader(bool & eof)
// {
//     if (first_row)
//     {
//         skipBOMIfExists(in);
//         first_row = false;
//     }

//     if (in.eof())
//     {
//         eof = true;
//         return {};
//     }

//     if (*in.position() == '\n')
//     {
//         ++in.position();
//         return {};
//     }

//     // NamesAndTypesList names_and_types;
//     StringRef name_ref;
//     String name_buf;
//     // readName(in, name_ref, name_buf);
//     String text = String(name_ref);
//     String res;

//     size_t pos = text.find('{');
//     std::map<String, String> header;
//     if (pos != String::npos) {
//         // Find the closing curly brace.
//         size_t end = text.find('}', pos + 1);
//         if (end != String::npos)
//         {
//         // Get the text in curly braces.
//         res = text.substr(pos + 1, end - pos - 1);

//         // Print the text in curly braces.
//         }
//         header["descr"] = res.substr(nthSubstr(1, res, "'descr':")+10, nthSubstr(1, res, "',")-2);
//     }
//     return text;
// }

void registerInputFormatNpy(FormatFactory & factory)
{
    factory.registerInputFormat("npy", [](
        ReadBuffer & buf,
        const Block & sample,
        IRowInputFormat::Params params,
        const FormatSettings & settings)
    {
        return std::make_shared<NpyRowInputFormat>(buf, sample, std::move(params), settings);
    });

    factory.markFormatSupportsSubsetOfColumns("npy");
}
void registerNpySchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("Npy", [](ReadBuffer & buf, const FormatSettings & settings)
    {
        return std::make_shared<NpySchemaReader>(buf, settings);
    });
    factory.registerAdditionalInfoForSchemaCacheGetter("npy", [](const FormatSettings & settings)
    {
        return getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::Escaped);
    });
}

}
