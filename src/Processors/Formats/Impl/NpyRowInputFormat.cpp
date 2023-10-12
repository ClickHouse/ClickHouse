#include <IO/ReadHelpers.h>
#include <iterator>
#include <memory>
#include <regex>
#include <string>
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
#include "DataTypes/Serializations/ISerialization.h"
#include "IO/ReadBuffer.h"
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
        name_map[sample_block.getByPosition(i).name] = i;        /// NOTE You could place names more cache-locally.
}

/** Read the field name in the `Npy` format.
  * Return true if the field is followed by an equal sign,
  *  otherwise (field with no value) return false.
  * The reference to the field name will be written to `ref`.
  * A temporary `tmp` buffer can also be used to copy the field name to it.
  * When reading, skips the name and the equal sign after it.
  */
static bool readName(ReadBuffer & buf, StringRef & ref, String & tmp)
{
    tmp.clear();

    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'\t', '\n', '\\', '='>(buf.position(), buf.buffer().end());

        if(*next_pos == '*')
            break;

        if (next_pos == buf.buffer().end())
        {
            tmp.append(buf.position(), next_pos - buf.position());
            buf.position() = buf.buffer().end();
            buf.next();
            continue;
        }

        /// Came to the end of the name.
        if (*next_pos != '\\')
        {
            bool have_value = *next_pos == '=';
            if (tmp.empty())
            {
                /// No need to copy data, you can refer directly to the `buf`.
                ref = StringRef(buf.position(), next_pos - buf.position());
                buf.position() += next_pos + have_value - buf.position();
            }
            else
            {
                /// Copy the data to a temporary string and return a reference to it.
                tmp.append(buf.position(), next_pos - buf.position());
                buf.position() += next_pos + have_value - buf.position();
                ref = StringRef(tmp);
            }
            return have_value;
        }
        /// The name has an escape sequence.
        else
        {
            tmp.append(buf.position(), next_pos - buf.position());
            buf.position() += next_pos + 1 - buf.position();
            if (buf.eof())
                throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Cannot parse escape sequence");

            tmp.push_back(parseEscapeSequence(*buf.position()));
            ++buf.position();
            continue;
        }
    }

    throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream while reading key name from Npy format");
}

template <typename T>
T readItem(T &value, ReadBuffer &in)
{
    readBinaryLittleEndian(value, in);
    return value;
}

void read(String type, ReadBuffer &in)
{
    if (type == "<i1")
    {
        Int8 value;
        readItem(value, in);
    }
    else if (type == "<i2")
    {
        Int16 value;
        readItem(value, in);
    }
    else if (type == "<i4")
    {
        Int32 value;
        readItem(value, in);
    }
    else if (type == "<i8")
    {
        Int64 value;
        readItem(value, in);
    }
    else if (type == "<u1")
    {
        UInt8 value;
        readItem(value, in);
    }
    else if (type == "<u2")
    {
        UInt16 value;
        readItem(value, in);
    }
    else if (type == "<u4")
    {
        UInt32 value;
        readItem(value, in);
    }
    else if (type == "<u8")
    {
        UInt64 value;
        readItem(value, in);
    }
    else if (type == "<f2")
    {
        Float32 value;
        readItem(value, in);
    }
    else if (type == "<f4")
    {
        Float32 value;
        readItem(value, in);
    }
    else if (type == "<f8")
    {
        Float64 value;
        readItem(value, in);
    }
    else if (type == "<c8" || type == "<c16")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support complex numeric type");
    else if (type == "|b1")
    {
        Int8 value;
        readItem(value, in);
    }
    else if (type == "<U10" || type == "<U20")
    {
        String value;
        readItem(value, in);
    }
    else if (type == "O")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support object types");
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support this type of data");
}

bool NpyRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    // while (!in.eof())
    // {
    //     [[maybe_unused]] auto *pos = in.position();
    //     Float64 value_float;
    //     readBinaryLittleEndian(value_float, in);
    //     std::cout << "value: " << value_float << std::endl;
    // }

    if (in->eof())
        return false;

    while (*in->position() != '\n')
        ++in->position();
    ++in->position();

    const auto & header_local = getPort().getHeader();
    size_t num_columns = columns.size();

    /// Set of columns for which the values were read. The rest will be filled with default values.
    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);

    if (unlikely(*in->position() == '\n'))
    {
        /// An empty string. It is permissible, but it is unclear why.
        ++in->position();
    }
    else
    {
        while (true)
        {
            StringRef name_ref;
            bool has_value = readName(*in, name_ref, name_buf);
            ssize_t index = -1;

            if (has_value)
            {
                /// NOTE Optimization is possible by caching the order of fields (which is almost always the same)
                /// and quickly checking for the next expected field, instead of searching the hash table.

                auto * it = name_map.find(name_ref);
                if (!it)
                {
                    if (!format_settings.skip_unknown_fields)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown field found while parsing Npy format: {}", name_ref.toString());

                    /// If the key is not found, skip the value.
                    NullOutput sink;
                    readEscapedStringInto(sink, *in);
                }
                else
                {
                    index = it->getMapped();

                    if (seen_columns[index])
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate field found while parsing Npy format: {}", name_ref.toString());

                    seen_columns[index] = read_columns[index] = true;
                    const auto & type = getPort().getHeader().getByPosition(index).type;
                    const auto & serialization = serializations[index];
                    if (format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type))
                        read_columns[index] = SerializationNullable::deserializeTextEscapedImpl(*columns[index], *in, format_settings, serialization);
                    else
                        serialization->deserializeTextEscaped(*columns[index], *in, format_settings);
                }
            }
            else
            {
                /// The only thing that can go without value is `Npy` fragment that is ignored.
                if (!(name_ref.size == 4 && 0 == memcmp(name_ref.data, "npy", 4)))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Found field without value while parsing Npy format: {}", name_ref.toString());
            }

            if (in->eof())
            {
                throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream after field in Npy format: {}", name_ref.toString());
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
            else
            {
                /// Possibly a garbage was written into column, remove it
                if (index >= 0)
                {
                    columns[index]->popBack(1);
                    seen_columns[index] = read_columns[index] = false;
                }

                throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Found garbage after field in Npy format: {}", name_ref.toString());
            }
        }
    }

    /// Fill in the not met columns with default values.
    for (size_t i = 0; i < num_columns; ++i)
        if (!seen_columns[i])
            header_local.getByPosition(i).type->insertDefaultInto(*columns[i]);

    /// return info about defaults set
    if (format_settings.defaults_for_omitted_fields)
        ext.read_columns = read_columns;
    else
        ext.read_columns.assign(num_columns, true);

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

DataTypePtr parseType(String type) //is ok
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
    else if (type == "<U10" || type == "<U20")
        return std::make_shared<DataTypeString>();
    else if (type == "O")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support object types");
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouse doesn't support this type of data");
}

Tuple parseShape(String shapeString)
{
    shapeString.erase(std::remove(shapeString.begin(), shapeString.end(), '('), shapeString.end());
    shapeString.erase(std::remove(shapeString.begin(), shapeString.end(), ')'), shapeString.end());

    // Use a string stream to extract integers
    std::istringstream ss(shapeString);
    int value;
    char comma; // to handle commas between values

    Tuple shape;

    while (ss >> value) {
        shape.push_back(value);
        ss >> comma; // read the comma
    }
    return shape;
}

void NpyRowInputFormat::readPrefix() //is ok
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

    data_type = parseType(header_map["descr"]);
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

    NamesAndTypesList names_and_types;
    String name_buf;
    String value;
    DataTypePtr data_type;
    {
        const char * begin_pos = find_first_symbols<'\''>(in.position(), in.buffer().end());
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

        data_type = parseType(header_map["descr"]);
        Tuple shape_tuple = parseShape(header_map["shape"]);
        [[maybe_unused]] std::unordered_map<String, String> header = header_map;
    }
    while (*in.position() != '\n')
    {
        [[maybe_unused]] auto *pos = in.position();
        in.ignore(1);
    }
    in.ignore(1);
    while (!in.eof())
    {
        [[maybe_unused]] auto *pos = in.position();
        Float64 value_float;
        readBinaryLittleEndian(value_float, in);
        std::cout << "value: " << value_float << std::endl;
    }

    const char * begin_pos = find_first_symbols<'\''>(in.position(), in.buffer().end());
    String header(begin_pos);
    std::unordered_map<String, String> header_map;

    // Make getting headers without searching everytime

    return names_and_types;
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
//     readName(in, name_ref, name_buf);
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
