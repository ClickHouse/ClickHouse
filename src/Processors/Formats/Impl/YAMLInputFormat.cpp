#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Formats/verbosePrintString.h>
#include <Processors/Formats/Impl/YAMLInputFormat.h>
#include <Formats/FormatFactory.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeNothing.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

std::vector<String> parseHeader(String & s, ReadBuffer & in)
{
    std::vector<String> result;
    readEscapedStringUntilEOL(s, in);
    if(s == "---")
    {
        skipToNextLineOrEOF(in);
        readStringUntilWhitespace(s, in);
        if(s == "attributes:")
        {
            while(*in.position() != '\n')
            {
                ++in.position();
                skipWhitespaceIfAny(in);
                readYAMLHeader(s, in);
                result.push_back(s);
            }
            skipToNextLineOrEOF(in);
            readEscapedStringUntilEOL(s, in);
            if(s == "---")
            {
                skipToNextLineOrEOF(in);
                return result;
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Header must end with ---");
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "There must be an attributes list inside the header");
        }
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There must be a header at the beginning of the file");
    }
}

YAMLInputFormat::YAMLInputFormat(
    const Block & header_,
    ReadBuffer & in_,
    const Params & params_,
    const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_)
    , format_settings(format_settings_)
{
    String s;
    attrs = parseHeader(s, in);
    num_columns = attrs.size() + 3;

    current_id = 1;
    spaces = 0;
    parents[0] = 0;
    wasSet = false;
}

int skipWhitespaceIfAnyAndGetDiff(int spaces, ReadBuffer & in)
{
    int count = 0;
    while (!in.eof() && isWhitespaceASCIIOneLine(*in.position()))
    {
        ++in.position();
        ++count;
    }
    return count - spaces;
}

std::pair<String, String> parseString(String & s, ReadBuffer & in)
{
    readStringUntilWhitespace(s, in);
    // this is needed to delete ':'.
    s.pop_back();
    String key = s;
    String value;
    if (s != "set")
    {
        skipWhitespaceIfAny(in);
        readEscapedStringUntilEOL(s, in);
        value = s;
    }
    else
    {
        value = "";
    }
    skipToNextLineOrEOF(in);
    return std::pair<String, String>(key, value);
}

void YAMLInputFormat::saveData(MutableColumns & columns, int id, int parent_id, String regexp, std::map<String, String> dataFromYAML)
{
    columns[0]->insert(id);
    columns[1]->insert(parent_id);
    columns[2]->insert(regexp);
    for(size_t i = 0; i < attrs.size(); ++i)
    {
        auto it = dataFromYAML.find(attrs[i]);
        if(it != dataFromYAML.end())
        {
            columns[i+3]->insert(it->second);
        }
        else
        {
            columns[i+3]->insertDefault();
        }
    }
}

bool YAMLInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if(in.eof())
    {
	return false;
    }
    String s;
    int diff = skipWhitespaceIfAnyAndGetDiff(spaces, in);
    spaces+=diff;
    std::pair<String, String> keyAndValue = parseString(s, in);
    if(spaces == 0 && diff == 0)
    {
        if(keyAndValue.first == "match")
        {
            name = keyAndValue.second;
        }
        else if(keyAndValue.first == "set")
        {
            wasSet = true;
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected match or set, your string is {}, value is {}, spaces are {}, diff is {}", keyAndValue.first, keyAndValue.second, spaces, diff);
        }
    }
    else if(diff == 1)
    {
        if(keyAndValue.first == "match")
        {
            saveData(columns, current_id, parents[spaces-1], name, data);
            parents[spaces] = current_id;
            data.clear();
            name = keyAndValue.second;
            ++current_id;
        }
        else if(keyAndValue.first == "set")
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Indentation problem if diff = 1: key is {}, value is {}, spaces are {}, diff is {}", keyAndValue.first, keyAndValue.second, spaces, diff);
        }
        else
        {
            data[keyAndValue.first] = keyAndValue.second;
            parents[spaces] = parents[spaces-1];
            wasSet = false;
        }
    }
    else if(diff == 0)
    {
        if(keyAndValue.first == "set")
        {
            wasSet = true;
        }
        else if(keyAndValue.first == "match")
        {
            if(wasSet)
            {
                saveData(columns, current_id, parents[spaces], name, data);
                data.clear();
                name = keyAndValue.second;
                ++current_id;
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Indentation problem if diff = 0: key is {}, value is {}, spaces are {}, diff is {}", keyAndValue.first, keyAndValue.second, spaces, diff);
            }
        }
        else
        {
            data[keyAndValue.first] = keyAndValue.second;
            wasSet = false;
        }
    }
    else if(diff < 0)
    {
        if(keyAndValue.first == "match")
        {
            saveData(columns, current_id, parents[spaces-diff], name, data);
            data.clear();
            name = keyAndValue.second;
            ++current_id;
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected match:, your string is {}, value is {}, spaces is {}, diff is {}", keyAndValue.first, keyAndValue.second, spaces, diff);
        }
    }
    if(in.eof())
    {
        saveData(columns, current_id, parents[spaces], name, data);
    }
    return true;
}



void registerInputFormatProcessorYAML(FormatFactory & factory)
{
    factory.registerInputFormatProcessor( "YAML", [](
	ReadBuffer & buf,
	const Block & sample,
	IRowInputFormat::Params params,
	const FormatSettings & settings)
    {
	return std::make_shared<YAMLInputFormat>(sample, buf, params, settings);
    });
}	

}
