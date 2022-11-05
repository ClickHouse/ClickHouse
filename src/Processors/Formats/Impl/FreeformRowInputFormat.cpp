#include "Processors/Formats/Impl/FreeformRowInputFormat.h"
#include "Common/Exception.h"
#include "Formats/EscapingRuleUtils.h"
#include "Formats/FormatFactory.h"
#include "IO/ReadHelpers.h"
#include "Processors/Formats/IRowInputFormat.h"
#include "Processors/Formats/ISchemaReader.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNSUPPORTED_METHOD;
}

FreeformRowInputFormat::FreeformRowInputFormat(
    ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_), format_settings(format_settings_)
{
}

FreeformSchemaReader::FreeformSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowSchemaReader(in_, format_settings_)
{
}

NamesAndTypesList FreeformSchemaReader::readSchema()
{
    for (size_t i = 0; i < max_rows_to_read; ++i)
    {
    }
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unimplemented");
}

static inline void skipWhitespacesAndTabs(ReadBuffer & in)
{
    while (!in.eof() && (*in.position() == ' ' || *in.position() == '\t'))
        ++in.position();
}

std::vector<std::pair<DataTypePtr, char *>> FreeformSchemaReader::readNextPossibleFields()
{
    skipWhitespacesAndTabs(in);
    char * start = in.position();
    std::vector<std::pair<DataTypePtr, char *>> fields;

    for (const auto rule :
         {FormatSettings::EscapingRule::JSON,
          FormatSettings::EscapingRule::CSV,
          FormatSettings::EscapingRule::Quoted,
          FormatSettings::EscapingRule::Escaped,
          FormatSettings::EscapingRule::None})
    {
        String field;
        switch (rule)
        {
            case FormatSettings::EscapingRule::JSON:
                readJSONString(field, in);
                break;
            case FormatSettings::EscapingRule::CSV:
                readCSVField(field, in, format_settings.csv);
                break;
            case FormatSettings::EscapingRule::Quoted:
                readQuotedField(field, in);
                break;
            case FormatSettings::EscapingRule::Escaped:
                readEscapedString(field, in);
                break;
            case FormatSettings::EscapingRule::None:
                readString(field, in);
                break;
            default:
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Freeform text inference only supports JSON, CSV, Quoted, Escaped and None for now");
        }

        DataTypePtr type = determineDataTypeByEscapingRule(field, format_settings, rule);
        if (!type->onlyNull())
            fields.emplace_back(type, in.position());

        in.position() = start; // restart to start position
    }

    return fields;
}


void FreeformSchemaReader::recursivelyGetNextFieldInRow(char * current_pos, DataTypes current, std::vector<DataTypes> & solutions)
{
    char * tmp = in.position();
    in.position() = current_pos;
    if (*in.position() == '\n')
    {
        solutions.push_back(current);
        // not reseting the buffer as we already reach the end of row
        return;
    }

    const auto fields = readNextPossibleFields();
    for (const auto & field : fields)
    {
        auto next = current;
        next.push_back(field.first);
        recursivelyGetNextFieldInRow(field.second, next, solutions);
    }

    in.position() = tmp; // reset to initial position
}


std::vector<DataTypes> FreeformSchemaReader::readRowAndGenerateSolutions()
{
    skipBOMIfExists(in);
    skipWhitespacesAndTabs(in);

    if (in.eof())
        return {};

    std::vector<DataTypes> solutions;
    DataTypes current_result;
    recursivelyGetNextFieldInRow(in.position(), current_result, solutions);

    skipToNextLineOrEOF(in);
    return solutions;
}

void registerInputFormatFreeform(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Freeform",
        [](ReadBuffer & buf, const Block & header, const RowInputFormatParams & params, const FormatSettings & settings)
        { return std::make_shared<FreeformRowInputFormat>(buf, header, params, settings); });
}

void registerFreeformSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Freeform",
        [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<FreeformSchemaReader>(buf, settings); });
}
}
