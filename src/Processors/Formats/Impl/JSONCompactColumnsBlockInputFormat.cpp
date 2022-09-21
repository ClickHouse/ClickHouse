#include <Processors/Formats/Impl/JSONCompactColumnsBlockInputFormat.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>

namespace DB
{

JSONCompactColumnsReader::JSONCompactColumnsReader(ReadBuffer & in_) : JSONColumnsReaderBase(in_)
{
}

void JSONCompactColumnsReader::readChunkStart()
{
    skipWhitespaceIfAny(*in);
    assertChar('[', *in);
    skipWhitespaceIfAny(*in);
}

std::optional<String> JSONCompactColumnsReader::readColumnStart()
{
    skipWhitespaceIfAny(*in);
    assertChar('[', *in);
    skipWhitespaceIfAny(*in);
    return std::nullopt;
}

bool JSONCompactColumnsReader::checkChunkEnd()
{
    skipWhitespaceIfAny(*in);
    if (!in->eof() && *in->position() == ']')
    {
        ++in->position();
        skipWhitespaceIfAny(*in);
        return true;
    }
    return false;
}


void registerInputFormatJSONCompactColumns(FormatFactory & factory)
{
    factory.registerInputFormat(
        "JSONCompactColumns",
        [](ReadBuffer & buf,
           const Block &sample,
           const RowInputFormatParams &,
           const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsBlockInputFormatBase>(buf, sample, settings, std::make_unique<JSONCompactColumnsReader>(buf));
        }
    );
}

void registerJSONCompactColumnsSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "JSONCompactColumns",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsSchemaReaderBase>(buf, settings, std::make_unique<JSONCompactColumnsReader>(buf));
        }
    );
}

}
