#include <Processors/Formats/Impl/JSONColumnsBlockInputFormat.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>

namespace DB
{

JSONColumnsReader::JSONColumnsReader(ReadBuffer & in_) : JSONColumnsReaderBase(in_)
{
}

void JSONColumnsReader::readChunkStart()
{
    skipWhitespaceIfAny(*in);
    assertChar('{', *in);
    skipWhitespaceIfAny(*in);
}

std::optional<String> JSONColumnsReader::readColumnStart()
{
    skipWhitespaceIfAny(*in);
    String name;
    readJSONString(name, *in);
    skipWhitespaceIfAny(*in);
    assertChar(':', *in);
    skipWhitespaceIfAny(*in);
    assertChar('[', *in);
    skipWhitespaceIfAny(*in);
    return name;
}

bool JSONColumnsReader::checkChunkEnd()
{
    skipWhitespaceIfAny(*in);
    if (!in->eof() && *in->position() == '}')
    {
        ++in->position();
        skipWhitespaceIfAny(*in);
        return true;
    }
    return false;
}


void registerInputFormatJSONColumns(FormatFactory & factory)
{
    factory.registerInputFormat(
        "JSONColumns",
        [](ReadBuffer & buf,
           const Block &sample,
           const RowInputFormatParams &,
           const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsBlockInputFormatBase>(buf, sample, settings, std::make_unique<JSONColumnsReader>(buf));
        }
    );
    factory.markFormatSupportsSubsetOfColumns("JSONColumns");
}

void registerJSONColumnsSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "JSONColumns",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsSchemaReaderBase>(buf, settings, std::make_unique<JSONColumnsReader>(buf));
        }
    );
}

}
