#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONCompactEachRowWithNamesAndTypesRowOutputFormat.h>
#include <Formats/FormatFactory.h>

namespace DB
{

JSONCompactEachRowWithNamesAndTypesRowOutputFormat::JSONCompactEachRowWithNamesAndTypesRowOutputFormat(WriteBuffer & out_,
        const Block & header_,
        FormatFactory::WriteCallback callback,
        const FormatSettings & settings_)
        : JSONCompactEachRowRowOutputFormat(out_, header_, callback, settings_)
{
    auto & sample = getPort(PortKind::Main).getHeader();
    NamesAndTypesList columns(sample.getNamesAndTypesList());
    fields.assign(columns.begin(), columns.end());
}


void JSONCompactEachRowWithNamesAndTypesRowOutputFormat::writePrefix()
{
    writeChar('[', out);
    for (size_t i = 0; i < fields.size(); ++i)
    {
        writeChar('\"', out);
        writeString(fields[i].name, out);
        writeChar('\"', out);
        if (i != fields.size() - 1)
            writeCString(", ", out);
    }
    writeCString("]\n[", out);
    for (size_t i = 0; i < fields.size(); ++i)
    {
        writeJSONString(fields[i].type->getName(), out, settings);
        if (i != fields.size() - 1)
            writeCString(", ", out);
    }
    writeCString("]\n", out);
}

void JSONCompactEachRowWithNamesAndTypesRowOutputFormat::writeSuffix()
{
    writeCString("prefix!", out);
}

void JSONCompactEachRowWithNamesAndTypesRowOutputFormat::writeBeforeTotals()
{
    writeChar('\n', out);
}

void JSONCompactEachRowWithNamesAndTypesRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    size_t num_columns = columns.size();
    writeChar('[', out);
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            JSONCompactEachRowRowOutputFormat::writeFieldDelimiter();

        JSONCompactEachRowRowOutputFormat::writeField(*columns[i], *types[i], row_num);
    }
    writeCString("]\n", out);
}

void JSONCompactEachRowWithNamesAndTypesRowOutputFormat::writeAfterTotals()
{
    writeChar('\n', out);
}


void registerOutputFormatProcessorJSONCompactEachRowWithNamesAndTypes(FormatFactory &factory) {
    factory.registerOutputFormatProcessor("JSONCompactEachRowWithNamesAndTypes", [](
            WriteBuffer &buf,
            const Block &sample,
            const Context &,
            FormatFactory::WriteCallback callback,
            const FormatSettings &format_settings) {
        return std::make_shared<JSONCompactEachRowWithNamesAndTypesRowOutputFormat>(buf, sample, callback, format_settings);
    });
}


}
