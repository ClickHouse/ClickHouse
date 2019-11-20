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
        writeString(fields[i].name, out);
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
