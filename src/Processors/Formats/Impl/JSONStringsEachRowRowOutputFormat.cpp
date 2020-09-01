#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONStringsEachRowRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{


JSONStringsEachRowRowOutputFormat::JSONStringsEachRowRowOutputFormat(WriteBuffer & out_,
        const Block & header_,
        FormatFactory::WriteCallback callback,
        const FormatSettings & settings_,
        bool with_names_)
        : IRowOutputFormat(header_, out_, callback), settings(settings_), with_names(with_names_)
{
            const auto & sample = getPort(PortKind::Main).getHeader();
            NamesAndTypesList columns(sample.getNamesAndTypesList());
            fields.assign(columns.begin(), columns.end());
}


void JSONStringsEachRowRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    WriteBufferFromOwnString buf;

    type.serializeAsText(column, row_num, buf, settings);
    writeJSONString(buf.str(), out, settings);
}


void JSONStringsEachRowRowOutputFormat::writeFieldDelimiter()
{
    writeCString(", ", out);
}


void JSONStringsEachRowRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('[', out);
}


void JSONStringsEachRowRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("]\n", out);
}

void JSONStringsEachRowRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeChar('\n', out);
    size_t num_columns = columns.size();
    writeChar('[', out);
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            JSONStringsEachRowRowOutputFormat::writeFieldDelimiter();

        JSONStringsEachRowRowOutputFormat::writeField(*columns[i], *types[i], row_num);
    }
    writeCString("]\n", out);
}

void JSONStringsEachRowRowOutputFormat::writePrefix()
{
    if (with_names)
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
}

void JSONStringsEachRowRowOutputFormat::consumeTotals(DB::Chunk chunk)
{
    if (with_names)
        IRowOutputFormat::consumeTotals(std::move(chunk));
}

void registerOutputFormatProcessorJSONStringsEachRow(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("JSONStringsEachRow", [](
            WriteBuffer & buf,
            const Block & sample,
            FormatFactory::WriteCallback callback,
            const FormatSettings & format_settings)
    {
        return std::make_shared<JSONStringsEachRowRowOutputFormat>(buf, sample, callback, format_settings, false);
    });

    factory.registerOutputFormatProcessor("JSONStringsEachRowWithNamesAndTypes", [](
            WriteBuffer &buf,
            const Block &sample,
            FormatFactory::WriteCallback callback,
            const FormatSettings &format_settings)
    {
        return std::make_shared<JSONStringsEachRowRowOutputFormat>(buf, sample, callback, format_settings, true);
    });
}


}
