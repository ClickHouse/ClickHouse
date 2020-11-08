#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONCompactEachRowRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{


JSONCompactEachRowRowOutputFormat::JSONCompactEachRowRowOutputFormat(WriteBuffer & out_,
        const Block & header_,
        const RowOutputFormatParams & params_,
        const FormatSettings & settings_,
        bool with_names_,
        bool yield_strings_)
        : IRowOutputFormat(header_, out_, params_), settings(settings_), with_names(with_names_), yield_strings(yield_strings_)
{
            const auto & sample = getPort(PortKind::Main).getHeader();
            NamesAndTypesList columns(sample.getNamesAndTypesList());
            fields.assign(columns.begin(), columns.end());
}


void JSONCompactEachRowRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    if (yield_strings)
    {
        WriteBufferFromOwnString buf;

        type.serializeAsText(column, row_num, buf, settings);
        writeJSONString(buf.str(), out, settings);
    }
    else
        type.serializeAsTextJSON(column, row_num, out, settings);
}


void JSONCompactEachRowRowOutputFormat::writeFieldDelimiter()
{
    writeCString(", ", out);
}


void JSONCompactEachRowRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('[', out);
}


void JSONCompactEachRowRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("]\n", out);
}

void JSONCompactEachRowRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeChar('\n', out);
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

void JSONCompactEachRowRowOutputFormat::writePrefix()
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

void JSONCompactEachRowRowOutputFormat::consumeTotals(DB::Chunk chunk)
{
    if (with_names)
        IRowOutputFormat::consumeTotals(std::move(chunk));
}

void registerOutputFormatProcessorJSONCompactEachRow(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("JSONCompactEachRow", [](
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams & params,
            const FormatSettings & format_settings)
    {
        return std::make_shared<JSONCompactEachRowRowOutputFormat>(buf, sample, params, format_settings, false, false);
    });

    factory.registerOutputFormatProcessor("JSONCompactEachRowWithNamesAndTypes", [](
            WriteBuffer &buf,
            const Block &sample,
            const RowOutputFormatParams & params,
            const FormatSettings &format_settings)
    {
        return std::make_shared<JSONCompactEachRowRowOutputFormat>(buf, sample, params, format_settings, true, false);
    });

    factory.registerOutputFormatProcessor("JSONCompactStringsEachRow", [](
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams & params,
            const FormatSettings & format_settings)
    {
        return std::make_shared<JSONCompactEachRowRowOutputFormat>(buf, sample, params, format_settings, false, true);
    });

    factory.registerOutputFormatProcessor("JSONCompactStringsEachRowWithNamesAndTypes", [](
            WriteBuffer &buf,
            const Block &sample,
            const RowOutputFormatParams & params,
            const FormatSettings &format_settings)
    {
        return std::make_shared<JSONCompactEachRowRowOutputFormat>(buf, sample, params, format_settings, true, true);
    });
}


}
