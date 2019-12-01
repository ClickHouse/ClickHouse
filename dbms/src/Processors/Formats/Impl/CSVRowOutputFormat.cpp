#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#include <IO/WriteHelpers.h>


namespace DB
{


CSVRowOutputFormat::CSVRowOutputFormat(WriteBuffer & out_, const Block & header_, bool with_names_, bool text_file_, FormatFactory::WriteCallback callback, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, callback), with_names(with_names_), text_file(text_file_), format_settings(format_settings_)
{
    if (text_file)
    {
        format_settings.csv.delimiter = '\001';
    }
    auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();
    data_types.resize(columns);
    for (size_t i = 0; i < columns; ++i)
        data_types[i] = sample.safeGetByPosition(i).type;
}


void CSVRowOutputFormat::writePrefix()
{
    auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();

    if (with_names)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeCSVString(sample.safeGetByPosition(i).name, out);

            char delimiter = format_settings.csv.delimiter;
            if (i + 1 == columns)
                delimiter = '\n';

            writeChar(delimiter, out);
        }
    }
}


void CSVRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeAsTextCSV(column, row_num, out, format_settings);
}


void CSVRowOutputFormat::writeFieldDelimiter()
{
    writeChar(format_settings.csv.delimiter, out);
}


void CSVRowOutputFormat::writeRowEndDelimiter()
{
    writeChar('\n', out);
}

void CSVRowOutputFormat::writeBeforeTotals()
{
    writeChar('\n', out);
}

void CSVRowOutputFormat::writeBeforeExtremes()
{
    writeChar('\n', out);
}



void registerOutputFormatProcessorCSV(FormatFactory & factory)
{
    for (bool with_names : {false, true})
    {
        for (bool text_file : {false, true})
        {
            std::string format = text_file ? "HiveTextFile" : "CSV";
            factory.registerOutputFormatProcessor(with_names ? format + "WithNames" : format, [=](
                WriteBuffer & buf,
                const Block & sample,
                const Context &,
                FormatFactory::WriteCallback callback,
                const FormatSettings & format_settings)
            {
                return std::make_shared<CSVRowOutputFormat>(buf, sample, with_names, text_file, callback, format_settings);
            });
        }
    }
}

}
