#include <Processors/Formats/Impl/CSVRowOutputFormat.h>

#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FlattenTupleForCSVHeader.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <IO/WriteHelpers.h>
#include <Processors/Port.h>

namespace DB
{


CSVRowOutputFormat::CSVRowOutputFormat(WriteBuffer & out_, SharedHeader header_, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), with_names(with_names_), with_types(with_types_), format_settings(format_settings_)
{
    const auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();
    data_types.resize(columns);
    for (size_t i = 0; i < columns; ++i)
        data_types[i] = sample.safeGetByPosition(i).type;
}

void CSVRowOutputFormat::writeLine(const std::vector<String> & values)
{
    for (size_t i = 0; i < values.size(); ++i)
    {
        writeCSVString(values[i], out);
        if (i + 1 != values.size())
            writeFieldDelimiter();
    }
    writeRowEndDelimiter();
}

void CSVRowOutputFormat::writePrefix()
{
    const auto & sample = getPort(PortKind::Main).getHeader();

    /// When tuple values are serialized into separate columns, flatten the header the same way so
    /// that the number of header fields matches the number of data fields (issue #107342).
    const bool flatten = format_settings.csv.serialize_tuple_into_separate_columns
        && format_settings.csv.header_serialize_tuple_into_separate_columns;

    Names names;
    Names type_names;
    getCSVHeaderNamesAndTypes(sample, flatten, names, type_names);

    if (with_names)
        writeLine(names);

    if (with_types)
        writeLine(type_names);
}


void CSVRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serialization.serializeTextCSV(column, row_num, out, format_settings);
}


void CSVRowOutputFormat::writeFieldDelimiter()
{
    writeChar(format_settings.csv.delimiter, out);
}


void CSVRowOutputFormat::writeRowEndDelimiter()
{
    if (format_settings.csv.crlf_end_of_line)
        writeChar('\r', out);
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


void registerOutputFormatCSV(FormatFactory & factory);
void registerOutputFormatCSV(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerOutputFormat(format_name, [with_names, with_types](
                   WriteBuffer & buf,
                   const Block & sample,
                   const FormatSettings & format_settings,
                   FormatFilterInfoPtr /*format_filter_info*/)
        {
            return std::make_shared<CSVRowOutputFormat>(buf, std::make_shared<const Block>(sample), with_names, with_types, format_settings);
        });
        factory.markOutputFormatSupportsParallelFormatting(format_name);
        /// https://www.iana.org/assignments/media-types/text/csv
        factory.setContentType(format_name, String("text/csv; charset=UTF-8; header=") + (with_names ? "present" : "absent"));

        /// The `*WithNames*` variants write the column names (and data type names) into the header
        /// through `writeCSVString`, which quotes special characters but does not validate UTF-8, so a
        /// name that is not valid UTF-8 (a quoted identifier or an `Enum` element with arbitrary bytes)
        /// makes the output non-textual. When a Tuple column is flattened into separate columns the
        /// header carries the dotted leaf names (see `getCSVHeaderNamesAndTypes`), so a non-UTF-8 Tuple
        /// element name would slip past a top-level-only check; validate the actual flattened header
        /// under the current settings. The field delimiter is written verbatim between the fields (a
        /// single byte >= 0x80 is never valid UTF-8 on its own), and the row values are written through
        /// the `CSV` serializations, which write the `CSV` `NULL` representation and the `Bool`
        /// representations verbatim (see `settingsLiteralsMayProduceRawBytes`). All of this is knowable
        /// from the header and the settings, so the text framings reject or base64-encode the output
        /// accordingly.
        factory.registerOutputFormatMayProduceRawBytesChecker(
            format_name,
            [with_names, with_types](const FormatSettings & settings, const Block & header)
            {
                const bool flatten = settings.csv.serialize_tuple_into_separate_columns
                    && settings.csv.header_serialize_tuple_into_separate_columns;
                return ((with_names || with_types) && csvHeaderNamesMayProduceRawBytes(header, flatten, with_names, with_types))
                    || static_cast<unsigned char>(settings.csv.delimiter) >= 0x80
                    || settingsLiteralsMayProduceRawBytes(settings, FormatSettings::EscapingRule::CSV);
            });
    };

    registerWithNamesAndTypes("CSV", register_func);
}

}
