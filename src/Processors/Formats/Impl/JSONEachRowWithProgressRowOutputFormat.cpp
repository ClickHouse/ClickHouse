#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/JSONEachRowWithProgressRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>


namespace DB
{

void JSONEachRowWithProgressRowOutputFormat::writePrefix()
{
    writeCString("{\"meta\":[", *ostr);
    bool first = true;
    for (const auto & elem : getInputs().front().getHeader())
    {
        if (!first)
            writeChar(',', *ostr);
        first = false;
        writeCString("{\"name\":", *ostr);
        writeJSONString(elem.name, *ostr, settings);
        writeCString(",\"type\":", *ostr);
        writeJSONString(elem.type->getName(), *ostr, settings);
        writeChar('}', *ostr);
    }
    writeCString("]}\n", *ostr);
}

void JSONEachRowWithProgressRowOutputFormat::writeSuffix()
{
    /// Do not write exception here like JSONEachRow does. See finalizeImpl.
}

void JSONEachRowWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("{\"row\":{", *ostr);
}

void JSONEachRowWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("}}\n", *ostr);
    field_number = 0;
}

void JSONEachRowWithProgressRowOutputFormat::writeSpecialRow(const char * kind, const Columns & columns, size_t row_num)
{
    writeCString("{\"", *ostr);
    writeCString(kind, *ostr);
    writeCString("\":{", *ostr);

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }

    writeCString("}}\n", *ostr);
    field_number = 0;
}

void JSONEachRowWithProgressRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeSpecialRow("totals", columns, row_num);
}

void JSONEachRowWithProgressRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow("min", columns, row_num);
}

void JSONEachRowWithProgressRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow("max", columns, row_num);
}

void JSONEachRowWithProgressRowOutputFormat::writeProgress(const Progress & value)
{
    if (value.empty())
        return;
    writeCString("{\"progress\":", *ostr);
    value.writeJSON(*ostr, Progress::DisplayMode::Minimal);
    writeCString("}\n", *ostr);
}

void JSONEachRowWithProgressRowOutputFormat::finalizeImpl()
{
    if (statistics.applied_limit)
    {
        writeCString("{\"rows_before_limit_at_least\":", *ostr);
        writeIntText(statistics.rows_before_limit, *ostr);
        writeCString("}\n", *ostr);
    }
    if (statistics.applied_aggregation)
    {
        writeCString("{\"rows_before_aggregation\":", *ostr);
        writeIntText(statistics.rows_before_aggregation, *ostr);
        writeCString("}\n", *ostr);
    }
    if (!exception_message.empty())
    {
        writeCString("{\"exception\":", *ostr);
        writeJSONString(exception_message, *ostr, settings);
        writeCString("}\n", *ostr);
    }
}

void registerOutputFormatJSONEachRowWithProgress(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = false;
        return std::make_shared<JSONEachRowWithProgressRowOutputFormat>(buf, sample, settings);
    });
    factory.setContentType("JSONEachRowWithProgress", "application/json; charset=UTF-8");

    factory.registerOutputFormat("JSONStringsEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = true;
        return std::make_shared<JSONEachRowWithProgressRowOutputFormat>(buf, sample, settings);
    });
    factory.setContentType("JSONStringsEachRowWithProgress", "application/json; charset=UTF-8");
}

}
