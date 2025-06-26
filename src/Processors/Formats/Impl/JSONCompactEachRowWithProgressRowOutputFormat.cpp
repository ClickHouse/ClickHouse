#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/JSONCompactEachRowWithProgressRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>


namespace DB
{

void JSONCompactEachRowWithProgressRowOutputFormat::writePrefix()
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

void JSONCompactEachRowWithProgressRowOutputFormat::writeSuffix()
{
    /// Do not write exception here like JSONEachRow does. See finalizeImpl.
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("{\"row\":[", *ostr);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("]}\n", *ostr);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeSpecialRow(const char * kind, const Columns & columns, size_t row_num)
{
    writeCString("{\"", *ostr);
    writeCString(kind, *ostr);
    writeCString("\":[", *ostr);

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }

    writeCString("]}\n", *ostr);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    writeSpecialRow("totals", columns, row_num);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow("min", columns, row_num);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeSpecialRow("max", columns, row_num);
}

void JSONCompactEachRowWithProgressRowOutputFormat::writeProgress(const Progress & value)
{
    if (value.empty())
        return;
    writeCString("{\"progress\":", *ostr);
    value.writeJSON(*ostr);
    writeCString("}\n", *ostr);
}

void JSONCompactEachRowWithProgressRowOutputFormat::finalizeImpl()
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

void registerOutputFormatJSONCompactEachRowWithProgress(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONCompactEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = false;
        return std::make_shared<JSONCompactEachRowWithProgressRowOutputFormat>(buf, sample, settings, false, false);
    });

    factory.setContentType("JSONCompactEachRowWithProgress", "application/json; charset=UTF-8");

    factory.registerOutputFormat("JSONCompactStringsEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = true;
        return std::make_shared<JSONCompactEachRowWithProgressRowOutputFormat>(buf, sample, settings, false, false);
    });

    factory.setContentType("JSONCompactStringsEachRowWithProgress", "application/json; charset=UTF-8");
}

}
