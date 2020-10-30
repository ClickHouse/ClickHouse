#include <Processors/Formats/Impl/JSONRowOutputFormat.h>

#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Formats/FormatFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void JSONRowOutputFormat::addColumn(String name, DataTypePtr type,
    bool & need_validate_utf8, std::string tabs)
{
    if (!type->textCanContainOnlyValidUTF8())
        need_validate_utf8 = true;

    WriteBufferFromOwnString buf;
    writeJSONString(name, buf, settings);

    const auto * as_tuple = typeid_cast<const DataTypeTuple *>(type.get());
    const bool recurse = settings.json.named_tuple_as_object
        && as_tuple && as_tuple->haveExplicitNames();

    fields.emplace_back(FieldInfo{buf.str(), type, recurse, tabs});

    if (recurse)
    {
        const auto & element_types = as_tuple->getElements();
        const auto & names = as_tuple->getElementNames();

        assert(element_types.size() == names.size());
        for (size_t i = 0; i < element_types.size(); i++)
        {
            addColumn(names[i], element_types[i], need_validate_utf8, tabs + "\t");
        }
    }
}

JSONRowOutputFormat::JSONRowOutputFormat(
    WriteBuffer & out_,
    const Block & header,
    const RowOutputFormatParams & params_,
    const FormatSettings & settings_,
    bool yield_strings_)
    : IRowOutputFormat(header, out_, params_), settings(settings_), yield_strings(yield_strings_)
{
    const auto & sample = getPort(PortKind::Main).getHeader();
    NamesAndTypesList columns(sample.getNamesAndTypesList());

    fields.reserve(columns.size());

    const std::string initial_tabs = settings.json.write_metadata ? "\t\t\t" : "\t\t";
    bool need_validate_utf8 = false;
    for (const auto & column : columns)
    {
        addColumn(column.name, column.type, need_validate_utf8, initial_tabs);
    }

//    for (size_t i = 0; i < fields.size(); i++)
//    {
//        fmt::print(stderr, "{}: '{}' '{}' '{}\n",
//            i, fields[i].name, fields[i].type->getName(), fields[i].recurse);
//    }

    if (need_validate_utf8)
    {
        validating_ostr = std::make_unique<WriteBufferValidUTF8>(out);
        ostr = validating_ostr.get();
    }
    else
        ostr = &out;
}


void JSONRowOutputFormat::writePrefix()
{
    if (settings.json.write_metadata)
    {
        writeCString("{\n", *ostr);
        writeCString("\t\"meta\":\n", *ostr);
        writeCString("\t[\n", *ostr);

        for (size_t i = 0; i < fields.size(); ++i)
        {
            writeCString("\t\t{\n", *ostr);

            writeCString("\t\t\t\"name\": ", *ostr);
            writeString(fields[i].name, *ostr);
            writeCString(",\n", *ostr);
            writeCString("\t\t\t\"type\": ", *ostr);
            writeJSONString(fields[i].type->getName(), *ostr, settings);
            writeChar('\n', *ostr);

            writeCString("\t\t}", *ostr);
            if (i + 1 < fields.size())
                writeChar(',', *ostr);
            writeChar('\n', *ostr);
        }

        writeCString("\t],\n", *ostr);
        writeChar('\n', *ostr);
        writeCString("\t\"data\":\n", *ostr);
        writeCString("\t", *ostr);
    }
    writeCString("[\n", *ostr);
}

void JSONRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
//    fmt::print(stderr, "write field column '{}' type '{}'\n",
//        column.getName(), type.getName());

    writeString(fields[field_number].tabs, *ostr);
    writeString(fields[field_number].name, *ostr);
    writeCString(": ", *ostr);

    // Sanity check: the input column type is the same as in header block.
    // If I don't write out the raw pointer explicitly, for some reason clang
    // complains about side effect in dereferencing the pointer:
    // src/Processors/Formats/Impl/JSONRowOutputFormat.cpp:120:35: warning: expression with side effects will be evaluated despite being used as an operand to 'typeid' [-Wpotentially-evaluated-expression]
    [[maybe_unused]] const IDataType * raw_ptr = fields[field_number].type.get();
    assert(typeid(type) == typeid(*raw_ptr));

    if (fields[field_number].recurse)
    {
        const auto & tabs = fields[field_number].tabs;
        ++field_number;
        const auto & tuple_column = assert_cast<const ColumnTuple &>(column);
        const auto & nested_columns = tuple_column.getColumns();
        writeCString("{\n", *ostr);
        for (size_t i = 0; i < nested_columns.size(); i++)
        {
            // field_number is incremented inside, and should match the nested
            // columns.
            writeField(*nested_columns[i], *fields[field_number].type, row_num);
            if (i + 1 < nested_columns.size())
            {
                writeCString(",", *ostr);
            }
            writeCString("\n", *ostr);
        }
        writeString(tabs, *ostr);
        writeCString("}", *ostr);
        return;
    }

    if (yield_strings)
    {
        WriteBufferFromOwnString buf;

        type.serializeAsText(column, row_num, buf, settings);
        writeJSONString(buf.str(), *ostr, settings);
    }
    else
        type.serializeAsTextJSON(column, row_num, *ostr, settings);

    ++field_number;
}

void JSONRowOutputFormat::writeTotalsField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeCString("\t\t", *ostr);
    writeString(fields[field_number].name, *ostr);
    writeCString(": ", *ostr);

    if (yield_strings)
    {
        WriteBufferFromOwnString buf;

        type.serializeAsText(column, row_num, buf, settings);
        writeJSONString(buf.str(), *ostr, settings);
    }
    else
        type.serializeAsTextJSON(column, row_num, *ostr, settings);

    ++field_number;
}

void JSONRowOutputFormat::writeFieldDelimiter()
{
    writeCString(",\n", *ostr);
}


void JSONRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("\t\t{\n", *ostr);
}


void JSONRowOutputFormat::writeRowEndDelimiter()
{
    writeChar('\n', *ostr);
    writeCString("\t\t}\n", *ostr);
    field_number = 0;
    ++row_count;
}


void JSONRowOutputFormat::writeRowBetweenDelimiter()
{
    writeCString(",\n", *ostr);
}


void JSONRowOutputFormat::writeSuffix()
{
    writeChar('\n', *ostr);
    writeCString("\t]", *ostr);
}

void JSONRowOutputFormat::writeBeforeTotals()
{
    if (!settings.json.write_metadata)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot output totals in JSON format without metadata");
    }

    writeCString(",\n", *ostr);
    writeChar('\n', *ostr);
    writeCString("\t\"totals\":\n", *ostr);
    writeCString("\t{\n", *ostr);
}

void JSONRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    size_t num_columns = columns.size();

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeTotalsFieldDelimiter();

        writeTotalsField(*columns[i], *types[i], row_num);
    }
}

void JSONRowOutputFormat::writeAfterTotals()
{
    writeChar('\n', *ostr);
    writeCString("\t}", *ostr);
    field_number = 0;
}

void JSONRowOutputFormat::writeBeforeExtremes()
{
    if (!settings.json.write_metadata)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot output extremes in JSON format without metadata");
    }

    writeCString(",\n", *ostr);
    writeChar('\n', *ostr);
    writeCString("\t\"extremes\":\n", *ostr);
    writeCString("\t{\n", *ostr);
}

void JSONRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    writeCString("\t\t\"", *ostr);
    writeCString(title, *ostr);
    writeCString("\":\n", *ostr);
    writeCString("\t\t{\n", *ostr);

    size_t extremes_columns = columns.size();
    for (size_t i = 0; i < extremes_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *types[i], row_num);
    }

    writeChar('\n', *ostr);
    writeCString("\t\t}", *ostr);
    field_number = 0;
}

void JSONRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeExtremesElement("min", columns, row_num);
}

void JSONRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeExtremesElement("max", columns, row_num);
}

void JSONRowOutputFormat::writeAfterExtremes()
{
    writeChar('\n', *ostr);
    writeCString("\t}", *ostr);
}

void JSONRowOutputFormat::writeLastSuffix()
{
    if (settings.json.write_metadata)
    {
        writeCString(",\n\n", *ostr);
        writeCString("\t\"rows\": ", *ostr);
        writeIntText(row_count, *ostr);

        writeRowsBeforeLimitAtLeast();

        if (settings.write_statistics)
            writeStatistics();

        writeChar('\n', *ostr);
        writeCString("}\n", *ostr);
    }
    ostr->next();
}

void JSONRowOutputFormat::writeRowsBeforeLimitAtLeast()
{
    if (applied_limit)
    {
        writeCString(",\n\n", *ostr);
        writeCString("\t\"rows_before_limit_at_least\": ", *ostr);
        writeIntText(rows_before_limit, *ostr);
    }
}

void JSONRowOutputFormat::writeStatistics()
{
    writeCString(",\n\n", *ostr);
    writeCString("\t\"statistics\":\n", *ostr);
    writeCString("\t{\n", *ostr);

    writeCString("\t\t\"elapsed\": ", *ostr);
    writeText(watch.elapsedSeconds(), *ostr);
    writeCString(",\n", *ostr);
    writeCString("\t\t\"rows_read\": ", *ostr);
    writeText(progress.read_rows.load(), *ostr);
    writeCString(",\n", *ostr);
    writeCString("\t\t\"bytes_read\": ", *ostr);
    writeText(progress.read_bytes.load(), *ostr);
    writeChar('\n', *ostr);

    writeCString("\t}", *ostr);
}

void JSONRowOutputFormat::onProgress(const Progress & value)
{
    progress.incrementPiecewiseAtomically(value);
}


void registerOutputFormatProcessorJSON(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("JSON", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONRowOutputFormat>(buf, sample, params, format_settings, false);
    });

    factory.registerOutputFormatProcessor("JSONStrings", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONRowOutputFormat>(buf, sample, params, format_settings, true);
    });
}

}
