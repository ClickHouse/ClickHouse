#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>

#include <Processors/Formats/Impl/JSONCompactEachRowRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
}


JSONCompactEachRowRowInputFormat::JSONCompactEachRowRowInputFormat(ReadBuffer & in_,
        const Block & header_,
        Params params_,
        const FormatSettings & format_settings_,
        bool with_names_,
        bool yield_strings_)
        : IRowInputFormat(header_, in_, std::move(params_)), format_settings(format_settings_), with_names(with_names_), yield_strings(yield_strings_)
{
    const auto & sample = getPort().getHeader();
    size_t num_columns = sample.columns();

    data_types.resize(num_columns);
    column_indexes_by_names.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column_info = sample.getByPosition(i);

        data_types[i] = column_info.type;
        column_indexes_by_names.emplace(column_info.name, i);
    }
}

void JSONCompactEachRowRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    column_indexes_for_input_fields.clear();
    not_seen_columns.clear();
}

void JSONCompactEachRowRowInputFormat::readPrefix()
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(in);

    if (with_names)
    {
        size_t num_columns = getPort().getHeader().columns();
        read_columns.assign(num_columns, false);

        assertChar('[', in);
        do
        {
            skipWhitespaceIfAny(in);
            String column_name;
            readJSONString(column_name, in);
            addInputColumn(column_name);
            skipWhitespaceIfAny(in);
        }
        while (checkChar(',', in));
        assertChar(']', in);
        skipEndOfLine();

        /// Type checking
        assertChar('[', in);
        for (size_t i = 0; i < column_indexes_for_input_fields.size(); ++i)
        {
            skipWhitespaceIfAny(in);
            String data_type;
            readJSONString(data_type, in);

            if (column_indexes_for_input_fields[i] &&
                data_types[*column_indexes_for_input_fields[i]]->getName() != data_type)
            {
                throw Exception(
                        "Type of '" + getPort().getHeader().getByPosition(*column_indexes_for_input_fields[i]).name
                        + "' must be " + data_types[*column_indexes_for_input_fields[i]]->getName() +
                        ", not " + data_type,
                        ErrorCodes::INCORRECT_DATA
                );
            }

            if (i != column_indexes_for_input_fields.size() - 1)
                assertChar(',', in);
            skipWhitespaceIfAny(in);
        }
        assertChar(']', in);
    }
    else
    {
        size_t num_columns = getPort().getHeader().columns();
        read_columns.assign(num_columns, true);
        column_indexes_for_input_fields.resize(num_columns);

        for (size_t i = 0; i < num_columns; ++i)
        {
            column_indexes_for_input_fields[i] = i;
        }
    }

    for (size_t i = 0; i < read_columns.size(); ++i)
    {
        if (!read_columns[i])
        {
            not_seen_columns.emplace_back(i);
        }
    }
}

void JSONCompactEachRowRowInputFormat::addInputColumn(const String & column_name)
{
    names_of_columns.emplace_back(column_name);

    const auto column_it = column_indexes_by_names.find(column_name);
    if (column_it == column_indexes_by_names.end())
    {
        if (format_settings.skip_unknown_fields)
        {
            column_indexes_for_input_fields.push_back(std::nullopt);
            return;
        }

        throw Exception(
                "Unknown field found in JSONCompactEachRow header: '" + column_name + "' " +
                "at position " + std::to_string(column_indexes_for_input_fields.size()) +
                "\nSet the 'input_format_skip_unknown_fields' parameter explicitly to ignore and proceed",
                ErrorCodes::INCORRECT_DATA
        );
    }

    const auto column_index = column_it->second;

    if (read_columns[column_index])
        throw Exception("Duplicate field found while parsing JSONCompactEachRow header: " + column_name, ErrorCodes::INCORRECT_DATA);

    read_columns[column_index] = true;
    column_indexes_for_input_fields.emplace_back(column_index);
}

bool JSONCompactEachRowRowInputFormat::readRow(DB::MutableColumns &columns, DB::RowReadExtension &ext)
{
    skipEndOfLine();

    if (in.eof())
        return false;

    size_t num_columns = columns.size();

    read_columns.assign(num_columns, false);

    assertChar('[', in);
    for (size_t file_column = 0; file_column < column_indexes_for_input_fields.size(); ++file_column)
    {
        const auto & table_column = column_indexes_for_input_fields[file_column];
        if (table_column)
        {
            readField(*table_column, columns);
        }
        else
        {
            skipJSONField(in, StringRef(names_of_columns[file_column]));
        }

        skipWhitespaceIfAny(in);
        if (in.eof())
            throw Exception("Unexpected end of stream while parsing JSONCompactEachRow format", ErrorCodes::CANNOT_READ_ALL_DATA);
        if (file_column + 1 != column_indexes_for_input_fields.size())
        {
            assertChar(',', in);
            skipWhitespaceIfAny(in);
        }
    }
    assertChar(']', in);

    for (const auto & name : not_seen_columns)
        columns[name]->insertDefault();

    ext.read_columns = read_columns;
    return true;
}

void JSONCompactEachRowRowInputFormat::skipEndOfLine()
{
    skipWhitespaceIfAny(in);
    if (!in.eof() && (*in.position() == ',' || *in.position() == ';'))
        ++in.position();

    skipWhitespaceIfAny(in);
}

void JSONCompactEachRowRowInputFormat::readField(size_t index, MutableColumns & columns)
{
    try
    {
        read_columns[index] = true;
        const auto & type = data_types[index];

        if (yield_strings)
        {
            String str;
            readJSONString(str, in);

            ReadBufferFromString buf(str);

            if (format_settings.null_as_default && !type->isNullable())
                read_columns[index] = DataTypeNullable::deserializeWholeText(*columns[index], buf, format_settings, type);
            else
                type->deserializeAsWholeText(*columns[index], buf, format_settings);
        }
        else
        {
            if (format_settings.null_as_default && !type->isNullable())
                read_columns[index] = DataTypeNullable::deserializeTextJSON(*columns[index], in, format_settings, type);
            else
                type->deserializeAsTextJSON(*columns[index], in, format_settings);
        }
    }
    catch (Exception & e)
    {
        e.addMessage("(while reading the value of key " +  getPort().getHeader().getByPosition(index).name + ")");
        throw;
    }
}

void JSONCompactEachRowRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(in);
}

void registerInputFormatProcessorJSONCompactEachRow(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("JSONCompactEachRow", [](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
    {
        return std::make_shared<JSONCompactEachRowRowInputFormat>(buf, sample, std::move(params), settings, false, false);
    });

    factory.registerInputFormatProcessor("JSONCompactEachRowWithNamesAndTypes", [](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
    {
        return std::make_shared<JSONCompactEachRowRowInputFormat>(buf, sample, std::move(params), settings, true, false);
    });

    factory.registerInputFormatProcessor("JSONCompactStringsEachRow", [](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
    {
        return std::make_shared<JSONCompactEachRowRowInputFormat>(buf, sample, std::move(params), settings, false, true);
    });

    factory.registerInputFormatProcessor("JSONCompactStringsEachRowWithNamesAndTypes", [](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
    {
        return std::make_shared<JSONCompactEachRowRowInputFormat>(buf, sample, std::move(params), settings, true, true);
    });
}

}
