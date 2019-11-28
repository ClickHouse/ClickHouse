#include <IO/ReadHelpers.h>

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
    extern const int LOGICAL_ERROR;
}


JSONCompactEachRowRowInputFormat::JSONCompactEachRowRowInputFormat(
        ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
        : IRowInputFormat(header_, in_, std::move(params_)), format_settings(format_settings_)
{
    std::cerr << "\n\nEnter!\n\n" << std::endl;
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(in);
}

bool JSONCompactEachRowRowInputFormat::readRow(DB::MutableColumns &columns, DB::RowReadExtension &ext) {
    skipWhitespaceIfAny(in);
    if (!in.eof() && (*in.position() == ',' || *in.position() == ';'))
        ++in.position();

    skipWhitespaceIfAny(in);
    if (in.eof())
        return false;

    size_t num_columns = columns.size();


    read_columns.assign(num_columns, false);

    assertChar('[', in);
    for (size_t index = 0; index < num_columns; ++index)
    {
        readField(index, columns);

        skipWhitespaceIfAny(in);
        if (in.eof())
            throw Exception("Unexpected end of stream while parsing JSONCompactEachRow format", ErrorCodes::CANNOT_READ_ALL_DATA);
        if (index + 1 != num_columns)
        {
            assertChar(',', in);
            skipWhitespaceIfAny(in);
        }
    }
    assertChar(']', in);

    ext.read_columns = read_columns;
    return true;
}

void JSONCompactEachRowRowInputFormat::readField(size_t index, MutableColumns & columns)
{
    try
    {
        read_columns[index] = true;
        const auto & type = getPort().getHeader().getByPosition(index).type;
        if (format_settings.null_as_default && !type->isNullable())
            read_columns[index] = DataTypeNullable::deserializeTextJSON(*columns[index], in, format_settings, type);
        else
            type->deserializeAsTextJSON(*columns[index], in, format_settings);
    }
    catch (Exception & e)
    {
        e.addMessage("(while read the value of key " +  getPort().getHeader().getByPosition(index).name + ")");
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
            const Context &,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
    {
        return std::make_shared<JSONCompactEachRowRowInputFormat>(buf, sample, std::move(params), settings);
    });
}

}
