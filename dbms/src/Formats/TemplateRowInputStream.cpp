#include <Formats/TemplateRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_TEMPLATE_FORMAT;
}


TemplateRowInputStream::TemplateRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSettings & settings_, bool ignore_spaces_)
    : istr(istr_), header(header_), types(header.getDataTypes()), settings(settings_), ignore_spaces(ignore_spaces_)
{
    static const String default_format("${data}");
    const String & format_str = settings.template_settings.format.empty() ? default_format : settings.template_settings.format;
    format = ParsedTemplateFormat(format_str, [&](const String & partName) {
        if (partName == "data")
            return 0;
        throw Exception("invalid template format: unknown input part " + partName, ErrorCodes::INVALID_TEMPLATE_FORMAT);
    });

    if (format.formats.size() != 1 || format.formats[0] != ColumnFormat::Default)
        throw Exception("invalid template format: format_schema must be \"prefix ${data} suffix\"", ErrorCodes::INVALID_TEMPLATE_FORMAT);


    row_format = ParsedTemplateFormat(settings.template_settings.row_format, [&](const String & colName) {
        return header.getPositionByName(colName);
    });

    std::vector<UInt8> column_in_format(header.columns(), false);
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        size_t col_idx = row_format.format_idx_to_column_idx[i];
        if (column_in_format[col_idx])
            throw Exception("invalid template format: duplicate column " + header.getColumnsWithTypeAndName()[col_idx].name,
                    ErrorCodes::INVALID_TEMPLATE_FORMAT);
        column_in_format[col_idx] = true;

        if (row_format.formats[i] == ColumnFormat::Xml || row_format.formats[i] == ColumnFormat::Raw)
            throw Exception("invalid template format: XML and Raw deserialization is not supported", ErrorCodes::INVALID_TEMPLATE_FORMAT);
    }
}

void TemplateRowInputStream::readPrefix()
{
    skipSpaces();
    assertString(format.delimiters.front(), istr);
}

bool TemplateRowInputStream::read(MutableColumns & columns, RowReadExtension & extra)
{
    skipSpaces();

    // TODO check for suffix, not for EOF
    if (istr.eof())
        return false;

    if (row_count)
    {
        assertString(settings.template_settings.row_between_delimiter, istr);
    }

    extra.read_columns.assign(columns.size(), false);

    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        skipSpaces();
        assertString(row_format.delimiters[i], istr);
        size_t col_idx = row_format.format_idx_to_column_idx[i];
        skipSpaces();
        deserializeField(*types[col_idx], *columns[col_idx], row_format.formats[i]);
        extra.read_columns[col_idx] = true;
    }

    skipSpaces();
    assertString(row_format.delimiters.back(), istr);

    for (size_t i = 0; i < columns.size(); ++i)
        if (!extra.read_columns[i])
            header.getByPosition(i).type->insertDefaultInto(*columns[i]);

    ++row_count;
    return true;
}

void TemplateRowInputStream::deserializeField(const IDataType & type, IColumn & column, ColumnFormat col_format)
{
    switch (col_format)
    {
        case ColumnFormat::Default:
        case ColumnFormat::Escaped:
            type.deserializeAsTextEscaped(column, istr, settings);
            break;
        case ColumnFormat::Quoted:
            type.deserializeAsTextQuoted(column, istr, settings);
            break;
        case ColumnFormat::Json:
            type.deserializeAsTextJSON(column, istr, settings);
            break;
        default:
            break;
    }
}


void registerInputFormatTemplate(FormatFactory & factory)
{
    for (bool ignore_spaces : {false, true})
    {
        factory.registerInputFormat(ignore_spaces ? "TemplateIgnoreSpaces" : "Template", [=](
                ReadBuffer & buf,
                const Block & sample,
                const Context &,
                UInt64 max_block_size,
                const FormatSettings & settings) {
            return std::make_shared<BlockInputStreamFromRowInputStream>(
                    std::make_shared<TemplateRowInputStream>(buf, sample, settings, ignore_spaces),
                    sample, max_block_size, settings);
        });
    }
}

}
