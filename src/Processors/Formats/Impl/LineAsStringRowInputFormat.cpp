#include <Processors/Formats/Impl/LineAsStringRowInputFormat.h>
#include <Formats/JSONEachRowUtils.h>
#include <base/find_symbols.h>
#include <IO/ReadHelpers.h>
#include <Columns/ColumnString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

LineAsStringRowInputFormat::LineAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_) :
    IRowInputFormat(header_, in_, std::move(params_))
{
    if (header_.columns() != 1
        || !typeid_cast<const ColumnString *>(header_.getByPosition(0).column.get()))
    {
        throw Exception("This input format is only suitable for tables with a single column of type String.", ErrorCodes::INCORRECT_QUERY);
    }
}

void LineAsStringRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
}

void LineAsStringRowInputFormat::readLineObject(IColumn & column)
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    auto & chars = column_string.getChars();
    auto & offsets = column_string.getOffsets();

    readStringUntilNewlineInto(chars, *in);
    chars.push_back(0);
    offsets.push_back(chars.size());

    if (!in->eof())
        in->ignore(); /// Skip '\n'
}

bool LineAsStringRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in->eof())
        return false;

    readLineObject(*columns[0]);
    return true;
}

void registerInputFormatLineAsString(FormatFactory & factory)
{
    factory.registerInputFormat("LineAsString", [](
        ReadBuffer & buf,
        const Block & sample,
        const RowInputFormatParams & params,
        const FormatSettings &)
    {
        return std::make_shared<LineAsStringRowInputFormat>(sample, buf, params);
    });
}

void registerLineAsStringSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("LineAsString", [](
        const FormatSettings &)
    {
        return std::make_shared<LinaAsStringSchemaReader>();
    });
}

}
