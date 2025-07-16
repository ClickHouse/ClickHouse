#include <Processors/Formats/Impl/LineAsStringRowInputFormat.h>
#include <base/find_symbols.h>
#include <IO/ReadHelpers.h>
#include <Columns/ColumnString.h>
#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

LineAsStringRowInputFormat::LineAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_) :
    IRowInputFormat(header_, in_, std::move(params_))
{
    if (header_.columns() != 1
        || !typeid_cast<const ColumnString *>(header_.getByPosition(0).column.get()))
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "This input format is only suitable for tables with a single column of type String.");
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

size_t LineAsStringRowInputFormat::countRows(size_t max_block_size)
{
    size_t num_rows = 0;
    while (!in->eof() && num_rows < max_block_size)
    {
        skipToNextLineOrEOF(*in);
        ++num_rows;
    }

    return num_rows;
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


static std::pair<bool, size_t> segmentationEngine(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
{
    char * pos = in.position();
    bool need_more_data = true;
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        pos = find_first_symbols<'\n'>(pos, in.buffer().end());
        if (pos > in.buffer().end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
        if (pos == in.buffer().end())
            continue;

        ++number_of_rows;
        if ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_bytes) || (number_of_rows == max_rows))
            need_more_data = false;

        if (*pos == '\n')
            ++pos;
    }

    saveUpToPosition(in, memory, pos);

    return {loadAtPosition(in, memory, pos), number_of_rows};
}

void registerFileSegmentationEngineLineAsString(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("LineAsString", &segmentationEngine);
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
