#include <Processors/Formats/Impl/LineAsStringRowInputFormat.h>
#include <Formats/JSONEachRowUtils.h>
#include <common/find_symbols.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

LineAsStringRowInputFormat::LineAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_) :
    IRowInputFormat(header_, in_, std::move(params_))
{
    if (header_.columns() > 1 || header_.getDataTypes()[0]->getTypeId() != TypeIndex::String)
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
    DB::Memory<> object;

    char * pos = in.position();
    bool need_more_data = true;

    while (loadAtPosition(in, object, pos) && need_more_data)
    {
        pos = find_first_symbols<'\n'>(pos, in.buffer().end());
        if (pos == in.buffer().end())
            continue;

        if (*pos == '\n')
            need_more_data = false;

        ++pos;
    }

    saveUpToPosition(in, object, pos);
    loadAtPosition(in, object, pos);

    /// Last character is always \n.
    column.insertData(object.data(), object.size() - 1);
}

bool LineAsStringRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in.eof())
        return false;

    readLineObject(*columns[0]);

    return true;
}

void registerInputFormatProcessorLineAsString(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("LineAsString", [](
            ReadBuffer & buf,
            const Block & sample,
            const RowInputFormatParams & params,
            const FormatSettings &)
    {
        return std::make_shared<LineAsStringRowInputFormat>(sample, buf, params);
    });
}
}
