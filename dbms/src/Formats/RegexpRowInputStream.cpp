#include <Formats/RegexpRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <IO/ReadHelpers.h>
#include <common/find_symbols.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RegexpRowInputStream::RegexpRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSettings & settings_)
    : buf(istr_), header(header_), settings(settings_), regexp(settings.regexp_settings.regexp)
{
    if (header.columns() != regexp.getNumberOfSubpatterns() + 1)
        throw DB::Exception("Number of columns (" + std::to_string(header.columns()) + ") is not equal to number of patterns (" +
        std::to_string(regexp.getNumberOfSubpatterns() + 1) + ")", ErrorCodes::LOGICAL_ERROR);
}

bool RegexpRowInputStream::read(MutableColumns & columns, RowReadExtension & extra)
{
    bool match = false;
    OptimizedRegularExpression::MatchVec matches;
    auto begin = buf.position();
    auto end_of_line = buf.position();
    while(!match)
    {
        if (buf.eof())
            return false;
        end_of_line = loadDataUntilNewLine();
        begin = buf.position();

        if (regexp.match(begin, end_of_line - begin, matches))
        {
            for (const auto & m : matches)
            {
                if (m.offset != std::string::npos)
                {
                    match = true;
                    break;
                }
            }
        }

        if (!match)
        {
            buf.position() = end_of_line;

            if (!buf.eof())
                ++buf.position();
        }
    }

    extra.read_columns.assign(columns.size(), false);

    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (matches[i].offset == std::string::npos)
            continue;
        ReadBufferFromMemory m(begin + matches[i].offset, matches[i].length);
        header.getByPosition(i).type->deserializeAsTextEscaped(*columns[i], m, settings);
        extra.read_columns[i] = true;
    }

    buf.position() = end_of_line;
    if (!buf.eof())
        ++buf.position();

    for (size_t i = 0; i < columns.size(); ++i)
        if (!extra.read_columns[i])
            header.getByPosition(i).type->insertDefaultInto(*columns[i]);

    return true;
}

ReadBuffer::Position RegexpRowInputStream::loadDataUntilNewLine()
{
    ReadBuffer::Position end = nullptr;
    ReadBuffer::Position pos = nullptr;
    do
    {
        end = buf.buffer().end();
        pos = find_first_symbols<'\n'>(buf.position(), end);
    } while (pos == end && buf.peekNext());
    return pos;
}

void registerInputFormatRegexp(FormatFactory & factory)
{
    factory.registerInputFormat("Regexp", [=](
            ReadBuffer & buf,
            const Block & sample,
            const Context &,
            UInt64 max_block_size,
            UInt64 rows_portion_size,
            const FormatSettings & settings)
    {
        return std::make_shared<BlockInputStreamFromRowInputStream>(
                std::make_shared<RegexpRowInputStream>(buf, sample, settings),
                sample, max_block_size, rows_portion_size, settings);
    });

}

}
