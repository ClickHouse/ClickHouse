#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ColumnsSubstreams::addColumn(const String & column)
{
    if (!columns_substreams.empty() && columns_substreams.back().second.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add new column {} to ColumnsSubstreams: previous column {} has empty substreams", column, columns_substreams.back().first);

    columns_substreams.emplace_back(column, std::vector<String>());
    column_to_first_substream_position[column] = total_substreams;
}

void ColumnsSubstreams::addSubstreamToLastColumn(const String & substream)
{
    if (columns_substreams.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add new substream {} to ColumnsSubstreams: there are no columns", substream);

    columns_substreams.back().second.emplace_back(substream);
    column_to_substream_positions[columns_substreams.back().first][substream] = total_substreams;
    ++total_substreams;
}

size_t ColumnsSubstreams::getSubstreamPosition(const String & column, const String & substream) const
{
    return column_to_substream_positions.at(column).at(substream);
}

size_t ColumnsSubstreams::getSubstreamPosition(size_t column_position, const String & substream) const
{
    return column_to_substream_positions.at(columns_substreams[column_position].first).at(substream);
}

size_t ColumnsSubstreams::getFirstSubstreamPosition(const String & column) const
{
    return column_to_first_substream_position.at(column);
}

size_t ColumnsSubstreams::getFirstSubstreamPosition(size_t column_position) const
{
    return column_to_first_substream_position.at(columns_substreams[column_position].first);
}

void ColumnsSubstreams::writeText(WriteBuffer & buf) const
{
    writeString("columns substreams version: 1\n", buf);
    DB::writeText(columns_substreams.size(), buf);
    writeString(" columns:\n", buf);
    for (const auto & [column, substrams] : columns_substreams)
    {
        DB::writeText(substrams.size(), buf);
        writeString(" substreams for column ", buf);
        writeBackQuotedString(column, buf);
        writeCString(":\n", buf);
        for (const auto & substream : substrams)
        {
            writeChar('\t', buf);
            writeString(substream, buf);
            writeChar('\n', buf);
        }
    }
}

void ColumnsSubstreams::readText(ReadBuffer & buf)
{
    columns_substreams.clear();
    column_to_substream_positions.clear();
    column_to_first_substream_position.clear();
    total_substreams = 0;

    assertString("columns substreams version: 1\n", buf);
    size_t num_columns;
    DB::readText(num_columns, buf);
    assertString(" columns:\n", buf);
    columns_substreams.reserve(num_columns);
    column_to_substream_positions.reserve(num_columns);
    column_to_first_substream_position.reserve(num_columns);
    for (size_t i = 0; i != num_columns; ++i)
    {
        size_t num_substreams;
        DB::readText(num_substreams, buf);
        assertString(" substreams for column ", buf);
        String column;
        readBackQuotedStringWithSQLStyle(column, buf);
        assertString(":\n", buf);

        column_to_first_substream_position[column] = total_substreams;

        std::vector<String> substreams(num_substreams);
        for (size_t j = 0; j != num_substreams; ++j)
        {
            assertChar('\t', buf);
            readString(substreams[j], buf);
            assertChar('\n', buf);
            column_to_substream_positions[column][substreams[j]] = total_substreams++;
        }

        columns_substreams.emplace_back(std::move(column), std::move(substreams));
    }


}

}
