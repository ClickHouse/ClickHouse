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
    column_position_to_substream_positions.emplace_back();
}

void ColumnsSubstreams::addSubstreamToLastColumn(const String & substream)
{
    if (columns_substreams.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add new substream {} to ColumnsSubstreams: there are no columns", substream);

    columns_substreams.back().second.emplace_back(substream);
    column_position_to_substream_positions[columns_substreams.size() - 1][substream] = total_substreams;
    ++total_substreams;
}

size_t ColumnsSubstreams::getSubstreamPosition(size_t column_position, const String & substream) const
{
    if (column_position >= columns_substreams.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get position for substream {}: column position {} is invalid, there are only {} columns", substream, column_position, columns_substreams.size());

    auto it = column_position_to_substream_positions[column_position].find(substream);
    if (it == column_position_to_substream_positions[column_position].end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get position for substream {}: column {} with position {} doesn't have such substream", substream, columns_substreams[column_position].first, column_position);

    return it->second;
}

std::optional<size_t> ColumnsSubstreams::tryGetSubstreamPosition(const String & substream) const
{
    for (const auto & substream_to_position : column_position_to_substream_positions)
    {
        auto it = substream_to_position.find(substream);
        if (it != substream_to_position.end())
            return it->second;
    }

    return std::nullopt;
}

size_t ColumnsSubstreams::getFirstSubstreamPosition(size_t column_position) const
{
    if (column_position >= columns_substreams.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get first substream position: column position {} is invalid, there are only {} columns", column_position, columns_substreams.size());

    if (columns_substreams[column_position].second.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get first substream position: column {} with position {} doesn't have substreams", columns_substreams[column_position].first, column_position);

    return getSubstreamPosition(column_position, columns_substreams[column_position].second.front());
}

size_t ColumnsSubstreams::getLastSubstreamPosition(size_t column_position) const
{
    if (column_position >= columns_substreams.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get last substream position: column position {} is invalid, there are only {} columns", column_position, columns_substreams.size());

    if (columns_substreams[column_position].second.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get last substream position: column {} with position {} doesn't have substreams", columns_substreams[column_position].first, column_position);

    return getSubstreamPosition(column_position, columns_substreams[column_position].second.back());
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
    column_position_to_substream_positions.clear();
    total_substreams = 0;

    assertString("columns substreams version: 1\n", buf);
    size_t num_columns;
    DB::readText(num_columns, buf);
    assertString(" columns:\n", buf);
    columns_substreams.reserve(num_columns);
    column_position_to_substream_positions.resize(num_columns);
    for (size_t i = 0; i != num_columns; ++i)
    {
        size_t num_substreams;
        DB::readText(num_substreams, buf);
        assertString(" substreams for column ", buf);
        String column;
        readBackQuotedStringWithSQLStyle(column, buf);
        assertString(":\n", buf);

        std::vector<String> substreams(num_substreams);
        for (size_t j = 0; j != num_substreams; ++j)
        {
            assertChar('\t', buf);
            readString(substreams[j], buf);
            assertChar('\n', buf);
            column_position_to_substream_positions[i][substreams[j]] = total_substreams++;
        }

        columns_substreams.emplace_back(std::move(column), std::move(substreams));
    }
}

}
