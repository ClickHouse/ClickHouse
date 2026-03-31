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

    /// In Wide part we can write to the same stream several times, keep only the first occurrence.
    if (column_position_to_substream_positions[columns_substreams.size() - 1].contains(substream))
        return;

    columns_substreams.back().second.emplace_back(substream);
    column_position_to_substream_positions[columns_substreams.size() - 1][substream] = total_substreams;
    ++total_substreams;
}

void ColumnsSubstreams::addSubstreamsToLastColumn(const std::vector<String> & substreams)
{
    for (const auto & substream : substreams)
        addSubstreamToLastColumn(substream);
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

std::optional<size_t> ColumnsSubstreams::tryGetSubstreamPosition(size_t column_position, const String & substream) const
{
    if (column_position >= columns_substreams.size())
        return std::nullopt;

    auto it = column_position_to_substream_positions[column_position].find(substream);
    if (it == column_position_to_substream_positions[column_position].end())
        return std::nullopt;

    return it->second;
}

size_t ColumnsSubstreams::getSubstreamPosition(
    size_t column_position,
    const NameAndTypePair & name_and_type,
    const ISerialization::SubstreamPath & substream_path,
    const MergeTreeSettingsPtr & storage_settings) const
{
    ISerialization::StreamFileNameSettings stream_file_name_settings(*storage_settings);
    auto substream = ISerialization::getFileNameForStream(name_and_type, substream_path, stream_file_name_settings);
    if (auto position = tryGetSubstreamPosition(column_position, substream))
        return *position;

    /// To be able to read old parts after changes in stream file name settings, try to change settings and try to find it again.
    if (ISerialization::tryToChangeStreamFileNameSettingsForNotFoundStream(substream_path, stream_file_name_settings))
    {
        substream = ISerialization::getFileNameForStream(name_and_type, substream_path, stream_file_name_settings);
        if (auto position = tryGetSubstreamPosition(column_position, substream))
            return *position;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get position for substream {}: column {} with position {} doesn't have such substream", substream, name_and_type.name, column_position);
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

const std::vector<String> & ColumnsSubstreams::getColumnSubstreams(size_t column_position) const
{
    if (column_position >= columns_substreams.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get substreams: column position {} is invalid, there are only {} columns", column_position, columns_substreams.size());

    return columns_substreams[column_position].second;
}

ColumnsSubstreams ColumnsSubstreams::merge(const ColumnsSubstreams & left, const ColumnsSubstreams & right, const std::vector<String> & columns_order)
{
    std::unordered_map<std::string_view, size_t> left_column_to_position;
    left_column_to_position.reserve(left.columns_substreams.size());
    for (size_t i = 0; i != left.columns_substreams.size(); ++i)
        left_column_to_position[left.columns_substreams[i].first] = i;

    std::unordered_map<std::string_view, size_t> right_column_to_position;
    right_column_to_position.reserve(right.columns_substreams.size());
    for (size_t i = 0; i != right.columns_substreams.size(); ++i)
        right_column_to_position[right.columns_substreams[i].first] = i;

    ColumnsSubstreams merged;
    for (const auto & column : columns_order)
    {
        if (auto left_it = left_column_to_position.find(column); left_it != left_column_to_position.end())
        {
            merged.addColumn(column);
            merged.addSubstreamsToLastColumn(left.getColumnSubstreams(left_it->second));
        }
        else if (auto right_it = right_column_to_position.find(column); right_it != right_column_to_position.end())
        {
            merged.addColumn(column);
            merged.addSubstreamsToLastColumn(right.getColumnSubstreams(right_it->second));
        }
    }

    return merged;
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

String ColumnsSubstreams::toString() const
{
    WriteBufferFromOwnString buf;
    writeText(buf);
    return buf.str();
}

void ColumnsSubstreams::validateColumns(const std::vector<String> & columns) const
{
    if (columns.size() != columns_substreams.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid columns substreams: expected {} columns, got {}", columns.size(), columns_substreams.size());

    for (size_t i = 0; i != columns_substreams.size(); ++i)
    {
        if (columns_substreams[i].first != columns[i])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column at position {} in columns substreams: expected {}, got {}", i, columns[i], columns_substreams[i].first);
    }
}

}
