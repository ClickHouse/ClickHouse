#pragma once
#include <Core/Types.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/// Class that stores the list of substreams of columns in order of their serialization/deserialization.
/// For example:
/// Columns:
/// a UInt32, b Tuple(c UInt32, d Nullable(UInt32)), e Array(Array(String))
/// Substreams (stored grouped by columns):
/// (a), (b.c, b.d.null, b.d), (e.size0, e.size1, e).
class ColumnsSubstreams
{
public:
    ColumnsSubstreams() = default;

    /// Add new column to the list with empty list of substreams.
    void addColumn(const String & column);
    /// Add new stream for last added column.
    void addSubstreamForLastColumn(const String & substream);

    size_t getSubstreamPosition(const String & column, const String & substream) const;
    size_t getSubstreamPosition(size_t column_position, const String & substream) const;
    size_t getFirstSubstreamPosition(const String & column) const;
    size_t getFirstSubstreamPosition(size_t column_position) const;

    void writeText(WriteBuffer & buf) const;
    void readText(ReadBuffer & buf);

    size_t getTotalSubstreams() const { return total_substreams; }
    bool empty() const { return !total_substreams; }

private:
    std::vector<std::pair<String, std::vector<String>>> columns_substreams;
    std::unordered_map<String, size_t> column_to_first_substream_position;
    std::unordered_map<String, std::unordered_map<String, size_t>> column_to_substream_positions;
    size_t total_substreams = 0;
};

}
