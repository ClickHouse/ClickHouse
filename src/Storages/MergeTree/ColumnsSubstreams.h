#pragma once
#include <Core/NamesAndTypes.h>
#include <string>
#include <vector>
#include <unordered_map>

namespace DB
{

struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

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
    void addSubstreamToLastColumn(const String & substream);
    void addSubstreamsToLastColumn(const std::vector<String> & substreams);

    size_t getSubstreamPosition(size_t column_position, const String & substream) const;
    std::optional<size_t> tryGetSubstreamPosition(size_t column_position, const String & substream) const;
    size_t getSubstreamPosition(size_t column_position, const NameAndTypePair & name_and_type, const ISerialization::SubstreamPath & substream_path, const MergeTreeSettingsPtr & storage_settings) const;
    std::optional<size_t> tryGetSubstreamPosition(const String & substream) const;
    size_t getFirstSubstreamPosition(size_t column_position) const;
    size_t getLastSubstreamPosition(size_t column_position) const;

    const std::vector<String> & getColumnSubstreams(size_t column_position) const;

    void writeText(WriteBuffer & buf) const;
    void readText(ReadBuffer & buf);
    String toString() const;

    size_t getTotalSubstreams() const { return total_substreams; }
    bool empty() const { return !total_substreams; }

    /// Check that we have substreams for all columns and they have the same order as in provided list.
    void validateColumns(const std::vector<String> & columns) const;

    /// Merge 2 sets of columns substreams with specified columns order.
    /// If some column exists in both left and right we keep only substreams from the left.
    static ColumnsSubstreams merge(const ColumnsSubstreams & left, const ColumnsSubstreams & right, const std::vector<String> & columns_order);

private:
    std::vector<std::pair<String, std::vector<String>>> columns_substreams;
    std::vector<std::unordered_map<String, size_t>> column_position_to_substream_positions;
    size_t total_substreams = 0;
};

}
