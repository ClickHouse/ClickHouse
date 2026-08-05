#pragma once
#include <Core/NamesAndTypes.h>
#include <functional>
#include <memory>
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
///
/// The per-column entries are immutable once built and can be shared across the `ColumnsSubstreams`
/// of many data parts (see `SharedPartColumns`): a column's substreams depend only on the column and
/// its serialization kinds, so parts whose substreams differ in other columns still share the
/// per-column entries. The object itself then holds only a vector of shared entries and the global
/// position of the first substream of each column.
class ColumnsSubstreams
{
public:
    ColumnsSubstreams() = default;

    struct ColumnEntry
    {
        String column;
        std::vector<String> substreams;
        /// Substream name -> position within this column.
        /// The global substream position is `first_substream_positions[column_position] + local`.
        std::unordered_map<String, size_t> substream_to_local_position;
    };
    using ColumnEntryPtr = std::shared_ptr<const ColumnEntry>;

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

    /// Returns the recorded substreams for a column by name, or nullptr if the column is not present.
    const std::vector<String> * tryGetColumnSubstreams(const String & column_name) const;

    void writeText(WriteBuffer & buf) const;
    void readText(ReadBuffer & buf);
    String toString() const;

    size_t getTotalSubstreams() const { return total_substreams; }
    bool empty() const { return !total_substreams; }

    /// Check that we have substreams for all columns and they have the same order as in provided list.
    void validateColumns(const std::vector<String> & columns) const;

    /// Check that all substream names have valid prefixes matching their column names.
    /// Every substream for a column must start with escapeForFileName(column_name) (or
    /// escapeForFileName(Nested::extractTableName(column_name)) for shared Nested offsets),
    /// followed by '.', '%2E', or end-of-string.
    /// Returns {invalid_substream, column_name} pair, or empty strings if all are valid.
    std::pair<String, String> findInvalidSubstreamName() const;

    /// Merge 2 sets of columns substreams with specified columns order.
    /// If some column exists in both left and right we keep only substreams from the left.
    /// Shared per-column entries of the inputs are reused, not copied.
    static ColumnsSubstreams merge(const ColumnsSubstreams & left, const ColumnsSubstreams & right, const std::vector<String> & columns_order);

    /// Compares only the ordered lists of substreams (the rest of the state is derived from them).
    bool operator==(const ColumnsSubstreams & other) const;

    /// Hash of the ordered lists of substreams, consistent with `operator==`.
    UInt128 getHash() const;

    /// Hash of one per-column entry (its name and ordered substreams).
    static UInt128 getColumnEntryHash(const ColumnEntry & entry);

    /// Replace every per-column entry with a content-identical shared one returned by `intern`.
    /// After this call the entries must not be modified (they may be shared with other parts).
    void internColumnEntries(const std::function<ColumnEntryPtr(const ColumnEntryPtr &)> & intern);

private:
    /// The entry being built: entries are only modified while uniquely owned.
    ColumnEntry & lastEntryForModification();

    std::vector<ColumnEntryPtr> columns_substreams;
    /// Global position of the first substream of each column (prefix sums of the substream counts).
    /// UInt32: this vector exists per data part, and the number of substreams of a part is far
    /// below the limit.
    std::vector<UInt32> first_substream_positions;
    size_t total_substreams = 0;
};

}
