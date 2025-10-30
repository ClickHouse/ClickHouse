#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/Block_fwd.h>
#include <Core/BlockNameMap.h>

namespace DB
{

struct FormatSettings;
using Names = std::vector<std::string>;

/// Used for input text formats with headers/structure to map columns from input
/// and columns in header by names.
/// It's also used to pass info from header between different InputFormats in ParallelParsing
struct ColumnMapping
{
    /// Special flag for ParallelParsing. Non-atomic because there is strict
    /// `happens-before` between read and write access. See InputFormatParallelParsing
    bool is_set{false};

    /// Maps indexes of columns in the input file to indexes of table columns
    using OptionalIndexes = std::vector<std::optional<size_t>>;
    OptionalIndexes column_indexes_for_input_fields;

    /// The list of column indexes that are not presented in input data.
    std::vector<size_t> not_presented_columns;

    /// The list of column names in input data. Needed for better exception messages.
    std::vector<String> names_of_columns;

    void setupByHeader(const Block & header);

    void addColumns(
        const Names & column_names, const BlockNameMap & column_indexes_by_names, const FormatSettings & settings);

    void insertDefaultsForNotSeenColumns(MutableColumns & columns, std::vector<UInt8> & read_columns);
};

}
