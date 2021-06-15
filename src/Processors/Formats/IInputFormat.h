#pragma once

#include <Processors/ISource.h>

#include <memory>


namespace DB
{
/// Used to pass info from header between different InputFormats in ParallelParsing
struct ColumnMapping
{
    /// Non-atomic because there is strict `happens-before` between read and write access
    /// See InputFormatParallelParsing
    bool is_set{false};
    /// Maps indexes of columns in the input file to indexes of table columns
    using OptionalIndexes = std::vector<std::optional<size_t>>;
    OptionalIndexes column_indexes_for_input_fields;

    /// Tracks which columns we have read in a single read() call.
    /// For columns that are never read, it is initialized to false when we
    /// read the file header, and never changed afterwards.
    /// For other columns, it is updated on each read() call.
    std::vector<UInt8> read_columns;


    /// Whether we have any columns that are not read from file at all,
    /// and must be always initialized with defaults.
    bool have_always_default_columns{false};
};

using ColumnMappingPtr = std::shared_ptr<ColumnMapping>;

class ReadBuffer;

/** Input format is a source, that reads data from ReadBuffer.
  */
class IInputFormat : public ISource
{
protected:

    /// Skip GCC warning: ‘maybe_unused’ attribute ignored
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"

    ReadBuffer & in [[maybe_unused]];

#pragma GCC diagnostic pop

public:
    IInputFormat(Block header, ReadBuffer & in_);

    /** In some usecase (hello Kafka) we need to read a lot of tiny streams in exactly the same format.
     * The recreating of parser for each small stream takes too long, so we introduce a method
     * resetParser() which allow to reset the state of parser to continue reading of
     * source stream w/o recreating that.
     * That should be called after current buffer was fully read.
     */
    virtual void resetParser();

    virtual const BlockMissingValues & getMissingValues() const
    {
        static const BlockMissingValues none;
        return none;
    }

    /// Must be called from ParallelParsingInputFormat after readSuffix
    ColumnMappingPtr getColumnMapping() const { return column_mapping; }
    /// Must be called from ParallelParsingInputFormat before readPrefix
    void setColumnMapping(ColumnMappingPtr column_mapping_) { column_mapping = column_mapping_; }

    size_t getCurrentUnitNumber() const { return current_unit_number; }
    void setCurrentUnitNumber(size_t current_unit_number_) { current_unit_number = current_unit_number_; }

protected:
    ColumnMappingPtr column_mapping{};

private:
    /// Number of currently parsed chunk (if parallel parsing is enabled)
    size_t current_unit_number = 0;
};

}
