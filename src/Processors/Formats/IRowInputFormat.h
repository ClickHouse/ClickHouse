#pragma once

#include <string>
#include <Columns/IColumn.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/SizeLimits.h>
#include <Poco/Timespan.h>

class Stopwatch;

namespace DB
{

/// Contains extra information about read data.
struct RowReadExtension
{
    /// IRowInputFormat::read output. It contains non zero for columns that actually read from the source and zero otherwise.
    /// It's used to attach defaults for partially filled rows.
    std::vector<UInt8> read_columns;
};

/// Common parameters for generating blocks.
struct RowInputFormatParams
{
    size_t max_block_size = 0;

    UInt64 allow_errors_num = 0;
    Float64 allow_errors_ratio = 0;

    Poco::Timespan max_execution_time = 0;
    OverflowMode timeout_overflow_mode = OverflowMode::THROW;
};

bool isParseError(int code);
bool checkTimeLimit(const RowInputFormatParams & params, const Stopwatch & stopwatch);

/// Row oriented input format: reads data row by row.
class IRowInputFormat : public IInputFormat
{
public:
    using Params = RowInputFormatParams;

    IRowInputFormat(Block header, ReadBuffer & in_, Params params_);

    Chunk generate() override;

    void resetParser() override;

protected:
    /** Read next row and append it to the columns.
      * If no more rows - return false.
      */
    virtual bool readRow(MutableColumns & columns, RowReadExtension & extra) = 0;

    virtual void readPrefix() {}                /// delimiter before begin of result
    virtual void readSuffix() {}                /// delimiter after end of result

    /// Skip data until next row.
    /// This is intended for text streams, that allow skipping of errors.
    /// By default - throws not implemented exception.
    virtual bool allowSyncAfterError() const { return false; }
    virtual void syncAfterError();

    /// In case of parse error, try to roll back and parse last one or two rows very carefully
    ///  and collect as much as possible diagnostic information about error.
    /// If not implemented, returns empty string.
    virtual std::string getDiagnosticInfo() { return {}; }

    const BlockMissingValues & getMissingValues() const override { return block_missing_values; }

    size_t getTotalRows() const { return total_rows; }

    Serializations serializations;

private:
    Params params;

    size_t total_rows = 0;
    size_t num_errors = 0;

    BlockMissingValues block_missing_values;
};

}
