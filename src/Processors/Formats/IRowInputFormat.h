#pragma once

#include <string>
#include <Columns/IColumn.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/SizeLimits.h>
#include <Poco/Timespan.h>
#include <DataTypes/Serializations/SerializationInfo.h>

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

    Chunk read() override;

    void resetParser() override;

protected:
    /** Read next row and append it to the columns.
      * If no more rows - return false.
      */
    virtual bool readRow(MutableColumns & columns, RowReadExtension & extra) = 0;

    /// Count some rows. Called in a loop until it returns 0, and the return values are added up.
    /// `max_block_size` is the recommended number of rows after which to stop, if the implementation
    /// involves scanning the data. If the implementation just takes the count from metadata,
    /// `max_block_size` can be ignored.
    virtual size_t countRows(size_t max_block_size);
    virtual bool supportsCountRows() const { return false; }
    virtual bool supportsCustomSerializations() const { return false; }

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
    /// Get diagnostic info and raw data for a row
    virtual std::pair<std::string, std::string> getDiagnosticAndRawData() { return std::make_pair("", ""); }

    void logError();

    const BlockMissingValues * getMissingValues() const override { return &block_missing_values; }

    size_t getRowNum() const { return total_rows; }

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

    void setRowsReadBefore(size_t rows) override { total_rows = rows; }
    void setSerializationHints(const SerializationInfoByName & hints) override;

    Serializations serializations;

private:
    Params params;

    size_t total_rows = 0;
    size_t num_errors = 0;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;
};

}
