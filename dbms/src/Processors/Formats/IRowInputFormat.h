#pragma once

#include <string>
#include <Columns/IColumn.h>
#include <Processors/Formats/IInputFormat.h>


namespace DB
{

/** Row oriented input format: reads data row by row.
  */
class IRowInputFormat : public IInputFormat
{
public:
    /// Common parameters for generating blocks.
    struct Params
    {
        size_t max_block_size;

        UInt64 allow_errors_num;
        Float64 allow_errors_ratio;
    };

    IRowInputFormat(
        Block header,
        ReadBuffer & in,
        Params params)
        : IInputFormat(header, in), params(std::move(params))
    {
    }

    Chunk generate() override;

protected:
    /** Read next row and append it to the columns.
      * If no more rows - return false.
      */
    virtual bool readRow(MutableColumns & columns) = 0;

    virtual void readPrefix() {};                /// delimiter before begin of result
    virtual void readSuffix() {};                /// delimiter after end of result

    /// Skip data until next row.
    /// This is intended for text streams, that allow skipping of errors.
    /// By default - throws not implemented exception.
    virtual bool allowSyncAfterError() const { return false; }
    virtual void syncAfterError();

    /// In case of parse error, try to roll back and parse last one or two rows very carefully
    ///  and collect as much as possible diagnostic information about error.
    /// If not implemented, returns empty string.
    virtual std::string getDiagnosticInfo() { return {}; };

private:
    Params params;

    size_t total_rows = 0;
    size_t num_errors = 0;
};

}

