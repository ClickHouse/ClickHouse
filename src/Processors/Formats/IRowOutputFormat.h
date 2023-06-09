#pragma once

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>

#include <string>


namespace DB
{

struct RowOutputFormatParams
{
    using WriteCallback = std::function<void(const Columns & columns,size_t row)>;

    // Callback used to indicate that another row is written.
    WriteCallback callback;
};

class WriteBuffer;

/** Output format that writes data row by row.
  */
class IRowOutputFormat : public IOutputFormat
{
public:
    using Params = RowOutputFormatParams;

protected:
    IRowOutputFormat(const Block & header, WriteBuffer & out_, const Params & params_);
    void consume(Chunk chunk) override;
    void consumeTotals(Chunk chunk) override;
    void consumeExtremes(Chunk chunk) override;

    /** Write a row.
      * Default implementation calls methods to write single values and delimiters
      * (except delimiter between rows (writeRowBetweenDelimiter())).
      */
    virtual void write(const Columns & columns, size_t row_num);
    virtual void writeMinExtreme(const Columns & columns, size_t row_num);
    virtual void writeMaxExtreme(const Columns & columns, size_t row_num);
    virtual void writeTotals(const Columns & columns, size_t row_num);

    /** Write single value. */
    virtual void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) = 0;

    /** Write delimiter. */
    virtual void writeFieldDelimiter() {}       /// delimiter between values
    virtual void writeRowStartDelimiter() {}    /// delimiter before each row
    virtual void writeRowEndDelimiter() {}      /// delimiter after each row
    virtual void writeRowBetweenDelimiter() {}  /// delimiter between rows
    virtual void writePrefix() override {}      /// delimiter before resultset
    virtual void writeSuffix() override {}      /// delimiter after resultset
    virtual void writeBeforeTotals() {}
    virtual void writeAfterTotals() {}
    virtual void writeBeforeExtremes() {}
    virtual void writeAfterExtremes() {}
    virtual void finalizeImpl() override {}  /// Write something after resultset, totals end extremes.

    size_t num_columns;
    DataTypes types;
    Serializations serializations;
    Params params;

    bool first_row = true;
};

}
