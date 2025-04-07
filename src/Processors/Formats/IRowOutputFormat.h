#pragma once

#include <Processors/Formats/IOutputFormat.h>

#include <string>


namespace DB
{

class WriteBuffer;

/** Output format that writes data row by row.
  */
class IRowOutputFormat : public IOutputFormat
{
public:
    /// Used to work with IRowOutputFormat explicitly.
    void writeRow(const Columns & columns, size_t row_num)
    {
        first_row = false;
        write(columns, row_num);
    }

    virtual void writeRowBetweenDelimiter() {}  /// delimiter between rows

protected:
    IRowOutputFormat(const Block & header, WriteBuffer & out_);
    void consume(Chunk chunk) override;
    void consumeTotals(Chunk chunk) override;
    void consumeExtremes(Chunk chunk) override;

    virtual bool supportTotals() const { return false; }
    virtual bool supportExtremes() const { return false; }

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
    void writePrefix() override {}      /// delimiter before resultset
    void writeSuffix() override {}      /// delimiter after resultset
    virtual void writeBeforeTotals() {}
    virtual void writeAfterTotals() {}
    virtual void writeBeforeExtremes() {}
    virtual void writeAfterExtremes() {}
    void finalizeImpl() override {}  /// Write something after resultset, totals end extremes.

    bool haveWrittenData() { return !first_row || getRowsReadBefore() != 0; }

    size_t num_columns;
    DataTypes types;
    Serializations serializations;

    bool first_row = true;
};

}
