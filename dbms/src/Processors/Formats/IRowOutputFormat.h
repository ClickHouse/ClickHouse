#pragma once

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>

#include <string>


namespace DB
{

class WriteBuffer;

/** Output format that writes data row by row.
  */
class IRowOutputFormat : public IOutputFormat
{
protected:
    DataTypes types;

    void consume(Chunk chunk) override;
    void consumeTotals(Chunk chunk) override;
    void consumeExtremes(Chunk chunk) override;
    void finalize() override;

public:
    IRowOutputFormat(const Block & header, WriteBuffer & out_, FormatFactory::WriteCallback callback)
        : IOutputFormat(header, out_), types(header.getDataTypes()), write_single_row_callback(callback)
    {
    }

    /** Write a row.
      * Default implementation calls methods to write single values and delimiters
      * (except delimiter between rows (writeRowBetweenDelimiter())).
      */
    virtual void write(const Columns & columns, size_t row_num);
    virtual void writeMinExtreme(const Columns & columns, size_t row_num);
    virtual void writeMaxExtreme(const Columns & columns, size_t row_num);
    virtual void writeTotals(const Columns & columns, size_t row_num);

    /** Write single value. */
    virtual void writeField(const IColumn & column, const IDataType & type, size_t row_num) = 0;

    /** Write delimiter. */
    virtual void writeFieldDelimiter() {}       /// delimiter between values
    virtual void writeRowStartDelimiter() {}    /// delimiter before each row
    virtual void writeRowEndDelimiter() {}      /// delimiter after each row
    virtual void writeRowBetweenDelimiter() {}  /// delimiter between rows
    virtual void writePrefix() {}               /// delimiter before resultset
    virtual void writeSuffix() {}               /// delimiter after resultset
    virtual void writeBeforeTotals() {}
    virtual void writeAfterTotals() {}
    virtual void writeBeforeExtremes() {}
    virtual void writeAfterExtremes() {}
    virtual void writeLastSuffix() {}  /// Write something after resultset, totals end extremes.

private:
    bool first_row = true;
    bool prefix_written = false;
    bool suffix_written = false;

    // Callback used to indicate that another row is written.
    FormatFactory::WriteCallback write_single_row_callback;

    void writePrefixIfNot()
    {
        if (!prefix_written)
            writePrefix();

        prefix_written = true;
    }

    void writeSuffixIfNot()
    {
        if (!suffix_written)
            writeSuffix();

        suffix_written = true;
    }

};

}
