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

private:
    bool prefix_written = false;
    bool suffix_written = false;

protected:
    DataTypes types;
    Serializations serializations;
    Params params;

    bool first_row = true;

    void consume(Chunk chunk) override;
    void consumeTotals(Chunk chunk) override;
    void consumeExtremes(Chunk chunk) override;
    void finalize() override;

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

public:
    IRowOutputFormat(const Block & header, WriteBuffer & out_, const Params & params_);

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
    virtual void writePrefix() {}               /// delimiter before resultset
    virtual void writeSuffix() {}               /// delimiter after resultset
    virtual void writeBeforeTotals() {}
    virtual void writeAfterTotals() {}
    virtual void writeBeforeExtremes() {}
    virtual void writeAfterExtremes() {}
    virtual void writeLastSuffix() {}  /// Write something after resultset, totals end extremes.

};

}
