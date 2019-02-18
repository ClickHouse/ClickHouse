#pragma once

#include <string>
#include <Processors/Formats/IOutputFormat.h>


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

public:
    IRowOutputFormat(Block header, WriteBuffer & out)
        : IOutputFormat(header, out), types(header.getDataTypes())
    {
    }

    /** Write a row.
      * Default implementation calls methods to write single values and delimiters
      * (except delimiter between rows (writeRowBetweenDelimiter())).
      */
    virtual void write(const Columns & columns, size_t row_num);

    /** Write single value. */
    virtual void writeField(const IColumn & column, const IDataType & type, size_t row_num) = 0;

    /** Write delimiter. */
    virtual void writeFieldDelimiter() {};       /// delimiter between values
    virtual void writeRowStartDelimiter() {};    /// delimiter before each row
    virtual void writeRowEndDelimiter() {};      /// delimiter after each row
    virtual void writeRowBetweenDelimiter() {};  /// delimiter between rows
    virtual void writePrefix() {};               /// delimiter before resultset
    virtual void writeSuffix() {};               /// delimiter after resultset
    virtual void writeBeforeTotals() {};
    virtual void writeAfterTotals() {};
    virtual void writeBeforeExtremes() {};
    virtual void writeAfterExtremes() {};
};

}


