#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferValidUTF8.h>
#include <DataStreams/JSONRowOutputStream.h>


namespace DB
{

/** The stream for outputting data in the JSONCompact format.
  */
class JSONCompactRowOutputStream : public JSONRowOutputStream
{
public:
    JSONCompactRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool write_statistics_, bool force_quoting_64bit_integers_ = true);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

protected:
    void writeTotals() override;
    void writeExtremes() override;
};

}
