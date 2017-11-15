#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferValidUTF8.h>
#include <DataStreams/JSONRowOutputStream.h>

namespace DB
{

struct FormatSettingsJSON;

/** The stream for outputting data in the JSONCompact format.
  */
class JSONCompactRowOutputStream : public JSONRowOutputStream
{
public:
    JSONCompactRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool write_statistics_, const FormatSettingsJSON & settings);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

protected:
    void writeTotals() override;
    void writeExtremes() override;
};

}
