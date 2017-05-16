#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <DataStreams/IRowOutputStream.h>


namespace DB
{

/** The stream for outputting data in JSON format, by object per line.
  * Does not validate UTF-8.
  */
class JSONEachRowRowOutputStream : public IRowOutputStream
{
public:
    JSONEachRowRowOutputStream(WriteBuffer & ostr_, const Block & sample, bool force_quoting_64bit_integers_ = true);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

    void flush() override
    {
        ostr.next();
    }

private:
    WriteBuffer & ostr;
    size_t field_number = 0;
    Names fields;
    bool force_quoting_64bit_integers;
};

}

