#pragma once

#include <DataStreams/IRowOutputStream.h>


namespace DB
{

class WriteBuffer;


/** A stream for outputting data in the VALUES format (as in the INSERT request).
  */
class ValuesRowOutputStream : public IRowOutputStream
{
public:
    ValuesRowOutputStream(WriteBuffer & ostr_);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;
    void flush() override;

private:
    WriteBuffer & ostr;
};

}

