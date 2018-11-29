#pragma once

#include <Formats/IRowOutputStream.h>


namespace DB
{

class IColumn;
class IDataType;
class WriteBuffer;


/** A stream for outputting data in a binary line-by-line format.
  */
class BinaryRowOutputStream : public IRowOutputStream
{
public:
    BinaryRowOutputStream(WriteBuffer & ostr_);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;

    void flush() override;

    String getContentType() const override { return "application/octet-stream"; }

protected:
    WriteBuffer & ostr;
};

}

