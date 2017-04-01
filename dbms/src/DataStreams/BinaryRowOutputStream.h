#pragma once

#include <DataStreams/IRowOutputStream.h>


namespace DB
{

class IColumn;
class IDataType;
class WriteBuffer;


/** Поток для вывода данных в бинарном построчном формате.
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

