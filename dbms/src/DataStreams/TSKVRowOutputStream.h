#pragma once

#include <DataStreams/TabSeparatedRowOutputStream.h>


namespace DB
{

/** The stream for outputting data in the TSKV format.
  * TSKV is similar to TabSeparated, but before every value, its name and equal sign are specified: name=value.
  * This format is very inefficient.
  */
class TSKVRowOutputStream : public TabSeparatedRowOutputStream
{
public:
    TSKVRowOutputStream(WriteBuffer & ostr_, const Block & sample_);
    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeRowEndDelimiter() override;

protected:
    NamesAndTypes fields;
    size_t field_number = 0;
};

}

