#pragma once

#include <DataStreams/TabSeparatedRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в формате TSKV.
  * TSKV похож на TabSeparated, но перед каждым значением указывается его имя и знак равенства: name=value.
  * Этот формат весьма неэффективен.
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

