#pragma once

#include <DataStreams/TabSeparatedRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в формате tsv, но без эскейпинга отдельных значений.
  * (То есть - вывод необратимый.)
  */
class TabSeparatedRawRowOutputStream : public TabSeparatedRowOutputStream
{
public:
    TabSeparatedRawRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_ = false, bool with_types_ = false)
        : TabSeparatedRowOutputStream(ostr_, sample_, with_names_, with_types_) {}

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override
    {
        type.serializeText(column, row_num, ostr);
    }
};

}

