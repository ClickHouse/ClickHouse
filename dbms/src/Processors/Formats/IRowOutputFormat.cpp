#include <string>
#include <Processors/Formats/IRowOutputFormat.h>


namespace DB
{

void IRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    size_t num_columns = columns.size();

    writeRowStartDelimiter();

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *types[i], row_num);
    }

    writeRowEndDelimiter();
}

}



