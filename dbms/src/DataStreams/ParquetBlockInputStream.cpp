#include <DataStreams/ParquetBlockInputStream.h>

#include <IO/copyData.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

ParquetBlockInputStream::ParquetBlockInputStream(ReadBuffer & istr_, const Block & header_)
    : istr(istr_)
    , header(header_)
{
}

Block ParquetBlockInputStream::getHeader() const
{
    return header;
}

Block ParquetBlockInputStream::readImpl()
{
    Block res;

    if (istr.eof())
        return res;

    // TODO: maybe use parquet::RandomAccessSource?
    std::string file_data;

    {
        WriteBufferFromString file_buffer(file_data);
        copyData(istr, file_buffer);
    }

    return res;
}

}
