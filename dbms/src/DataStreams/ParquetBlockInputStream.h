#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
/* #include <DataStreams/MarkInCompressedFile.h> */
/* #include <Common/PODArray.h> */

namespace DB
{

class ParquetBlockInputStream : public IProfilingBlockInputStream
{
public:
    ParquetBlockInputStream(ReadBuffer & istr_, const Block & header_);

    String getName() const override { return "Parquet"; }
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    ReadBuffer & istr;
    Block header;
};

}
