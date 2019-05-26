#pragma once

#include <Common/config.h>
#if USE_PARQUET
#    include <DataStreams/IBlockInputStream.h>
//#    include <parquet/file_reader.h>
//#    include <parquet/arrow/reader.h>
//#    include <arrow/buffer.h>


namespace parquet { namespace arrow { class FileReader; } }
namespace arrow { class Buffer; }

namespace DB
{
class Context;

class ParquetBlockInputStream : public IBlockInputStream
{
public:
    ParquetBlockInputStream(ReadBuffer & istr_, const Block & header_, const Context & context_);

    String getName() const override { return "Parquet"; }
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    ReadBuffer & istr;
    Block header;

    // TODO: check that this class implements every part of its parent

    const Context & context;

    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    std::string file_data;
    std::unique_ptr<arrow::Buffer> buffer;
    int row_group_total = 0;
    int row_group_current = 0;
};

}

#endif
