#include "ParquetMetadataReader.h"
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <parquet/file_reader.h>

namespace DB
{

std::shared_ptr<parquet::FileMetaData>
getFileMetadata(ReadBuffer & in, const FormatSettings & format_settings, std::atomic<int> & is_stopped)
{
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true);
    return parquet::ReadMetaData(arrow_file);
}

}
