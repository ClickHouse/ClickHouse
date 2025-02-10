#pragma once

#include <Formats/FormatFactory.h>
#include <parquet/metadata.h>

namespace DB
{

std::shared_ptr<parquet::FileMetaData>
getFileMetadata(ReadBuffer & in, const FormatSettings & format_settings, std::atomic<int> & is_stopped);

}
