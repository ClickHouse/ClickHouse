#pragma once

#include "config.h"

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>

namespace Iceberg
{

DB::MutableColumns parseAvro(avro::DataFileReaderBase & file_reader, const DB::Block & header, const DB::FormatSettings & settings);


std::string getProperFilePathFromMetadataInfo(std::string_view data_path, std::string_view common_path, std::string_view table_location);
}

#endif
