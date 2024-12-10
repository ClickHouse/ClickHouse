#pragma once

#include "config.h"

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>

namespace DB
{

namespace Iceberg
{

MutableColumns parseAvro(avro::DataFileReaderBase & file_reader, const Block & header, const FormatSettings & settings);

}

}
#endif
