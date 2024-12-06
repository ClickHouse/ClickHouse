#pragma once

#include "config.h"

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.


#    include <Core/Types.h>
#    include <Disks/ObjectStorages/IObjectStorage.h>
#    include <Interpreters/Context_fwd.h>
#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/StorageObjectStorage.h>
#    include <DataFile.hh>

#    include <Poco/JSON/Array.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Parser.h>

#    include <Common/Exception.h>
#    include "Formats/FormatSettings.h"

#    include <Processors/Formats/Impl/AvroRowInputFormat.h>

namespace DB
{

namespace Iceberg
{

MutableColumns parseAvro(avro::DataFileReaderBase & file_reader, const Block & header, const FormatSettings & settings);

}

}
#endif
