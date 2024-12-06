#include "config.h"

#if USE_AVRO


#    include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
#    include "Storages/ObjectStorage/DataLakes/Iceberg/Utils.h"


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
#    include "DataTypes/DataTypeTuple.h"
#    include "Formats/FormatSettings.h"

#    include <Processors/Formats/Impl/AvroRowInputFormat.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

namespace Iceberg
{

class ManifestFileContentImpl
{
public:
    explicit ManifestFileContentImpl(
        std::unique_ptr<avro::DataFileReaderBase> manifest_file_reader_,
        Int32 format_version_,
        const String & common_path,
        const FormatSettings & format_settings,
        Int32 schema_id_);

    Int32 schema_id;
    std::vector<DataFileEntry> data_files;
};

}

}

#endif
