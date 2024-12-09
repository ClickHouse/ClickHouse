#include "config.h"

#if USE_AVRO

#include "Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h"
#include <Processors/Formats/Impl/AvroRowInputFormat.h>

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
