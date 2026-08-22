#pragma once

#include "config.h"

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <Poco/UUIDGenerator.h>

namespace DB
{

#if USE_AVRO

struct GeneratedMetadataFileWithInfo
{
    Iceberg::IcebergPathFromMetadata path;
    Int32 version;
    CompressionMethod compression_method;
};

/// Generates Iceberg metadata paths (IcebergPathFromMetadata) for new files.
///
/// All generated paths use table_location as prefix, ensuring they are
/// always in the correct format for writing into Iceberg metadata files.
/// To get the actual storage path for I/O, pass the result through
/// IcebergPathResolver::resolve().
class FileNamesGenerator
{
public:
    FileNamesGenerator() = default;
    explicit FileNamesGenerator(
        const String & table_location_,
        bool use_uuid_in_metadata_,
        CompressionMethod compression_method_,
        const String & format_name_);

    FileNamesGenerator(const FileNamesGenerator & other);
    FileNamesGenerator & operator=(const FileNamesGenerator & other);

    /// All generate* methods return IcebergPathFromMetadata.
    /// These paths are ready to be written into Iceberg metadata files.
    /// To get a storage path for actual I/O, use IcebergPathResolver::resolve().
    Iceberg::IcebergPathFromMetadata generateDataFileName();
    Iceberg::IcebergPathFromMetadata generateManifestEntryName();
    Iceberg::IcebergPathFromMetadata generateManifestListName(Int64 snapshot_id, Int32 format_version);
    GeneratedMetadataFileWithInfo generateMetadataPathWithInfo();
    Iceberg::IcebergPathFromMetadata generateVersionHint();
    Iceberg::IcebergPathFromMetadata generatePositionDeleteFile();

    void setVersion(Int32 initial_version_) { initial_version = initial_version_; }
    void setCompressionMethod(CompressionMethod compression_method_) { compression_method = compression_method_; }

    void setDataLocation(String data_location_)
    {
        data_location = std::move(data_location_);
        if (!data_location.empty() && data_location.back() == '/')
            data_location.pop_back();
    }

private:
    Poco::UUIDGenerator uuid_generator;
    String table_location;
    String data_location; /// Optional override from `write.data.path` table property
    bool use_uuid_in_metadata = false;
    CompressionMethod compression_method = CompressionMethod::None;
    String format_name;

    Int32 initial_version = 0;
};

#endif

}
