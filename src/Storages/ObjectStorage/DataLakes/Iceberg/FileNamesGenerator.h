#pragma once

#include "config.h"

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <Poco/UUIDGenerator.h>

namespace DB
{

#if USE_AVRO

/// Codec suffix used inside Iceberg metadata file names: `v{N}.<suffix>.metadata.json` or `{N}-<uuid>.<suffix>.metadata.json`.
/// The Iceberg spec (org.apache.iceberg.TableMetadataParser.Codec) defines the gzip
/// extension as "gz", not the HTTP Content-Encoding token "gzip" returned by
/// toContentEncodingName(). Using the wrong token makes Spark / Hadoop-catalog readers
/// unable to locate the metadata file.
std::string toIcebergMetadataCompressionExtension(CompressionMethod method);

struct GeneratedMetadataFileWithInfo
{
    Iceberg::IcebergPathFromMetadata path;
    Int32 version = 0;
    CompressionMethod compression_method = CompressionMethod::None;
};

/// Generates Iceberg metadata paths (IcebergPathFromMetadata) for new files.
///
/// Metadata files of a table without a catalog are named `v<N>.metadata.json`: the name is the commit lock, since writing
/// an existing name is refused. Metadata files of a catalog-backed table (`use_uuid_in_metadata`) are named
/// `<N>-<uuid>.metadata.json` with `N` zero-padded to 5 digits, as the Iceberg spec (section "Metastore Tables") and
/// Iceberg Java require. Iceberg Java and pyiceberg parse `N` only from this form, so other writers keep increasing it.
/// The UUID is derived from the name of the metadata file the commit is based on and from `N`, so the name is unique
/// for each base, but two writers that commit on top of the same base still pick the same name and only one of them
/// can write it: a catalog without a conditional pointer swap (Glue) relies on that to serialize writers.
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

    /// The next metadata file is a successor of the metadata file `base_metadata_path`, which has version `base_version`.
    /// Required with `use_uuid_in_metadata`.
    void setBaseMetadataFile(Int32 base_version, const String & base_metadata_path);
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
    String base_metadata_file_name;
    CompressionMethod compression_method = CompressionMethod::None;
    String format_name;

    Int32 initial_version = 0;
};

#endif

}
