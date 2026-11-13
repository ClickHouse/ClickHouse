#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>

#include <Common/Exception.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/format.h>

#include <filesystem>

#if USE_AVRO

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::string toIcebergMetadataCompressionExtension(CompressionMethod method)
{
    /// Iceberg only defines gzip compression for metadata files, and its file
    /// extension is "gz" (org.apache.iceberg.TableMetadataParser.Codec).
    if (method == CompressionMethod::Gzip)
        return "gz";
    return toContentEncodingName(method);
}

FileNamesGenerator::FileNamesGenerator(
    const String & table_location_,
    bool use_uuid_in_metadata_,
    CompressionMethod compression_method_,
    const String & format_name_)
    : table_location(table_location_)
    , use_uuid_in_metadata(use_uuid_in_metadata_)
    , compression_method(compression_method_)
    , format_name(boost::to_lower_copy(format_name_))
{
    if (!table_location.empty() && table_location.back() != '/')
        table_location += '/';
}

FileNamesGenerator::FileNamesGenerator(const FileNamesGenerator & other)
{
    initial_version = other.initial_version;
    table_location = other.table_location;
    data_location = other.data_location;
    use_uuid_in_metadata = other.use_uuid_in_metadata;
    base_metadata_file_name = other.base_metadata_file_name;
    compression_method = other.compression_method;
    format_name = other.format_name;
}

FileNamesGenerator & FileNamesGenerator::operator=(const FileNamesGenerator & other)
{
    if (this == &other)
        return *this;

    initial_version = other.initial_version;
    table_location = other.table_location;
    data_location = other.data_location;
    use_uuid_in_metadata = other.use_uuid_in_metadata;
    base_metadata_file_name = other.base_metadata_file_name;
    compression_method = other.compression_method;
    format_name = other.format_name;

    return *this;
}

void FileNamesGenerator::setBaseMetadataFile(Int32 base_version, const String & base_metadata_path)
{
    initial_version = base_version + 1;
    base_metadata_file_name = std::filesystem::path(base_metadata_path).filename();
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generateDataFileName()
{
    auto uuid_str = uuid_generator.createRandom().toString();
    if (!data_location.empty())
        return Iceberg::IcebergPathFromMetadata(fmt::format("{}/data-{}.{}", data_location, uuid_str, format_name));
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}data/data-{}.{}", table_location, uuid_str, format_name));
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generateManifestEntryName()
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}metadata/{}.avro", table_location, uuid_str));
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generateStatisticsFileName(Int64 snapshot_id)
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}metadata/{}-{}.stats", table_location, snapshot_id, uuid_str));
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generateManifestListName(Int64 snapshot_id, Int32 format_version)
{
    auto uuid_str = uuid_generator.createRandom().toString();
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}metadata/snap-{}-{}-{}.avro", table_location, snapshot_id, format_version, uuid_str));
}

GeneratedMetadataFileWithInfo FileNamesGenerator::generateMetadataPathWithInfo()
{
    auto compression_suffix = toIcebergMetadataCompressionExtension(compression_method);
    if (!compression_suffix.empty())
        compression_suffix = "." + compression_suffix;
    auto used_version = initial_version++;
    if (!use_uuid_in_metadata)
    {
        return GeneratedMetadataFileWithInfo{
            .path = Iceberg::IcebergPathFromMetadata(
                fmt::format("{}metadata/v{}{}.metadata.json", table_location, used_version, compression_suffix)),
            .version = used_version,
            .compression_method = compression_method};
    }
    else
    {
        if (base_metadata_file_name.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The base metadata file is not set for a metadata file name with a UUID");

        /// Not random: writers that commit on top of the same base must collide on the name, see the class comment.
        auto uuid_str = uuid_generator.createFromName(Poco::UUID::oid(), fmt::format("{}/{}", base_metadata_file_name, used_version)).toString();
        return GeneratedMetadataFileWithInfo{
            .path = Iceberg::IcebergPathFromMetadata(
                fmt::format("{}metadata/{:05d}-{}{}.metadata.json", table_location, used_version, uuid_str, compression_suffix)),
            .version = used_version,
            .compression_method = compression_method};
    }
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generateVersionHint()
{
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}metadata/version-hint.text", table_location));
}

Iceberg::IcebergPathFromMetadata FileNamesGenerator::generatePositionDeleteFile()
{
    auto uuid_str = uuid_generator.createRandom().toString();
    if (!data_location.empty())
        return Iceberg::IcebergPathFromMetadata(fmt::format("{}/{}-deletes.{}", data_location, uuid_str, format_name));
    return Iceberg::IcebergPathFromMetadata(fmt::format("{}data/{}-deletes.{}", table_location, uuid_str, format_name));
}

}

#endif
