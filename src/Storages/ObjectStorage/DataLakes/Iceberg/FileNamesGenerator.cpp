#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <filesystem>
#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/format.h>

#if USE_AVRO

namespace DB
{

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
    compression_method = other.compression_method;
    format_name = other.format_name;
    metadata_naming = other.metadata_naming;
}

FileNamesGenerator & FileNamesGenerator::operator=(const FileNamesGenerator & other)
{
    if (this == &other)
        return *this;

    initial_version = other.initial_version;
    table_location = other.table_location;
    data_location = other.data_location;
    use_uuid_in_metadata = other.use_uuid_in_metadata;
    compression_method = other.compression_method;
    format_name = other.format_name;
    metadata_naming = other.metadata_naming;

    return *this;
}

void FileNamesGenerator::continueMetadataChain(const String & current_metadata_path, Int32 current_version)
{
    initial_version = current_version + 1;
    const String name = std::filesystem::path(current_metadata_path).filename();
    if (Iceberg::isVersionNumberedCommitScheme(name))
        metadata_naming = MetadataNaming::Version;
    else if (name.starts_with('v'))
        metadata_naming = MetadataNaming::VersionWithUuid;
    else
        metadata_naming = MetadataNaming::NumberWithUuid;
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
        if (metadata_naming == MetadataNaming::Version)
        {
            return GeneratedMetadataFileWithInfo{
                .path = Iceberg::IcebergPathFromMetadata(
                    fmt::format("{}metadata/v{}{}.metadata.json", table_location, used_version, compression_suffix)),
                .version = used_version,
                .compression_method = compression_method};
        }

        /// The file name is the commit's compare-and-set, so every writer of a version must aim at the same one.
        auto uuid_str = uuid_generator.createFromName(Poco::UUID::uri(), fmt::format("{}metadata/{}", table_location, used_version)).toString();
        auto file_name = metadata_naming == MetadataNaming::VersionWithUuid
            ? fmt::format("v{}-{}{}.metadata.json", used_version, uuid_str, compression_suffix)
            : fmt::format("{:05}-{}{}.metadata.json", used_version, uuid_str, compression_suffix);
        return GeneratedMetadataFileWithInfo{
            .path = Iceberg::IcebergPathFromMetadata(fmt::format("{}metadata/{}", table_location, file_name)),
            .version = used_version,
            .compression_method = compression_method};
    }
    else
    {
        auto uuid_str = uuid_generator.createRandom().toString();
        return GeneratedMetadataFileWithInfo{
            .path = Iceberg::IcebergPathFromMetadata(
                fmt::format("{}metadata/v{}-{}{}.metadata.json", table_location, used_version, uuid_str, compression_suffix)),
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
