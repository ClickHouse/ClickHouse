#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>

#include <boost/algorithm/string/case_conv.hpp>

#if USE_AVRO

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB
{

FileNamesGenerator::FileNamesGenerator(
    const String & table_dir_,
    const String & storage_dir_,
    bool use_uuid_in_metadata_,
    CompressionMethod compression_method_,
    const String & format_name_)
    : table_dir(table_dir_)
    , storage_dir(storage_dir_)
    , data_dir(table_dir + "data/")
    , metadata_dir(table_dir + "metadata/")
    , storage_data_dir(storage_dir + "data/")
    , storage_metadata_dir(storage_dir + "metadata/")
    , use_uuid_in_metadata(use_uuid_in_metadata_)
    , compression_method(compression_method_)
    , format_name(boost::to_lower_copy(format_name_))
{
}

FileNamesGenerator::FileNamesGenerator(const FileNamesGenerator & other)
{
    data_dir = other.data_dir;
    metadata_dir = other.metadata_dir;
    storage_data_dir = other.storage_data_dir;
    storage_metadata_dir = other.storage_metadata_dir;
    initial_version = other.initial_version;

    table_dir = other.table_dir;
    storage_dir = other.storage_dir;
    use_uuid_in_metadata = other.use_uuid_in_metadata;
    compression_method = other.compression_method;
    format_name = other.format_name;
}

FileNamesGenerator & FileNamesGenerator::operator=(const FileNamesGenerator & other)
{
    if (this == &other)
        return *this;

    data_dir = other.data_dir;
    metadata_dir = other.metadata_dir;
    storage_data_dir = other.storage_data_dir;
    storage_metadata_dir = other.storage_metadata_dir;
    initial_version = other.initial_version;

    table_dir = other.table_dir;
    storage_dir = other.storage_dir;
    use_uuid_in_metadata = other.use_uuid_in_metadata;
    compression_method = other.compression_method;
    format_name = other.format_name;

    return *this;
}

FileNamesGenerator::Result FileNamesGenerator::generateDataFileName()
{
    auto uuid_str = uuid_generator.createRandom().toString();

    return Result{
        .path_in_metadata = fmt::format("{}data-{}.{}", data_dir, uuid_str, format_name),
        .path_in_storage = fmt::format("{}data-{}.{}", storage_data_dir, uuid_str, format_name)
    };
}

FileNamesGenerator::Result FileNamesGenerator::generateManifestEntryName()
{
    auto uuid_str = uuid_generator.createRandom().toString();

    return Result{
        .path_in_metadata = fmt::format("{}{}.avro", metadata_dir, uuid_str),
        .path_in_storage = fmt::format("{}{}.avro", storage_metadata_dir, uuid_str),
    };
}

FileNamesGenerator::Result FileNamesGenerator::generateManifestListName(Int64 snapshot_id, Int32 format_version)
{
    auto uuid_str = uuid_generator.createRandom().toString();

    return Result{
        .path_in_metadata = fmt::format("{}snap-{}-{}-{}.avro", metadata_dir, snapshot_id, format_version, uuid_str),
        .path_in_storage = fmt::format("{}snap-{}-{}-{}.avro", storage_metadata_dir, snapshot_id, format_version, uuid_str),
    };
}

FileNamesGenerator::Result FileNamesGenerator::generateMetadataName()
{
    auto compression_suffix = toContentEncodingName(compression_method);
    if (!compression_suffix.empty())
        compression_suffix = "." + compression_suffix;
    if (!use_uuid_in_metadata)
    {
        auto res = Result{
            .path_in_metadata = fmt::format("{}v{}{}.metadata.json", metadata_dir, initial_version, compression_suffix),
            .path_in_storage = fmt::format("{}v{}{}.metadata.json", storage_metadata_dir, initial_version, compression_suffix),
        };
        initial_version++;
        return res;
    }
    else
    {
        auto uuid_str = uuid_generator.createRandom().toString();
        auto res = Result{
            .path_in_metadata = fmt::format("{}v{}-{}{}.metadata.json", metadata_dir, initial_version, uuid_str, compression_suffix),
            .path_in_storage = fmt::format("{}v{}-{}{}.metadata.json", storage_metadata_dir, initial_version, uuid_str, compression_suffix),
        };
        initial_version++;
        return res;
    }
}

FileNamesGenerator::Result FileNamesGenerator::generateVersionHint()
{
    return Result{
        .path_in_metadata = fmt::format("{}version-hint.text", metadata_dir),
        .path_in_storage = fmt::format("{}version-hint.text", storage_metadata_dir),
    };
}

FileNamesGenerator::Result FileNamesGenerator::generatePositionDeleteFile()
{
    auto uuid_str = uuid_generator.createRandom().toString();

    return Result{
        .path_in_metadata = fmt::format("{}{}-deletes.{}", data_dir, uuid_str, format_name),
        .path_in_storage = fmt::format("{}{}-deletes.{}", storage_data_dir, uuid_str, format_name)
    };
}

String FileNamesGenerator::convertMetadataPathToStoragePath(const String & metadata_path) const
{
    if (!metadata_path.starts_with(table_dir))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Paths in Iceberg must use a consistent format — either /your/path or s3://your/path. Use the write_full_path_in_iceberg_metadata setting to control this behavior {} {}", metadata_path, table_dir);
    return storage_dir + metadata_path.substr(table_dir.size());
}

}

#endif
