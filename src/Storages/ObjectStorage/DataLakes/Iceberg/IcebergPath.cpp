#include <filesystem>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <Databases/DataLake/ICatalog.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Common/Exception.h>
#include <Common/FullyQualifiedObjectPath.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

namespace
{
std::string_view trimTrailingSlashes(std::string_view str)
{
    while (!str.empty() && str.back() == '/')
        str.remove_suffix(1);
    return str;
}
}

BlobStorageDescription BlobStorageDescription::fromConfiguration(const DB::StorageObjectStorageConfiguration & configuration)
{
    return {
        .type_name = configuration.getTypeName(),
        .namespace_name = configuration.getNamespace(),
        .allow_foreign_namespaces = configuration.supportsFullyQualifiedPaths(),
    };
}

IcebergPathResolver::TableRootDerivation IcebergPathResolver::deriveTableRoot(
    const String & table_location, const String & queried_path, const String & metadata_file_key)
{
    static constexpr std::string_view metadata_dir = "/metadata/";

    auto queried = trimTrailingSlashes(queried_path);
    auto key = trimTrailingSlashes(metadata_file_key);

    /// Only a document directly inside the table's own `metadata` directory names the table root.
    auto metadata_dir_pos = key.rfind(metadata_dir);
    if (metadata_dir_pos == std::string_view::npos
        || key.find('/', metadata_dir_pos + metadata_dir.size()) != std::string_view::npos)
        return {queried_path, RootRelation::Unknown};
    auto candidate = key.substr(0, metadata_dir_pos);

    if (candidate == queried)
        return {queried_path, RootRelation::Same};

    /// A component-aligned proper descendant; an empty queried path is the storage root.
    const bool is_descendant = queried.empty()
        ? !candidate.empty()
        : candidate.size() > queried.size() && candidate.starts_with(queried) && candidate[queried.size()] == '/';
    if (!is_descendant)
        return {queried_path, RootRelation::Unknown};

    auto strip_leading_slash = [](std::string_view str) { return str.starts_with('/') ? str.substr(1) : str; };

    /// Adopt only when the declared location denotes that same directory. It may differ from the
    /// storage path by a leading slash and by a `<scheme>://<authority>/` prefix, both of which
    /// still name that directory; a prefix carrying a path component names a different one.
    auto location = strip_leading_slash(trimTrailingSlashes(table_location));
    auto tail = strip_leading_slash(candidate);
    if (tail.empty())
        return {queried_path, RootRelation::Unknown};
    bool location_agrees = location == tail;
    if (!location_agrees && location.size() > tail.size() && location.ends_with(tail)
        && location[location.size() - tail.size() - 1] == '/')
    {
        auto prefix = location.substr(0, location.size() - tail.size());
        auto scheme_end = prefix.find("://");
        location_agrees = scheme_end != std::string_view::npos && prefix.find('/', scheme_end + 3) == prefix.size() - 1;
    }
    if (!location_agrees)
        return {queried_path, RootRelation::Unknown};

    return {String(candidate), RootRelation::AdoptedDescendant};
}

String IcebergPathResolver::parseNamespace(std::string_view path)
{
    if (auto qualified = trySplitFullyQualifiedObjectPath(path))
        return String(qualified->object_namespace);
    return {};
}

bool IcebergPathResolver::isInForeignNamespace(const String & raw_path) const
{
    if (!blob_storage.allow_foreign_namespaces || blob_storage.namespace_name.empty())
        return false;

    auto qualified = trySplitFullyQualifiedObjectPath(raw_path);
    if (!qualified)
        return false;

    auto path_storage_type = DataLake::tryParseStorageTypeFromString(String(qualified->scheme));
    if (!path_storage_type || path_storage_type != DataLake::tryParseStorageTypeFromString(blob_storage.type_name))
        return false;

    const String path_namespace(qualified->object_namespace);
    return path_namespace != blob_storage.namespace_name && path_namespace != table_location_namespace;
}

// This function is used to get the file path inside the directory which corresponds to Iceberg table from the full blob path which is written in manifest and metadata files.
// For example, if the full blob path is s3://bucket/table_name/data/00000-1-1234567890.avro, the function will return table_name/data/00000-1-1234567890.avro
// Common path should end with "<table_name>" or "<table_name>/".
String IcebergPathResolver::resolve(const IcebergPathFromMetadata & metadata_path) const
{
    auto trim_forward_slash = [](std::string_view str) -> std::string_view
    {
        if (str.starts_with('/'))
        {
            return str.substr(1);
        }
        return str;
    };

    auto raw_path = metadata_path.serialize();

    if (raw_path.starts_with(table_location)
        && (raw_path.size() == table_location.size() || raw_path[table_location.size()] == '/'))
    {
        auto result = std::filesystem::path{table_root} / trim_forward_slash(raw_path.substr(table_location.size()));
        return result;
    }

    if (isInForeignNamespace(raw_path))
        return raw_path;

    if (table_root.empty())
    {
        throw ::DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
            "IcebergPathResolver::resolve failed first branch. raw_path='{}', table_location='{}', table_root='{}'",
            raw_path, table_location, table_root);
    }


    auto pos = raw_path.find(table_root);
    /// Valid situation when data and metadata files are stored in different directories.
    if (pos == std::string::npos)
    {
        /// connection://bucket
        auto prefix = table_location.substr(0, table_location.size() - table_root.size());
        if (raw_path.size() < prefix.size())
        {
            throw ::DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "IcebergPathResolver::resolve failed in the second branch. raw_path='{}', table_location='{}', table_root='{}'",
                raw_path,
                table_location,
                table_root);
        }
        return std::string{raw_path.substr(prefix.size())};
    }

    size_t good_pos = std::string::npos;
    while (pos != std::string::npos)
    {
        auto potential_position = pos + table_root.size();
        if (((potential_position + 6 <= raw_path.size()) && (std::string_view(raw_path.data() + potential_position, 6) == "/data/"))
            || ((potential_position + 10 <= raw_path.size())
                && (std::string_view(raw_path.data() + potential_position, 10) == "/metadata/")))
        {
            good_pos = pos;
            break;
        }
        size_t new_pos = raw_path.find(table_root, pos + 1);
        if (new_pos == std::string::npos)
        {
            break;
        }
        pos = new_pos;
    }


    if (good_pos != std::string::npos)
    {
        return std::string{raw_path.substr(good_pos)};
    }
    else if (pos != std::string::npos)
    {
        return std::string{raw_path.substr(pos)};
    }
    else
    {
        throw ::DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Expected to find '{}' in data path: '{}'", table_root, raw_path);
    }
}
}
