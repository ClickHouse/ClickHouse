#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

#include <filesystem>

#include <fmt/ranges.h>

namespace ProfileEvents
{
    extern const Event DeltaLakeDeltaLogExistenceChecks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int PATH_ACCESS_DENIED;
}

namespace
{

/// Whether `key`, returned by listing `directory`, is an object directly inside it. A storage can put a root of its
/// own in front of the directory (`LocalObjectStorage` resolves a relative path against its root), so the directory's
/// first occurrence in `key` anchors the comparison.
bool isDirectlyInside(std::string_view key, const String & directory)
{
    const String haystack = "/" + String(key);
    for (const String & spelling : {directory, std::filesystem::path(directory).lexically_normal().string()})
    {
        std::string_view dir = spelling;
        while (dir.starts_with('/'))
            dir.remove_prefix(1);
        while (dir.ends_with('/'))
            dir.remove_suffix(1);
        const String needle = dir.empty() ? String("/") : "/" + String(dir) + "/";
        const size_t pos = haystack.find(needle);
        if (pos == String::npos)
            continue;
        const std::string_view rest = std::string_view(haystack).substr(pos + needle.size());
        return !rest.empty() && !rest.contains('/');
    }
    return false;
}

}

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix)
{
    return listFiles(
        object_storage,
        path,
        prefix,
        [&suffix](const RelativePathWithMetadata & files_with_metadata) { return files_with_metadata.relative_path.ends_with(suffix); });
}


std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    /// Without the trailing separator the key also matches sibling directories such as `metadata_old/`.
    const auto key = (std::filesystem::path(path) / prefix / "").string();
    RelativePathsWithMetadata files_with_metadata;
    object_storage.listObjects(key, files_with_metadata, 0);
    Strings res;
    for (const auto & file_with_metadata : files_with_metadata)
    {
        if (check_need(*file_with_metadata))
            res.push_back(file_with_metadata->relative_path);
    }
    LOG_TRACE(getLogger("DataLakeCommon"), "Listed {} files ({})", res.size(), fmt::join(res, ", "));
    return res;
}

std::vector<String> listFilesNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix)
{
    return listFilesNonRecursive(
        object_storage,
        path,
        prefix,
        [&suffix](const RelativePathWithMetadata & files_with_metadata) { return files_with_metadata.relative_path.ends_with(suffix); });
}

std::vector<String> listFilesNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    const auto directory = (std::filesystem::path(path) / prefix / "").string();
    return listFiles(
        object_storage,
        path,
        prefix,
        [&](const RelativePathWithMetadata & file_with_metadata)
        { return check_need(file_with_metadata) && isDirectlyInside(file_with_metadata.relative_path, directory); });
}

bool deltaLogExists(const IObjectStorage & object_storage, const String & path)
{
    ProfileEvents::increment(ProfileEvents::DeltaLakeDeltaLogExistenceChecks);
    const auto delta_log_dir = (std::filesystem::path(path) / "_delta_log").string() + "/";
    RelativePathsWithMetadata files;
    object_storage.listObjects(delta_log_dir, files, /* max_keys */ 1);
    return !files.empty();
}

String resolvePathInsideTable(const String & table_path, const String & relative_path)
{
    auto base = std::filesystem::path(table_path);
    auto combined = base / relative_path;

    if (!pathStartsWith(combined, base))
        throw Exception(
            ErrorCodes::PATH_ACCESS_DENIED,
            "Data lake path `{}` should be inside the table directory `{}`",
            relative_path,
            table_path);

    return combined.string();
}
}
