#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
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

/// Whether `key`, returned by listing `directory`, is an object directly inside it. A storage may return the keys
/// without leading separators and with the directory in normal form (HDFS canonicalizes it).
bool isDirectlyInside(std::string_view key, const String & directory)
{
    while (key.starts_with('/'))
        key.remove_prefix(1);
    for (const String & spelling : {directory, std::filesystem::path(directory).lexically_normal().string()})
    {
        std::string_view dir = spelling;
        while (dir.starts_with('/'))
            dir.remove_prefix(1);
        while (dir.ends_with('/'))
            dir.remove_suffix(1);
        std::string_view rest = key;
        if (!dir.empty())
        {
            if (!rest.starts_with(dir) || rest.size() == dir.size() || rest[dir.size()] != '/')
                continue;
            rest.remove_prefix(dir.size() + 1);
        }
        return !rest.empty() && !rest.contains('/');
    }
    return false;
}

/// Objects at any depth inside the directory `path`/`prefix` whose names inside it start with `name_prefix`.
std::vector<String> listFilesImpl(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const String & name_prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    /// Without the trailing separator the key also matches sibling directories such as `metadata_old/`.
    const auto key = (std::filesystem::path(path) / prefix / "").string() + name_prefix;
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

std::vector<String> listFilesNonRecursiveImpl(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const String & name_prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    auto directory = (std::filesystem::path(path) / prefix / "").string();
    /// The local storage lists a relative directory under its root, the same way it resolves any other key.
    if (object_storage.getType() == ObjectStorageType::Local)
        directory = resolvePathRelativelyToBase(directory, object_storage.getCommonKeyPrefix());
    return listFilesImpl(
        object_storage,
        path,
        prefix,
        name_prefix,
        [&](const RelativePathWithMetadata & file_with_metadata)
        { return check_need(file_with_metadata) && isDirectlyInside(file_with_metadata.relative_path, directory); });
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
    return listFilesImpl(object_storage, path, prefix, "", check_need);
}

std::vector<String> listFilesNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix)
{
    return listFilesNonRecursive(object_storage, path, prefix, "", suffix);
}

std::vector<String> listFilesNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const String & name_prefix,
    const String & suffix)
{
    return listFilesNonRecursiveImpl(
        object_storage,
        path,
        prefix,
        name_prefix,
        [&suffix](const RelativePathWithMetadata & files_with_metadata) { return files_with_metadata.relative_path.ends_with(suffix); });
}

std::vector<String> listFilesNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    return listFilesNonRecursiveImpl(object_storage, path, prefix, "", check_need);
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
