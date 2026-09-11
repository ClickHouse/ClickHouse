#include <base/pathToString.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/Exception.h>
#include <Common/ObjectStorageKey.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

#include <filesystem>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int PATH_ACCESS_DENIED;
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
    /// An object storage key, not a filesystem path, so it is joined in key string space.
    const auto key = appendObjectStorageKeySegment(path, prefix);
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

String resolvePathInsideTable(const String & table_path, const String & relative_path)
{
    /// The containment test is what defines "inside" here, and it is `std::filesystem`'s notion of
    /// it, so the two keys are converted into paths explicitly rather than through the narrow
    /// constructor. The result is read back generically: `path::string()` would hand back the `\`
    /// that `operator/` appends on Windows, where it is an ordinary character in a key.
    const auto base = pathFromString(table_path);
    const auto combined = base / pathFromString(relative_path);

    if (!pathStartsWith(combined, base))
        throw Exception(
            ErrorCodes::PATH_ACCESS_DENIED,
            "Data lake path `{}` should be inside the table directory `{}`",
            relative_path,
            table_path);

    return pathToGenericString(combined);
}
}
