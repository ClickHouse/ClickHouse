#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/Exception.h>
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
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, const String & suffix)
{
    auto key = std::filesystem::path(configuration.getPathForRead().path) / prefix;
    RelativePathsWithMetadata files_with_metadata;
    object_storage.listObjects(key, files_with_metadata, 0);
    Strings res;
    for (const auto & file_with_metadata : files_with_metadata)
    {
        const auto & filename = file_with_metadata->relative_path;
        if (filename.ends_with(suffix))
            res.push_back(filename);
    }
    LOG_TRACE(getLogger("DataLakeCommon"), "Listed {} files ({})", res.size(), fmt::join(res, ", "));
    return res;
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
