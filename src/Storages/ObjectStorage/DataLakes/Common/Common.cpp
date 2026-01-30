#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

namespace DB
{

std::vector<RelativePathWithMetadataPtr> listFiles(
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


std::vector<RelativePathWithMetadataPtr> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    auto key = std::filesystem::path(path) / prefix;
    RelativePathsWithMetadata files_with_metadata;
    object_storage.listObjects(key, files_with_metadata, 0);
    std::vector<RelativePathWithMetadataPtr> res;
    for (const auto & file_with_metadata : files_with_metadata)
    {
        if (check_need(*file_with_metadata))
            res.emplace_back(file_with_metadata);
    }
    LOG_TRACE(
        getLogger("DataLakeCommon"),
        "Listed {} files ({})",
        res.size(),
        fmt::join(res | std::views::transform([](const auto & p) -> const String & { return p->relative_path; }), ", "));
    return res;
}
}
