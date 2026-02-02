#include <Disks/DiskManifest.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/Manifest/BaseManifestEntry.h>
#include <Storages/MergeTree/Manifest/IManifestStorage.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromString.h>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

ManifestDirectoryIterator::ManifestDirectoryIterator(Strings names_)
    : names(std::move(names_))
    , pos(0)
{
}

void ManifestDirectoryIterator::next()
{
    ++pos;
}

bool ManifestDirectoryIterator::isValid() const
{
    return pos < names.size();
}

String ManifestDirectoryIterator::name() const
{
    if (isValid())
        return names[pos];
    return "";
}

String ManifestDirectoryIterator::path() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

DiskManifest::DiskManifest(
    const String & name_,
    const String & db_path_,
    const String & relative_data_path_,
    std::weak_ptr<IManifestStorage> manifest_storage_,
    MergeTreeDataFormatVersion format_version_)
    : IDisk(name_)
    , db_path(db_path_)
    , relative_data_path(relative_data_path_)
    , manifest_storage(manifest_storage_)
    , format_version(format_version_)
{
    data_source_description = DataSourceDescription{
        .type = DataSourceType::Local,
        .description = db_path,
        .is_encrypted = false,
        .is_cached = false,
        .zookeeper_name = "",
    };
}

DirectoryIteratorPtr DiskManifest::iterateDirectory(const String & /* path */) const
{
    std::vector<String> keys;
    std::vector<String> values;
    Strings names;

    auto manifest = manifest_storage.lock();
    if (!manifest)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ManifestStorage is expired in DiskManifest");

    manifest->getByPrefix("", keys, values);

    for (size_t i = 0; i < keys.size(); ++i)
    {
        const String & uuid = keys[i];

        BaseManifestEntry entry;
        ReadBufferFromString read_buffer(values[i]);
        entry.readText(read_buffer);
        String part_name = entry.name;

        auto state = static_cast<MergeTreeDataPartState>(entry.state);
        auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

        if (state == MergeTreeDataPartState::Active)
        {
            entry.part_uuid = uuid;
            part_map[part_name] = entry;
            names.push_back(part_name);
        }
        else if (state == MergeTreeDataPartState::PreActive)
        {
            const String & disk_path = entry.disk_path;
            std::filesystem::path from_path = std::filesystem::path(disk_path) / relative_data_path / uuid;

            if (std::filesystem::exists(from_path))
            {
                std::filesystem::path detached_dir = std::filesystem::path(disk_path) / relative_data_path
                                        / MergeTreeData::DETACHED_DIR_NAME;
                if (!std::filesystem::exists(detached_dir))
                    std::filesystem::create_directories(detached_dir);
                std::filesystem::path to_path = detached_dir / uuid;

                try
                {
                    std::filesystem::rename(from_path, to_path);
                }
                catch (const std::filesystem::filesystem_error & e)
                {
                    (void)e;
                }
            }
            try
            {
                throwIfNotOKOrNotFound(manifest->del(uuid), "delete PreActive part from manifest", part_name);
            }
            catch (const Exception & e)
            {
                LOG_WARNING(
                    &Poco::Logger::get("DiskManifest"),
                    "Failed to delete PreActive part '{}' from manifest: {}",
                    part_name,
                    e.message());
            }
        }
        else
        {
            try
            {
                throwIfNotOKOrNotFound(manifest->del(uuid), "delete part from manifest", part_name);
            }
            catch (const Exception & e)
            {
                LOG_WARNING(
                    &Poco::Logger::get("DiskManifest"),
                    "Failed to delete part '{}' from manifest: {}",
                    part_name,
                    e.message());
            }
        }
    }

    return std::make_unique<ManifestDirectoryIterator>(std::move(names));
}

}
