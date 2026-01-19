#include <Disks/DiskManifest.h>

#include <Disks/DiskFactory.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include "manifest.pb.h"
#include <Common/Macros.h>
#include <Common/Exception.h>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

ManifestDirectoryIterator::ManifestDirectoryIterator(Strings names_) 
    : names(std::move(names_)), pos(0) 
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
    const String & local_disk_path_,
    const String & remote_disk_path_,
    std::weak_ptr<IManifestStorage> manifest_storage_,
    MergeTreeDataFormatVersion format_version_)
    : IDisk(name_)
    , db_path(db_path_)
    , local_disk_path(local_disk_path_)
    , remote_disk_path(remote_disk_path_)
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

    // Get all keys (empty prefix means all keys)
    manifest->getByPrefix("", keys, values);

    Int64 max_block_number = 0;
    part_map.clear();
    std::vector<Manifest::PartObject> precommit_parts;
    
    for (size_t i = 0; i < keys.size(); ++i)
    {
        // keys[i] is the UUID (e.g., "550e8400-e29b-41d4-a716-446655440000")
        String uuid = keys[i];
        
        Manifest::PartObject part_obj;
        part_obj.ParseFromString(values[i]);
        String part_name = part_obj.name();

        auto remove_part_data = [&]()
        {
            std::filesystem::remove_all(std::filesystem::path(local_disk_path) / uuid);
            manifest->del(uuid);
        };
        
        auto state = static_cast<MergeTreeDataPartState>(part_obj.state());
        auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

        switch (state)
        {
            case MergeTreeDataPartState::Active:
            {
                /// We need to record the block number for renaming PreCommitted parts
                max_block_number = std::max(max_block_number, part_info.max_block);

                part_map[part_name] = part_obj;
                names.push_back(part_name);
                break;
            }
            case MergeTreeDataPartState::Temporary:
            {
                /// PreCommitted (Temporary) part handling
                /// If the part has a level > 0, it may be a leftover from a failed merge.
                /// Loading such parts could complicate the dependency tree in the MergeTree,
                /// and previously loaded parts may become outdated.
                /// To avoid this complexity and potential inconsistency, we discard these parts
                /// instead of restoring them, allowing them to be merged again properly.
                /// Otherwise (level == 0), the part is still valid for precommit and is kept.
                if (part_info.max_block > part_info.min_block)
                {
                    /// Remove the temporary part directory and its contents.
                    remove_part_data();
                }
                else
                {
                    precommit_parts.emplace_back(part_obj);
                }
                break;
            }
            case MergeTreeDataPartState::PreActive:
            {
                /// This state represents previously broken parts that attempted to be moved to the 'detached' folder
                /// but did not complete successfully. The part's UUID still exists in the manifest store. We handle
                /// this state by retrying the rename operation here.
                String from_path = std::filesystem::path(local_disk_path) / uuid;
                if (std::filesystem::exists(from_path))
                {
                    String to_path = std::filesystem::path(local_disk_path) 
                                        / MergeTreeData::DETACHED_DIR_NAME / uuid;
                    std::filesystem::rename(from_path, to_path);
                }
                manifest->del(uuid);
                break;
            }
            case MergeTreeDataPartState::Deleting:
            {
                remove_part_data();
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, 
                                "Corrupted manifest part state: {}", 
                                static_cast<int>(state));
            }
        }
    }

    for (auto & part_obj : precommit_parts)
    {
        auto part_info = MergeTreePartInfo::tryParsePartName(part_obj.name(), format_version);
        part_info->min_block = part_info->max_block = ++max_block_number;
        String new_part_name = part_info->getPartNameV1();
        part_obj.set_name(new_part_name);
        part_map[new_part_name] = part_obj;
        names.push_back(new_part_name);
    }

    return std::make_unique<ManifestDirectoryIterator>(std::move(names));
}

} // namespace DB
