#include <Storages/StorageUniqueMergeTree.h>
#include <Storages/UniqueMergeTree/DeleteBitmapCache.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}

DeleteBitmapPtr DeleteBitmapCache::getOrCreate(const MergeTreeDataPartPtr & part, UInt64 version)
{
    if (auto it = get({part->info, version}))
        return it;

    auto disk = part->data_part_storage->getDisk();
    String full_path = part->data_part_storage->getRelativePath() + StorageUniqueMergeTree::DELETE_DIR_NAME + toString(version) + ".bitmap";
    if (!disk->exists(full_path))
    {
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST,
            "The delete bitmap file of version {} for data part {} does not exist",
            version,
            part->info.getPartName());
    }
    auto new_delete_bitmap = std::make_shared<DeleteBitmap>();
    new_delete_bitmap->deserialize(full_path, disk);
    set({part->info, version}, new_delete_bitmap);
    return new_delete_bitmap;
}
}
