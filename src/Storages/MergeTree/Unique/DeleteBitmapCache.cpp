#include <Storages/MergeTree/Unique/DeleteBitmapCache.h>

namespace DB
{

DeleteBitmapPtr DeleteBitmapCache::getOrCreate(const MergeTreeDataPartPtr & part, UInt64 version)
{
    if (auto it = get({part->info, version}))
        return it;

    MutableDataPartStoragePtr data_part_storage = const_cast<IMergeTreeDataPart *>(part.get())->getDataPartStoragePtr();

    auto new_delete_bitmap = std::make_shared<DeleteBitmap>(version);
    new_delete_bitmap->deserialize(data_part_storage);

    set({part->info, version}, new_delete_bitmap);
    return new_delete_bitmap;
}
}
